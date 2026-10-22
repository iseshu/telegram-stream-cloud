package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BlockSource interface {
	ReadBlock(context.Context, Part, int64) ([]byte, error)
}
type flight struct {
	done    chan struct{}
	data    []byte
	err     error
	waiters int
	cancel  context.CancelFunc
}
type Server struct {
	cfg     Config
	store   *Store
	cache   *BlockCache
	source  BlockSource
	pool    *Pool
	ctx     context.Context
	key     []byte
	slots   chan struct{}
	mu      sync.Mutex
	flights map[string]*flight
}

func signingKey(dir string) ([]byte, error) {
	path := filepath.Join(dir, "link.key")
	b, e := os.ReadFile(path)
	if e == nil {
		if len(b) != 32 {
			return nil, errors.New("invalid link signing key")
		}
		return b, nil
	}
	if !os.IsNotExist(e) {
		return nil, e
	}
	b = make([]byte, 32)
	if _, e = rand.Read(b); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e != nil {
		return nil, e
	}
	return b, ce
}
func (s *Server) signature(id string) string {
	h := hmac.New(sha256.New, s.key)
	_, _ = h.Write([]byte(id))
	return hex.EncodeToString(h.Sum(nil))
}
func (s *Server) link(route, id string) string {
	return s.cfg.BaseURL + route + id + "?sig=" + s.signature(id)
}
func parseRange(value string, size int64) (start, end int64, partial bool, err error) {
	if value == "" || !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") {
		return 0, size - 1, false, nil
	}
	v := strings.TrimSpace(strings.TrimPrefix(value, "bytes="))
	a, b, ok := strings.Cut(v, "-")
	bad := func() (int64, int64, bool, error) { return 0, 0, false, errors.New("unsatisfiable range") }
	if !ok || size <= 0 {
		return bad()
	}
	number := func(s string) (int64, error) {
		if s == "" {
			return 0, errors.New("empty")
		}
		for _, c := range s {
			if c < '0' || c > '9' {
				return 0, errors.New("invalid")
			}
		}
		return strconv.ParseInt(s, 10, 64)
	}
	if a == "" {
		n, e := number(b)
		if e != nil || n <= 0 {
			return bad()
		}
		return max(0, size-n), size - 1, true, nil
	}
	start, err = number(a)
	if err != nil || start >= size {
		return bad()
	}
	end = size - 1
	if b != "" {
		end, err = number(b)
		if err != nil || end < start {
			return bad()
		}
		end = min(end, size-1)
	}
	return start, end, true, nil
}
func (s *Server) block(ctx context.Context, p Part, offset int64) ([]byte, error) {
	key := fmt.Sprintf("%d:%d:%d", p.MessageID, p.Size, offset)
	if b, ok := s.cache.Get(key); ok {
		return b, nil
	}
	s.mu.Lock()
	f, ok := s.flights[key]
	if !ok {
		call, cancel := context.WithTimeout(s.ctx, 60*time.Second)
		f = &flight{done: make(chan struct{}), cancel: cancel}
		s.flights[key] = f
		go func() {
			defer cancel()
			f.data, f.err = s.source.ReadBlock(call, p, offset)
			if f.err == nil && (offset < 8*1024*1024 || offset >= p.Size-8*1024*1024) {
				s.cache.Put(key, f.data)
			}
			s.mu.Lock()
			if s.flights[key] == f {
				delete(s.flights, key)
			}
			close(f.done)
			s.mu.Unlock()
		}()
	}
	f.waiters++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		f.waiters--
		if f.waiters == 0 {
			f.cancel()
			if s.flights[key] == f {
				delete(s.flights, key)
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.done:
		return f.data, f.err
	}
}

type piece struct {
	data    []byte
	err     error
	waiters int
	cancel  context.CancelFunc
}

func (s *Server) produce(ctx context.Context, m Manifest, start, end int64) <-chan piece {
	out := make(chan piece, s.cfg.Prefetch)
	go func() {
		defer close(out)
		send := func(p piece) bool {
			select {
			case out <- p:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for _, seg := range segments(m, start, end) {
			stop := seg.Offset + seg.Length
			for off := seg.Offset / chunkSize * chunkSize; off < stop; off += chunkSize {
				b, e := s.block(ctx, seg.Part, off)
				if e != nil {
					send(piece{err: e})
					return
				}
				lo, hi := max(seg.Offset-off, 0), min(stop-off, int64(len(b)))
				if hi <= lo {
					send(piece{err: io.ErrUnexpectedEOF})
					return
				}
				if !send(piece{data: b[lo:hi]}) {
					return
				}
			}
		}
	}()
	return out
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		stats := map[string]int{"ready": 0, "total": 0}
		if s.pool != nil {
			stats = s.pool.Stats()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "up", "workers": stats, "cache": s.cache.Stats(), "active_streams": len(s.slots)})
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if s.pool == nil || s.pool.Stats()["ready"] == 0 {
			http.Error(w, "no ready workers", 503)
			return
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("/inline/", s.media)
	mux.HandleFunc("/dl/", s.media)
	mux.HandleFunc("/watch/", s.player)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	return mux
}
func (s *Server) authorize(w http.ResponseWriter, r *http.Request) (Manifest, bool) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", 405)
		return Manifest{}, false
	}
	id := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	sig, e := hex.DecodeString(r.URL.Query().Get("sig"))
	want, _ := hex.DecodeString(s.signature(id))
	if !validID(id) || e != nil || !hmac.Equal(sig, want) {
		http.Error(w, "invalid link", 403)
		return Manifest{}, false
	}
	m, e := s.store.Get(id)
	if e != nil {
		http.NotFound(w, r)
		return Manifest{}, false
	}
	return m, true
}
func (s *Server) media(w http.ResponseWriter, r *http.Request) {
	m, ok := s.authorize(w, r)
	if !ok {
		return
	}
	h := w.Header()
	h.Set("Accept-Ranges", "bytes")
	h.Set("ETag", `"`+m.ID+`"`)
	h.Set("Last-Modified", m.Created.UTC().Format(http.TimeFormat))
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Accel-Buffering", "no")
	h.Set("Cache-Control", "private, max-age=3600, no-transform")
	if s.cfg.CDNCacheSeconds > 0 {
		h.Set("Cache-Control", fmt.Sprintf("public, max-age=%d, immutable, no-transform", s.cfg.CDNCacheSeconds))
	}
	mt := m.MIME
	if !strings.HasPrefix(mt, "video/") && !strings.HasPrefix(mt, "audio/") {
		mt = "application/octet-stream"
	}
	h.Set("Content-Type", mt)
	disposition := "inline"
	if strings.HasPrefix(r.URL.Path, "/dl/") {
		disposition = "attachment"
	}
	h.Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": m.Name}))
	if match := r.Header.Get("If-None-Match"); match != "" {
		for _, tag := range strings.Split(match, ",") {
			tag = strings.TrimSpace(tag)
			if tag == "*" || strings.TrimPrefix(tag, "W/") == h.Get("ETag") {
				w.WriteHeader(304)
				return
			}
		}
	}
	rv := r.Header.Get("Range")
	if r.Method == "HEAD" {
		rv = ""
	}
	if ir := r.Header.Get("If-Range"); ir != "" && ir != h.Get("ETag") {
		date, e := http.ParseTime(ir)
		if e != nil || m.Created.Truncate(time.Second).After(date) {
			rv = ""
		}
	}
	start, end, partial, e := parseRange(rv, m.Size)
	if e != nil {
		h.Set("Content-Range", fmt.Sprintf("bytes */%d", m.Size))
		http.Error(w, "range not satisfiable", 416)
		return
	}
	h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	status := 200
	if partial {
		status = 206
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, m.Size))
	}
	if r.Method == "HEAD" {
		w.WriteHeader(status)
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		h.Del("Content-Length")
		h.Del("Content-Range")
		h.Set("Retry-After", "2")
		h.Set("Cache-Control", "no-store")
		http.Error(w, "stream capacity reached", 503)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	q := s.produce(ctx, m, start, end)
	first, ok := <-q
	if !ok || first.err != nil {
		h.Del("Content-Length")
		h.Del("Content-Range")
		h.Set("Cache-Control", "no-store")
		http.Error(w, "upstream unavailable", 502)
		return
	}
	w.WriteHeader(status)
	write := func(b []byte) bool {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, e := w.Write(b)
		if e == nil {
			_ = http.NewResponseController(w).Flush()
		}
		return e == nil
	}
	if !write(first.data) {
		return
	}
	for p := range q {
		if p.err != nil {
			panic(http.ErrAbortHandler)
		}
		if !write(p.data) {
			return
		}
	}
}

var playerTemplate = template.Must(template.New("player").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{{.Name}} · FileStream</title><style>body{margin:0;background:#101218;color:#e7e9f0;font:16px system-ui}main{max-width:1100px;margin:5vh auto;padding:24px}h1{font-size:clamp(20px,4vw,32px);overflow-wrap:anywhere}video{width:100%;max-height:70vh;background:#000;border-radius:16px}nav{display:flex;gap:12px;flex-wrap:wrap;margin:24px 0}a,button{background:#292f40;color:#e7e9f0;border:1px solid #49536b;padding:12px 18px;border-radius:9px;text-decoration:none;cursor:pointer;font:inherit}p{color:#a9b2c9}input{width:100%;box-sizing:border-box;background:#181d29;color:#c5d6ff;border:1px solid #49536b;padding:12px;border-radius:8px}</style><main><p>FILESTREAM</p><h1>{{.Name}}</h1><video id="video" controls playsinline preload="metadata" src="{{.Stream}}"></video><nav><button id="copy">Copy stream link</button><a href="{{.Download}}">Download</a><button id="vlc">Open in VLC</button></nav><input id="link" aria-label="Stream URL" readonly value="{{.Stream}}"><p id="hint">Space: play/pause · ←/→: seek 10 seconds · F: fullscreen. Playback depends on your browser's codec support.</p></main><script>const v=document.getElementById('video'),l=document.getElementById('link');document.getElementById('copy').onclick=async()=>{try{await navigator.clipboard.writeText(l.value);document.getElementById('hint').textContent='Stream link copied'}catch(e){l.select();document.getElementById('hint').textContent='Copy the selected link'}};document.getElementById('vlc').onclick=()=>{location.href='vlc://'+l.value};document.addEventListener('keydown',e=>{if(/INPUT|BUTTON|A|TEXTAREA/.test(e.target.tagName))return;if(e.code==='Space'){e.preventDefault();v.paused?v.play():v.pause()}if(e.key==='ArrowRight'&&Number.isFinite(v.duration)){e.preventDefault();v.currentTime=Math.min(v.duration,v.currentTime+10)}if(e.key==='ArrowLeft'){e.preventDefault();v.currentTime=Math.max(0,v.currentTime-10)}if(e.key.toLowerCase()==='f')v.requestFullscreen?.()})</script></html>`))

func (s *Server) player(w http.ResponseWriter, r *http.Request) {
	m, ok := s.authorize(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; media-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'")
	if r.Method == "HEAD" {
		return
	}
	_ = playerTemplate.Execute(w, map[string]string{"Name": m.Name, "Stream": s.link("/inline/", m.ID), "Download": s.link("/dl/", m.ID)})
}
