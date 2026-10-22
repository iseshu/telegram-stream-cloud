package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeSource struct {
	parts map[int][]byte
	calls atomic.Int64
}

func (f *fakeSource) ReadBlock(ctx context.Context, p Part, off int64) ([]byte, error) {
	f.calls.Add(1)
	b := f.parts[p.MessageID]
	return b[off:min(int64(len(b)), off+chunkSize)], nil
}
func testServer(t *testing.T) (*Server, Manifest, []byte) {
	t.Helper()
	store, e := newStore(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	all := make([]byte, 2_300_123)
	for i := range all {
		all[i] = byte((i*17 + i/19) % 251)
	}
	parts := []Part{{1, 700_003}, {2, 900_007}, {3, 700_113}}
	f := &fakeSource{parts: map[int][]byte{}}
	off := 0
	for _, p := range parts {
		f.parts[p.MessageID] = all[off : off+int(p.Size)]
		off += int(p.Size)
	}
	id, _ := randomID()
	m := Manifest{Version: 1, ID: id, Name: "test.mp4", MIME: "video/mp4", Size: int64(len(all)), Created: time.Now().UTC().Truncate(time.Second), Parts: parts}
	if e = store.Save(m); e != nil {
		t.Fatal(e)
	}
	s := &Server{cfg: Config{BaseURL: "https://files.example", Prefetch: 2}, ctx: context.Background(), store: store, cache: newCache(8 * 1024 * 1024), source: f, key: []byte("test-only-signing-key"), slots: make(chan struct{}, 4), flights: map[string]*flight{}}
	return s, m, all
}
func TestRangesAcrossParts(t *testing.T) {
	s, m, all := testServer(t)
	r := rand.New(rand.NewSource(7))
	cases := [][2]int64{{0, 0}, {m.Size - 1, m.Size - 1}, {699999, 700010}, {699999, 1600020}, {524287, 524289}, {0, m.Size - 1}}
	for i := 0; i < 80; i++ {
		a := r.Int63n(m.Size)
		b := a + r.Int63n(m.Size-a)
		cases = append(cases, [2]int64{a, b})
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", s.link("/inline/", m.ID), nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", c[0], c[1]))
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != 206 || !bytes.Equal(w.Body.Bytes(), all[c[0]:c[1]+1]) {
			t.Fatalf("range %v: status=%d bytes=%d", c, w.Code, w.Body.Len())
		}
		if w.Header().Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", c[0], c[1], m.Size) {
			t.Fatal("wrong content-range")
		}
	}
}
func TestRangeSemantics(t *testing.T) {
	s, m, all := testServer(t)
	for _, tc := range []struct {
		method, br string
		status     int
		length     int
	}{{"GET", "bytes=-9", 206, 9}, {"GET", fmt.Sprintf("bytes=%d-", m.Size-8), 206, 8}, {"GET", "bytes=99999999-", 416, -1}, {"GET", "bytes=5-4", 416, -1}, {"GET", "bytes=-0", 416, -1}, {"GET", "bytes=+1-2", 416, -1}, {"GET", "bytes=0-0,2-2", 200, len(all)}, {"GET", "", 200, len(all)}, {"HEAD", "bytes=0-0", 200, 0}} {
		req := httptest.NewRequest(tc.method, s.link("/dl/", m.ID), nil)
		req.Header.Set("Range", tc.br)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != tc.status || (tc.length >= 0 && w.Body.Len() != tc.length) {
			t.Fatalf("%+v got %d len=%d", tc, w.Code, w.Body.Len())
		}
		if !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment") {
			t.Fatal("missing attachment header")
		}
	}
}
func TestAuthorizationAndConditional(t *testing.T) {
	s, m, _ := testServer(t)
	for _, tc := range []struct {
		path, method, header, value string
		status                      int
	}{{"/inline/" + m.ID, "GET", "", "", 403}, {s.link("/inline/", m.ID), "POST", "", "", 405}, {s.link("/inline/", m.ID), "GET", "If-None-Match", `W/"` + m.ID + `"`, 304}, {s.link("/inline/", m.ID), "GET", "If-Range", `"different"`, 200}} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if tc.header != "" {
			r.Header.Set(tc.header, tc.value)
		}
		r.Header.Set("Range", "bytes=0-0")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%+v got %d", tc, w.Code)
		}
	}
}
func TestCacheBoundsAndConcurrency(t *testing.T) {
	c := newCache(1024)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				k := fmt.Sprintf("%d:%d", i, j)
				c.Put(k, make([]byte, 127))
				c.Get(k)
			}
		}(i)
	}
	wg.Wait()
	if c.Stats()["bytes"] > 1024 {
		t.Fatal("cache over budget")
	}
}
func TestManifestValidationAndPersistence(t *testing.T) {
	s, m, _ := testServer(t)
	got, e := s.store.Get(m.ID)
	if e != nil || got.Size != m.Size {
		t.Fatal(e)
	}
	m.Parts[0].Size++
	if s.store.Save(m) == nil {
		t.Fatal("accepted invalid manifest")
	}
	if _, e = s.store.Get("../secret"); e == nil {
		t.Fatal("accepted traversal")
	}
}
func TestPlayerEscapesFilename(t *testing.T) {
	s, m, _ := testServer(t)
	m.Name = `<script>alert(1)</script>`
	if e := s.store.Save(m); e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", s.link("/watch/", m.ID), nil))
	if strings.Contains(w.Body.String(), m.Name) {
		t.Fatal("unescaped filename")
	}
}
func TestStreamCapacity(t *testing.T) {
	s, m, _ := testServer(t)
	for i := 0; i < cap(s.slots); i++ {
		s.slots <- struct{}{}
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", s.link("/inline/", m.ID), nil))
	if w.Code != 503 || w.Header().Get("Content-Length") != "" {
		t.Fatal("bad capacity response")
	}
}
func TestHTTPRealContentLength(t *testing.T) {
	s, m, _ := testServer(t)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	u, e := url.Parse(s.link("/inline/", m.ID))
	if e != nil {
		t.Fatal(e)
	}
	r, e := http.NewRequest("GET", ts.URL+u.RequestURI(), nil)
	if e != nil {
		t.Fatal(e)
	}
	r.Header.Set("Range", "bytes=699999-700010")
	resp, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	b, e := io.ReadAll(resp.Body)
	if e != nil || len(b) != 12 || resp.ContentLength != 12 {
		t.Fatalf("%v, length %d", e, len(b))
	}
}

type blockingSource struct{ started, stopped chan struct{} }

func (b *blockingSource) ReadBlock(ctx context.Context, p Part, off int64) ([]byte, error) {
	close(b.started)
	<-ctx.Done()
	close(b.stopped)
	return nil, ctx.Err()
}
func TestCanceledViewerReleasesFetch(t *testing.T) {
	s, _, _ := testServer(t)
	src := &blockingSource{started: make(chan struct{}), stopped: make(chan struct{})}
	s.source = src
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _, _ = s.block(ctx, Part{1, 10}, 0) }()
	<-src.started
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("viewer stuck")
	}
	select {
	case <-src.stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream fetch leaked")
	}
}

func TestNamedSignedPath(t *testing.T) {
	s, m, _ := testServer(t)
	m.Name = "Film 2026 #1 తెలుగు.mp4"
	if e := s.store.Save(m); e != nil {
		t.Fatal(e)
	}
	for _, route := range []string{"/watch/", "/inline/", "/dl/"} {
		raw := s.link(route, m.ID)
		u, e := url.Parse(raw)
		if e != nil || u.RawQuery != "" {
			t.Fatal("unexpected query", raw)
		}
		parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
		if len(parts) != 3 || parts[2] != m.Name || len(parts[1]) != 112 {
			t.Fatal(raw)
		}
		r := httptest.NewRequest("HEAD", raw, nil)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("%s returned %d", raw, w.Code)
		}
		token := parts[1]
		replacement := "0"
		if token[48] == '0' {
			replacement = "1"
		}
		bad := strings.Replace(raw, token, token[:48]+replacement+token[49:], 1)
		w = httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("HEAD", bad, nil))
		if w.Code != 403 {
			t.Fatal("accepted forged hash")
		}
	}
	raw := s.link("/watch/", m.ID)
	u, _ := url.Parse(raw)
	u.Path = path.Dir(u.Path) + "/different.mp4"
	u.RawPath = ""
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", u.String(), nil))
	if w.Code != 404 {
		t.Fatal("accepted wrong filename")
	}
}
