package main

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

type atomicSession struct {
	Path string
	mu   sync.Mutex
}

func (s *atomicSession) LoadSession(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, e := os.ReadFile(s.Path)
	if os.IsNotExist(e) {
		return nil, session.ErrNotFound
	}
	return b, e
}
func (s *atomicSession) StoreSession(ctx context.Context, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicBytes(s.Path, b)
}

type docRef struct {
	doc *tg.Document
	at  time.Time
}
type Worker struct {
	index                          int
	token                          string
	cfg                            Config
	connected, authenticated, busy atomic.Bool
	cooldown                       atomic.Int64
	mu                             sync.Mutex
	client                         *telegram.Client
	api                            *tg.Client
	channel                        *tg.InputChannel
	docs                           map[int]docRef
	dcs                            map[int]telegram.CloseInvoker
}
type Pool struct {
	workers []*Worker
	next    atomic.Uint64
}

func newPool(c Config) *Pool {
	p := &Pool{}
	tokens := c.WorkerTokens
	if len(tokens) == 0 {
		tokens = []string{c.PrimaryToken}
	}
	for i, t := range tokens {
		p.workers = append(p.workers, &Worker{index: i, token: t, cfg: c, docs: map[int]docRef{}, dcs: map[int]telegram.CloseInvoker{}})
	}
	return p
}
func (w *Worker) ready() bool {
	return w.connected.Load() && w.authenticated.Load() && time.Now().UnixNano() >= w.cooldown.Load()
}
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		w.authenticated.Store(false)
		botID, _, _ := stringsCut(w.token)
		c := telegram.NewClient(w.cfg.APIID, w.cfg.APIHash, telegram.Options{SessionStorage: &atomicSession{Path: filepath.Join(w.cfg.DataDir, "sessions", "bot-"+botID+".json")}, NoUpdates: true, OnConnectionState: func(s telegram.ConnectionState) { w.connected.Store(s == telegram.ConnectionStateReady) }})
		w.mu.Lock()
		w.client = c
		w.api = c.API()
		w.docs = map[int]docRef{}
		w.dcs = map[int]telegram.CloseInvoker{}
		w.mu.Unlock()
		err := c.Run(ctx, func(ctx context.Context) error {
			status, e := c.Auth().Status(ctx)
			if e != nil {
				return e
			}
			if !status.Authorized {
				if _, e = c.Auth().Bot(ctx, w.token); e != nil {
					return e
				}
			}

			ch, e := resolveChannel(ctx, c.API(), w.cfg.ChannelID)
			if e != nil {
				return e
			}
			w.mu.Lock()
			w.channel = ch
			w.mu.Unlock()
			w.authenticated.Store(true)
			slog.Info("worker ready", "worker", w.index+1)
			<-ctx.Done()
			return ctx.Err()
		})
		w.authenticated.Store(false)
		w.connected.Store(false)
		w.mu.Lock()
		for _, dc := range w.dcs {
			_ = dc.Close()
		}
		w.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		slog.Warn("worker reconnect scheduled", "worker", w.index+1, "reason", safeError(err))
		if pause(ctx, 10*time.Second) != nil {
			return
		}
	}
}
func stringsCut(s string) (string, string, bool) {
	for i := range s {
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}
func resolveChannel(ctx context.Context, api *tg.Client, id int64) (*tg.InputChannel, error) {
	if id <= -1000000000000 {
		id = -id - 1000000000000
	}
	if id <= 0 {
		return nil, errors.New("invalid storage channel")
	}
	r, e := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: id}})
	if e != nil {
		return nil, e
	}
	for _, v := range r.GetChats() {
		if ch, ok := v.(*tg.Channel); ok && ch.ID == id {
			return &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash}, nil
		}
	}
	return nil, errors.New("storage channel inaccessible")
}
func safeError(e error) string {
	if e == nil {
		return "none"
	}
	if r, ok := tgerr.As(e); ok {
		return r.Type
	}
	if errors.Is(e, context.Canceled) {
		return "canceled"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "timeout"
	}
	return "connection or transfer error"
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (w *Worker) noteError(e error) {
	if d, ok := tgerr.AsFloodWait(e); ok {
		w.cooldown.Store(time.Now().Add(d + time.Second).UnixNano())
	}
}
func (p *Pool) pick(upload bool) (*Worker, error) {
	n := len(p.workers)
	start := int(p.next.Add(1)-1) % n
	for i := 0; i < n; i++ {
		w := p.workers[(start+i)%n]
		if w.ready() && (!upload || w.busy.CompareAndSwap(false, true)) {
			return w, nil
		}
	}
	return nil, errors.New("no ready worker")
}
func (p *Pool) acquire(ctx context.Context) (*Worker, error) {
	for {
		if w, e := p.pick(true); e == nil {
			return w, nil
		}
		if e := pause(ctx, 250*time.Millisecond); e != nil {
			return nil, e
		}
	}
}
func (p *Pool) Stats() map[string]int {
	r := map[string]int{"total": len(p.workers), "ready": 0, "uploading": 0}
	for _, w := range p.workers {
		if w.ready() {
			r["ready"]++
		}
		if w.busy.Load() {
			r["uploading"]++
		}
	}
	return r
}
func (w *Worker) document(ctx context.Context, id int, refresh bool) (*tg.Document, error) {
	w.mu.Lock()
	v, ok := w.docs[id]
	api, ch := w.api, w.channel
	w.mu.Unlock()
	if !refresh && ok && time.Since(v.at) < 20*time.Minute {
		return v.doc, nil
	}
	r, e := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: ch, ID: []tg.InputMessageClass{&tg.InputMessageID{ID: id}}})
	if e != nil {
		return nil, e
	}
	list, ok := r.(interface{ GetMessages() []tg.MessageClass })
	if !ok {
		return nil, errors.New("missing document")
	}
	for _, v := range list.GetMessages() {
		if m, ok := v.(*tg.Message); ok {
			if media, ok := m.Media.(*tg.MessageMediaDocument); ok {
				if d, ok := media.Document.(*tg.Document); ok {
					w.mu.Lock()
					if len(w.docs) >= 256 {
						w.docs = map[int]docRef{}
					}
					w.docs[id] = docRef{d, time.Now()}
					w.mu.Unlock()
					return d, nil
				}
			}
		}
	}
	return nil, errors.New("missing document")
}
func (w *Worker) mediaAPI(ctx context.Context, dc int) (*tg.Client, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if inv, ok := w.dcs[dc]; ok {
		return tg.NewClient(inv), nil
	}
	inv, e := w.client.DC(ctx, dc, 1)
	if e != nil {
		return nil, e
	}
	w.dcs[dc] = inv
	return tg.NewClient(inv), nil
}
func (w *Worker) readBlock(ctx context.Context, p Part, offset int64) ([]byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		d, e := w.document(ctx, p.MessageID, attempt > 0)
		if e != nil {
			w.noteError(e)
			return nil, e
		}
		if d.Size != p.Size {
			return nil, errors.New("stored part size changed")
		}
		api, e := w.mediaAPI(ctx, d.DCID)
		if e != nil {
			return nil, e
		}
		r, e := api.UploadGetFile(ctx, &tg.UploadGetFileRequest{Location: &tg.InputDocumentFileLocation{ID: d.ID, AccessHash: d.AccessHash, FileReference: d.FileReference}, Offset: offset, Limit: int(chunkSize)})
		if e != nil {
			w.noteError(e)
			if tgerr.Is(e, "FILE_REFERENCE_EXPIRED", "FILE_REFERENCE_INVALID") {
				continue
			}
			return nil, e
		}
		f, ok := r.(*tg.UploadFile)
		if !ok {
			return nil, errors.New("unexpected Telegram redirect")
		}
		if int64(len(f.Bytes)) != min(chunkSize, p.Size-offset) {
			return nil, io.ErrUnexpectedEOF
		}
		return f.Bytes, nil
	}
	return nil, errors.New("file reference refresh failed")
}
func randomInt64() (int64, error) {
	var b [8]byte
	_, e := rand.Read(b[:])
	return int64(binary.LittleEndian.Uint64(b[:])), e
}

// A single retained 512 KiB buffer is retried until acknowledged. No payload is staged on disk.
func (w *Worker) upload(ctx context.Context, r io.Reader, size int64, name, mime string) (Part, error) {
	id, e := randomInt64()
	if e != nil {
		return Part{}, e
	}
	total := int((size + chunkSize - 1) / chunkSize)
	if total < 1 || total > 4000 {
		return Part{}, errors.New("unsupported slice size")
	}
	w.mu.Lock()
	api, ch := w.api, w.channel
	w.mu.Unlock()
	buf := make([]byte, chunkSize)
	digest := md5.New()
	big := size > 10*1024*1024
	for i := 0; i < total; i++ {
		n := int(min(chunkSize, size-int64(i)*chunkSize))
		if _, e = io.ReadFull(r, buf[:n]); e != nil {
			return Part{}, e
		}
		if !big {
			_, _ = digest.Write(buf[:n])
		}
		for retry := 0; ; retry++ {
			var ok bool
			rpcCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			if big {
				ok, e = api.UploadSaveBigFilePart(rpcCtx, &tg.UploadSaveBigFilePartRequest{FileID: id, FilePart: i, FileTotalParts: total, Bytes: buf[:n]})
			} else {
				ok, e = api.UploadSaveFilePart(rpcCtx, &tg.UploadSaveFilePartRequest{FileID: id, FilePart: i, Bytes: buf[:n]})
			}
			cancel()
			if e == nil && ok {
				break
			}
			w.noteError(e)
			delay := time.Second * time.Duration(retry+1)
			if d, yes := tgerr.AsFloodWait(e); yes {
				delay = d + time.Second
			} else if retry >= 3 {
				if e == nil {
					e = errors.New("upload not acknowledged")
				}
				return Part{}, e
			}
			if e = pause(ctx, delay); e != nil {
				return Part{}, e
			}
		}
	}
	var file tg.InputFileClass = &tg.InputFile{ID: id, Parts: total, Name: name, MD5Checksum: hex.EncodeToString(digest.Sum(nil))}
	if big {
		file = &tg.InputFileBig{ID: id, Parts: total, Name: name}
	}
	rid, e := randomInt64()
	if e != nil {
		return Part{}, e
	}
	request := &tg.MessagesSendMediaRequest{Peer: &tg.InputPeerChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}, RandomID: rid, Media: &tg.InputMediaUploadedDocument{File: file, MimeType: mime, Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: name}}, ForceFile: true}, Message: name}
	var updates tg.UpdatesClass
	for attempt := 0; ; attempt++ {
		call, cancel := context.WithTimeout(ctx, 60*time.Second)
		updates, e = api.MessagesSendMedia(call, request)
		cancel()
		if e == nil {
			break
		}
		w.noteError(e)
		delay := time.Second * time.Duration(attempt+1)
		if d, ok := tgerr.AsFloodWait(e); ok {
			delay = d + time.Second
		} else if attempt >= 3 {
			return Part{}, e
		}
		if e = pause(ctx, delay); e != nil {
			return Part{}, e
		}
	}
	for _, m := range updateMessages(updates) {
		if media, ok := m.Media.(*tg.MessageMediaDocument); ok {
			if d, ok := media.Document.(*tg.Document); ok && d.Size == size {
				return Part{m.ID, size}, nil
			}
		}
	}
	return Part{}, errors.New("upload committed without message confirmation")
}
func updateMessages(u tg.UpdatesClass) []*tg.Message {
	var updates []tg.UpdateClass
	switch v := u.(type) {
	case *tg.Updates:
		updates = v.Updates
	case *tg.UpdatesCombined:
		updates = v.Updates
	case *tg.UpdateShort:
		updates = []tg.UpdateClass{v.Update}
	}
	out := []*tg.Message{}
	for _, v := range updates {
		var mc tg.MessageClass
		switch x := v.(type) {
		case *tg.UpdateNewMessage:
			mc = x.Message
		case *tg.UpdateNewChannelMessage:
			mc = x.Message
		}
		if m, ok := mc.(*tg.Message); ok {
			out = append(out, m)
		}
	}
	return out
}
func (p *Pool) ReadBlock(ctx context.Context, part Part, offset int64) ([]byte, error) {
	var last error
	for i := 0; i < len(p.workers); i++ {
		w, e := p.pick(false)
		if e != nil {
			return nil, e
		}
		call, cancel := context.WithTimeout(ctx, 45*time.Second)
		b, e := w.readBlock(call, part, offset)
		cancel()
		if e == nil {
			return b, nil
		}
		last = e
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("workers could not read block: %w", last)
}
