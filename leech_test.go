package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFallbackCleanupAndExactSlice(t *testing.T) {
	all := []byte("abcdefghijklmnopqrstuvwxyz")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a, b, _, e := parseRange(r.Header.Get("Range"), int64(len(all)))
		if e != nil {
			t.Error(e)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(all)))
		w.WriteHeader(206)
		w.Write(all[a : b+1])
	}))
	defer srv.Close()
	for _, fail := range []bool{false, true} {
		dir := t.TempDir()
		l := &Leecher{cfg: Config{DataDir: dir}, disk: make(chan struct{}, 1), remote: func() *http.Client { return srv.Client() }}
		if e := l.Init(); e != nil {
			t.Fatal(e)
		}
		src := Remote{URL: srv.URL, Size: int64(len(all)), Ranges: true, ETag: `"v1"`}
		_, e := l.fallback(context.Background(), src, 5, 12, func(r io.Reader) (Part, error) {
			b, e := io.ReadAll(r)
			if e != nil || !bytes.Equal(b, all[5:17]) {
				t.Fatalf("bad slice %q %v", b, e)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "fallback", "slice-*"))
			if len(files) != 1 {
				t.Fatal("expected isolated slice")
			}
			if fail {
				return Part{}, errors.New("upload failed")
			}
			return Part{1, 12}, nil
		})
		if (e != nil) != fail {
			t.Fatal(e)
		}
		files, _ := os.ReadDir(filepath.Join(dir, "fallback"))
		if len(files) != 0 {
			t.Fatal("temporary slice leaked")
		}
	}
}
func TestStartupMarksInterruptedJob(t *testing.T) {
	dir := t.TempDir()
	l := &Leecher{cfg: Config{DataDir: dir}, disk: make(chan struct{}, 1)}
	if e := l.Init(); e != nil {
		t.Fatal(e)
	}
	id, _ := randomID()
	j := &Job{ID: id, State: "running"}
	if e := l.persist(j); e != nil {
		t.Fatal(e)
	}
	tmp := filepath.Join(dir, "fallback", "slice-abandoned")
	os.WriteFile(tmp, []byte("partial"), 0600)
	if e := l.Init(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(tmp); !os.IsNotExist(e) {
		t.Fatal("abandoned slice remains")
	}
	b, e := os.ReadFile(l.jobPath(id))
	if e != nil || !bytes.Contains(b, []byte("interrupted")) {
		t.Fatal("job not marked interrupted")
	}
}

type transferRPC struct {
	mu       sync.Mutex
	files    map[int64][]byte
	messages map[int][]byte
	next     int
	workers  map[int]bool
}
type workerRPC struct {
	shared *transferRPC
	index  int
}

func (r workerRPC) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	m := r.shared
	m.mu.Lock()
	defer m.mu.Unlock()
	switch v := input.(type) {
	case *tg.UploadSaveFilePartRequest:
		m.files[v.FileID] = append(m.files[v.FileID], v.Bytes...)
		m.workers[r.index] = true
		output.(*tg.BoolBox).Bool = &tg.BoolTrue{}
	case *tg.MessagesSendMediaRequest:
		f := v.Media.(*tg.InputMediaUploadedDocument).File.(*tg.InputFile)
		data := m.files[f.ID]
		m.next++
		id := m.next
		m.messages[id] = data
		output.(*tg.UpdatesBox).Updates = &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: &tg.Message{ID: id, Media: &tg.MessageMediaDocument{Document: &tg.Document{Size: int64(len(data))}}}}}}
	case *tg.MessagesEditMessageRequest:
		output.(*tg.UpdatesBox).Updates = &tg.Updates{}
	default:
		return errors.New("unexpected RPC")
	}
	return nil
}
func TestParallelLeechProducesOrderedManifest(t *testing.T) {
	data := make([]byte, 2_500_017)
	for i := range data {
		data[i] = byte(i % 251)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"fixed"`)
		w.Header().Set("Content-Type", "video/mp4")
		if r.Method == "HEAD" {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		a, b, partial, e := parseRange(r.Header.Get("Range"), int64(len(data)))
		if e != nil {
			w.WriteHeader(416)
			return
		}
		if partial {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(data)))
			w.WriteHeader(206)
		}
		w.Write(data[a : b+1])
	}))
	defer srv.Close()
	dir := t.TempDir()
	store, _ := newStore(dir)
	rpc := &transferRPC{files: map[int64][]byte{}, messages: map[int][]byte{}, workers: map[int]bool{}}
	pool := &Pool{}
	for i := 0; i < 3; i++ {
		w := &Worker{api: tg.NewClient(workerRPC{rpc, i}), channel: &tg.InputChannel{ChannelID: 1}}
		w.connected.Store(true)
		w.authenticated.Store(true)
		pool.workers = append(pool.workers, w)
	}
	l := &Leecher{cfg: Config{DataDir: dir, PartSize: 1_000_000}, pool: pool, store: store, disk: make(chan struct{}, 1), remote: func() *http.Client { return srv.Client() }}
	if e := l.Init(); e != nil {
		t.Fatal(e)
	}
	id, _ := randomID()
	m, e := l.run(context.Background(), id, srv.URL+"/video.mp4", false)
	if e != nil {
		t.Fatal(e)
	}
	var rebuilt []byte
	for _, p := range m.Parts {
		rebuilt = append(rebuilt, rpc.messages[p.MessageID]...)
	}
	if !bytes.Equal(data, rebuilt) {
		t.Fatal("stitched upload differs from source")
	}
	if len(rpc.workers) != 3 {
		t.Fatalf("used %d workers", len(rpc.workers))
	}
	if _, e = store.Get(id); e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "fallback"))
	if len(entries) != 0 {
		t.Fatal("normal transfer wrote payload files")
	}
}
