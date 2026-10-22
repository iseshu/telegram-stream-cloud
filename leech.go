package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	ID       string
	Source   Remote
	State    string
	Parts    []Part
	PartSize int64
	Updated  time.Time
	Error    string
}
type Leecher struct {
	remote func() *http.Client
	wg     sync.WaitGroup
	cfg    Config
	pool   *Pool
	store  *Store
	mu     sync.Mutex
	cancel context.CancelFunc
	active string
	disk   chan struct{}
}

func (l *Leecher) client() *http.Client {
	if l.remote != nil {
		return l.remote()
	}
	return remoteClient()
}

func (l *Leecher) jobPath(id string) string { return filepath.Join(l.cfg.DataDir, "jobs", id+".json") }
func (l *Leecher) persist(j *Job) error {
	j.Updated = time.Now().UTC()
	return atomicJSON(l.jobPath(j.ID), j)
}
func (l *Leecher) Init() error {
	for _, d := range []string{"jobs", "fallback"} {
		if e := os.MkdirAll(filepath.Join(l.cfg.DataDir, d), 0700); e != nil {
			return e
		}
	}
	files, _ := filepath.Glob(filepath.Join(l.cfg.DataDir, "fallback", "slice-*"))
	for _, f := range files {
		if e := os.Remove(f); e != nil {
			return e
		}
	}
	files, _ = filepath.Glob(filepath.Join(l.cfg.DataDir, "jobs", "*.json"))
	for _, p := range files {
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		var j Job
		if e = json.Unmarshal(b, &j); e != nil {
			return e
		}
		if j.State == "running" {
			j.State = "interrupted"
			j.Error = "service restarted; resume explicitly"
			if e = l.persist(&j); e != nil {
				return e
			}
		}
	}
	return nil
}
func (l *Leecher) Start(ctx context.Context, raw, resume string, notify func(string)) error {
	l.mu.Lock()
	if ctx.Err() != nil {
		l.mu.Unlock()
		return ctx.Err()
	}
	if l.active != "" {
		l.mu.Unlock()
		return errors.New("a leech job is already active")
	}
	id := resume
	var e error
	if id == "" {
		id, e = randomID()
	} else if !validID(id) {
		e = errors.New("invalid job ID")
	}
	if e != nil {
		l.mu.Unlock()
		return e
	}
	ctx, cancel := context.WithCancel(ctx)
	l.active = id
	l.cancel = cancel
	l.wg.Add(1)
	l.mu.Unlock()
	go func() {
		defer l.wg.Done()
		defer func() { cancel(); l.mu.Lock(); l.active = ""; l.cancel = nil; l.mu.Unlock() }()
		notify("Leech started. Job: " + id)
		m, e := l.run(ctx, id, raw, resume != "")
		if e != nil {
			notify("Leech stopped: " + safeLeechError(e) + ". Job: " + id)
			return
		}
		notify("Ready: " + m.ID)
	}()
	return nil
}
func safeLeechError(e error) string {
	if errors.Is(e, context.Canceled) {
		return "canceled"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "timed out"
	}
	return "source or Telegram transfer failed; use /status for the saved job state"
}
func (l *Leecher) Cancel() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel == nil {
		return false
	}
	l.cancel()
	return true
}
func (l *Leecher) Status() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active == "" {
		return "No active leech job."
	}
	return "Active job: " + l.active
}
func (l *Leecher) run(ctx context.Context, id, raw string, resume bool) (m Manifest, err error) {
	client := l.client()
	defer client.CloseIdleConnections()
	j := Job{ID: id, PartSize: l.cfg.PartSize, State: "running"}
	if resume {
		b, e := os.ReadFile(l.jobPath(id))
		if e != nil {
			return m, e
		}
		if e = json.Unmarshal(b, &j); e != nil {
			return m, e
		}
		if j.State == "complete" {
			return l.store.Get(id)
		}
		if j.PartSize != l.cfg.PartSize {
			return m, errors.New("part size changed")
		}
		raw = j.Source.URL
	}

	if !resume {
		j.Source.URL = raw
	}
	j.State = "running"
	j.Error = ""
	if e := l.persist(&j); e != nil {
		return m, e
	}
	defer func() {
		if err != nil {
			j.State = "failed"
			if errors.Is(err, context.Canceled) {
				j.State = "canceled"
			}
			j.Error = safeError(err)
			_ = l.persist(&j)
		}
	}()
	src, e := inspectRemote(ctx, client, raw)
	if e != nil {
		return m, e
	}
	if resume && j.Source.Size > 0 && !j.Source.matches(src) {
		return m, errors.New("source cannot be safely resumed")
	}
	j.Source = src
	if src.Size > j.PartSize*10000 {
		return m, errors.New("source exceeds manifest part limit")
	}
	count := int((src.Size + j.PartSize - 1) / j.PartSize)
	if len(j.Parts) == 0 {
		j.Parts = make([]Part, count)
	}
	if len(j.Parts) != count {
		return m, errors.New("invalid job state")
	}
	if e = l.persist(&j); e != nil {
		return m, e
	}
	var persistMu sync.Mutex
	save := func(i int, p Part) error {
		persistMu.Lock()
		defer persistMu.Unlock()
		j.Parts[i] = p
		return l.persist(&j)
	}
	upload := func(ctx context.Context, i int, r io.Reader) (Part, error) {
		w, e := l.pool.acquire(ctx)
		if e != nil {
			return Part{}, e
		}
		defer w.busy.Store(false)
		size := min(j.PartSize, src.Size-int64(i)*j.PartSize)
		name := src.Name
		if count > 1 {
			name = fmt.Sprintf("%s.part%05d", src.Name, i+1)
		}
		return w.upload(ctx, r, size, name, src.MIME)
	}
	if src.Ranges && src.stable() {
		group, cancel := context.WithCancel(ctx)
		defer cancel()
		indices := make(chan int)
		var wg sync.WaitGroup
		var first error
		var errMu sync.Mutex
		for n := 0; n < len(l.pool.workers); n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range indices {
					start := int64(i) * j.PartSize
					size := min(j.PartSize, src.Size-start)
					var part Part
					body, e := src.open(group, client, start, size, true)
					if e == nil {
						part, e = upload(group, i, body)
						body.Close()
					}
					if e != nil && group.Err() == nil {
						part, e = l.fallback(group, src, start, size, func(r io.Reader) (Part, error) { return upload(group, i, r) })
					}
					if e == nil {
						e = save(i, part)
					}
					if e != nil {
						errMu.Lock()
						if first == nil {
							first = e
						}
						errMu.Unlock()
						cancel()
						return
					}
				}
			}()
		}
	loop:
		for i, p := range j.Parts {
			if p.MessageID != 0 {
				continue
			}
			select {
			case indices <- i:
			case <-group.Done():
				break loop
			}
		}
		close(indices)
		wg.Wait()
		if first != nil {
			return m, first
		}
		if ctx.Err() != nil {
			return m, ctx.Err()
		}
	} else {
		body, e := src.open(ctx, client, 0, src.Size, false)
		if e != nil {
			return m, e
		}
		defer func() {
			if body != nil {
				body.Close()
			}
		}()
		for i, p := range j.Parts {
			size := min(j.PartSize, src.Size-int64(i)*j.PartSize)
			if p.MessageID != 0 {
				if _, e = io.CopyN(io.Discard, body, size); e != nil {
					return m, e
				}
				continue
			}
			p, e = upload(ctx, i, io.LimitReader(body, size))
			if e != nil && ctx.Err() == nil && src.stable() {
				body.Close()
				p, e = l.fallback(ctx, src, int64(i)*j.PartSize, size, func(r io.Reader) (Part, error) { return upload(ctx, i, r) })
				if e == nil && i+1 < count {
					body, e = src.open(ctx, client, 0, src.Size, false)
					if e == nil {
						_, e = io.CopyN(io.Discard, body, int64(i)*j.PartSize+size)
					}
				}
			}
			if e != nil {
				return m, e
			}
			if e = save(i, p); e != nil {
				return m, e
			}
		}
	}
	m = Manifest{Version: 1, ID: id, Name: src.Name, MIME: src.MIME, Size: src.Size, Created: time.Now().UTC(), Parts: j.Parts}
	if e = l.backup(ctx, m); e != nil {
		return m, e
	}
	if e = l.store.Save(m); e != nil {
		return m, e
	}
	j.State = "complete"
	if e = l.persist(&j); e != nil {
		return m, e
	}
	return m, nil
}
func (l *Leecher) fallback(ctx context.Context, src Remote, start, size int64, upload func(io.Reader) (Part, error)) (Part, error) {
	select {
	case l.disk <- struct{}{}:
		defer func() { <-l.disk }()
	case <-ctx.Done():
		return Part{}, ctx.Err()
	}
	dir := filepath.Join(l.cfg.DataDir, "fallback")
	var stat syscall.Statfs_t
	if e := syscall.Statfs(dir, &stat); e != nil {
		return Part{}, e
	}
	if uint64(size+128*1024*1024) > stat.Bavail*uint64(stat.Bsize) {
		return Part{}, errors.New("insufficient fallback disk space")
	}
	f, e := os.CreateTemp(dir, "slice-*")
	if e != nil {
		return Part{}, e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	client := l.client()
	defer client.CloseIdleConnections()
	var copied int64
	buf := make([]byte, chunkSize)
	for attempt := 0; copied < size && attempt < 4; attempt++ {
		body, e := src.open(ctx, client, start+copied, size-copied, src.Ranges)
		if e != nil {
			return Part{}, e
		}
		if !src.Ranges {
			if _, e = io.CopyN(io.Discard, body, start+copied); e != nil {
				body.Close()
				return Part{}, e
			}
		}
		n, copyErr := io.CopyBuffer(f, io.LimitReader(body, size-copied), buf)
		body.Close()
		copied += n
		if copied == size {
			break
		}
		if copyErr == nil {
			copyErr = io.ErrUnexpectedEOF
		}
		if attempt == 3 {
			return Part{}, copyErr
		}
		if e = pause(ctx, time.Second*time.Duration(attempt+1)); e != nil {
			return Part{}, e
		}
	}
	if copied != size {
		return Part{}, io.ErrUnexpectedEOF
	}
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return Part{}, e
	}
	return upload(f)
}
