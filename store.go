package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Part struct {
	MessageID int   `json:"message_id"`
	Size      int64 `json:"size"`
}
type Manifest struct {
	Version int       `json:"version"`
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	MIME    string    `json:"mime"`
	Size    int64     `json:"size"`
	Created time.Time `json:"created"`
	Parts   []Part    `json:"parts"`
}

func (m Manifest) Validate() error {
	if m.Version != 1 || len(m.Parts) == 0 || m.Size <= 0 {
		return errors.New("invalid manifest")
	}
	var total int64
	for _, p := range m.Parts {
		if p.Size <= 0 || p.MessageID <= 0 || p.Size > m.Size-total {
			return errors.New("invalid part")
		}
		total += p.Size
	}
	if total != m.Size {
		return errors.New("manifest size mismatch")
	}
	return nil
}

type Store struct {
	dir string
	mu  sync.Mutex
}

func newStore(dir string) (*Store, error) {
	dir = filepath.Join(dir, "files")
	return &Store{dir: dir}, os.MkdirAll(dir, 0700)
}
func randomID() (string, error) {
	b := make([]byte, 24)
	_, e := rand.Read(b)
	return hex.EncodeToString(b), e
}
func atomicJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return atomicBytes(path, b)
}
func atomicBytes(path string, b []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (s *Store) Save(m Manifest) error {
	if e := m.Validate(); e != nil {
		return e
	}
	if !validID(m.ID) {
		return errors.New("invalid file ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return atomicJSON(filepath.Join(s.dir, m.ID+".json"), m)
}
func validID(id string) bool {
	if len(id) != 48 {
		return false
	}
	_, e := hex.DecodeString(id)
	return e == nil
}
func (s *Store) Get(id string) (Manifest, error) {
	var m Manifest
	if !validID(id) {
		return m, os.ErrNotExist
	}
	b, e := os.ReadFile(filepath.Join(s.dir, id+".json"))
	if e != nil {
		return m, e
	}
	if e = json.Unmarshal(b, &m); e != nil {
		return m, e
	}
	return m, m.Validate()
}

type Segment struct {
	Part           Part
	Offset, Length int64
}

func segments(m Manifest, start, end int64) []Segment {
	out := []Segment{}
	var base int64
	for _, p := range m.Parts {
		lo := max(start, base)
		hi := min(end, base+p.Size-1)
		if lo <= hi {
			out = append(out, Segment{p, lo - base, hi - lo + 1})
		}
		base += p.Size
	}
	return out
}
