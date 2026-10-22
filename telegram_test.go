package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"io"
	"testing"
)

type mockUpload struct {
	data      []byte
	next      int
	maxBuffer int
	commits   int
	checksum  string
}

func (m *mockUpload) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	switch r := input.(type) {
	case *tg.UploadSaveFilePartRequest:
		if r.FilePart != m.next {
			return errors.New("wrong part order")
		}
		m.next++
		m.maxBuffer = max(m.maxBuffer, len(r.Bytes))
		m.data = append(m.data, r.Bytes...)
		output.(*tg.BoolBox).Bool = &tg.BoolTrue{}
	case *tg.UploadSaveBigFilePartRequest:
		if r.FilePart != m.next {
			return errors.New("wrong part order")
		}
		m.next++
		m.maxBuffer = max(m.maxBuffer, len(r.Bytes))
		m.data = append(m.data, r.Bytes...)
		output.(*tg.BoolBox).Bool = &tg.BoolTrue{}
	case *tg.MessagesSendMediaRequest:
		m.commits++
		d := r.Media.(*tg.InputMediaUploadedDocument)
		if f, ok := d.File.(*tg.InputFile); ok {
			m.checksum = f.MD5Checksum
		}
		output.(*tg.UpdatesBox).Updates = &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateNewChannelMessage{Message: &tg.Message{ID: 123, Media: &tg.MessageMediaDocument{Document: &tg.Document{Size: int64(len(m.data))}}}}}}
	default:
		return errors.New("unexpected RPC")
	}
	return nil
}
func TestUploadExactBytesAndBoundedChunks(t *testing.T) {
	for _, size := range []int{1, 524289, 11*1024*1024 + 7} {
		data := bytes.Repeat([]byte{37}, size)
		rpc := &mockUpload{}
		w := &Worker{api: tg.NewClient(rpc), channel: &tg.InputChannel{ChannelID: 1}}
		part, e := w.upload(context.Background(), bytes.NewReader(data), int64(size), "test.bin", "application/octet-stream")
		if e != nil || part.MessageID != 123 || part.Size != int64(size) || !bytes.Equal(data, rpc.data) {
			t.Fatalf("size=%d part=%+v err=%v", size, part, e)
		}
		if rpc.maxBuffer > int(chunkSize) || rpc.commits != 1 {
			t.Fatal("unbounded chunk or duplicate commit")
		}
		if size <= 10*1024*1024 {
			sum := md5.Sum(data)
			if rpc.checksum != hex.EncodeToString(sum[:]) {
				t.Fatal("bad small-file checksum")
			}
		}
	}
}
func TestShortUploadNeverCommits(t *testing.T) {
	rpc := &mockUpload{}
	w := &Worker{api: tg.NewClient(rpc), channel: &tg.InputChannel{ChannelID: 1}}
	_, e := w.upload(context.Background(), bytes.NewReader([]byte("short")), 100, "test.bin", "application/octet-stream")
	if !errors.Is(e, io.ErrUnexpectedEOF) || rpc.commits != 0 {
		t.Fatalf("%v commits=%d", e, rpc.commits)
	}
}
func TestPoolSkipsUnreadyAndBusy(t *testing.T) {
	a, b := &Worker{}, &Worker{}
	b.connected.Store(true)
	b.authenticated.Store(true)
	p := &Pool{workers: []*Worker{a, b}}
	w, e := p.pick(true)
	if e != nil || w != b {
		t.Fatal("wrong worker")
	}
	if _, e = p.pick(true); e == nil {
		t.Fatal("leased busy worker")
	}
	b.busy.Store(false)
	b.connected.Store(false)
	if _, e = p.pick(false); e == nil {
		t.Fatal("selected disconnected worker")
	}
}
