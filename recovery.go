package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gotd/td/tg"
	"log/slog"
)

// Backups contain only public file metadata and message IDs, never source URLs or credentials.
func (l *Leecher) backup(ctx context.Context, m Manifest) error {
	payload, e := json.Marshal(m)
	if e != nil {
		return e
	}
	w, e := l.pool.acquire(ctx)
	if e != nil {
		return e
	}
	defer w.busy.Store(false)
	part, e := w.upload(ctx, bytes.NewReader(payload), int64(len(payload)), m.ID+".manifest.json", "application/json")
	if e != nil {
		return e
	}
	w.mu.Lock()
	api, ch := w.api, w.channel
	w.mu.Unlock()
	peer := &tg.InputPeerChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}
	caption := "FILESTREAM_MANIFEST_V1 " + string(payload)
	if len([]rune(caption)) > 1000 {
		caption = fmt.Sprintf("FILESTREAM_MANIFEST_V1 %s; manifest_message_id=%d", m.ID, part.MessageID)
	}
	edit := &tg.MessagesEditMessageRequest{Peer: peer, ID: m.Parts[0].MessageID}
	edit.SetMessage(caption)
	if _, e = api.MessagesEditMessage(ctx, edit); e == nil {
		return nil
	}
	if _, e = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{Peer: peer, ID: part.MessageID, Silent: true}); e != nil {
		slog.Warn("manifest backed up but caption and pin unavailable", "manifest_message_id", part.MessageID)
	}
	return nil
}
func (l *Leecher) restore(ctx context.Context, messageID int) (Manifest, error) {
	var m Manifest
	w, e := l.pool.pick(false)
	if e != nil {
		return m, e
	}
	d, e := w.document(ctx, messageID, true)
	if e != nil {
		return m, e
	}
	if d.Size <= 0 || d.Size > 2*1024*1024 {
		return m, errors.New("manifest payload exceeds limit")
	}
	var payload bytes.Buffer
	part := Part{MessageID: messageID, Size: d.Size}
	for off := int64(0); off < d.Size; off += chunkSize {
		b, e := w.readBlock(ctx, part, off)
		if e != nil {
			return m, e
		}
		payload.Write(b)
	}
	if e = json.Unmarshal(payload.Bytes(), &m); e != nil {
		return m, errors.New("invalid manifest JSON")
	}
	if e = m.Validate(); e != nil {
		return m, e
	}
	if !validID(m.ID) {
		return m, errors.New("invalid manifest ID")
	}
	if _, e = l.store.Get(m.ID); e == nil {
		return m, errors.New("manifest already exists; use /link")
	}
	for _, p := range m.Parts {
		d, e := w.document(ctx, p.MessageID, true)
		if e != nil || d.Size != p.Size {
			return m, errors.New("manifest references an inaccessible or changed part")
		}
	}
	return m, l.store.Save(m)
}
