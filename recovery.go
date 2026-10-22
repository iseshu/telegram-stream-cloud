package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/gotd/td/tg"
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

// backupStandalone stores a recoverable manifest without modifying a media message's caption.
func (l *Leecher) backupStandalone(ctx context.Context, m Manifest) (int, error) {
	payload, e := json.Marshal(m)
	if e != nil {
		return 0, e
	}
	w, e := l.pool.acquire(ctx)
	if e != nil {
		return 0, e
	}
	defer w.busy.Store(false)
	part, e := w.upload(ctx, bytes.NewReader(payload), int64(len(payload)), m.ID+".manifest.json", "application/json")
	if e != nil {
		return 0, e
	}
	w.mu.Lock()
	api, ch := w.api, w.channel
	w.mu.Unlock()
	peer := &tg.InputPeerChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}
	if _, e = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{Peer: peer, ID: part.MessageID, Silent: true}); e != nil {
		return part.MessageID, e
	}
	return part.MessageID, nil
}

func documentName(d *tg.Document) string {
	for _, attr := range d.Attributes {
		if f, ok := attr.(*tg.DocumentAttributeFilename); ok && f.FileName != "" {
			return filepath.Base(strings.ReplaceAll(f.FileName, "\\", "/"))
		}
	}
	return ""
}

func (l *Leecher) restore(ctx context.Context, messageID int) (Manifest, int, error) {
	var m Manifest
	w, e := l.pool.pick(false)
	if e != nil {
		return m, 0, e
	}
	d, e := w.document(ctx, messageID, true)
	if e != nil {
		return m, 0, e
	}
	name := documentName(d)
	isManifest := d.MimeType == "application/json" || strings.HasSuffix(strings.ToLower(name), ".manifest.json")
	if !isManifest {
		if d.Size <= 0 {
			return m, 0, errors.New("storage message has no usable file size")
		}
		if name == "" {
			name = fmt.Sprintf("file-%d", messageID)
		}
		mime := d.MimeType
		if mime == "" {
			mime = "application/octet-stream"
		}
		id, e := randomID()
		if e != nil {
			return m, 0, e
		}
		m = Manifest{Version: 1, ID: id, Name: name, MIME: mime, Size: d.Size, Parts: []Part{{MessageID: messageID, Size: d.Size}}}
		if e = l.store.Save(m); e != nil {
			return m, 0, e
		}
		backupID, backupErr := l.backupStandalone(ctx, m)
		if backupErr != nil {
			slog.Warn("restored file link created but standalone manifest backup failed", "reason", safeError(backupErr))
		}
		return m, backupID, nil
	}
	if d.Size <= 0 || d.Size > 2*1024*1024 {
		return m, 0, errors.New("manifest payload exceeds limit")
	}
	var payload bytes.Buffer
	part := Part{MessageID: messageID, Size: d.Size}
	for off := int64(0); off < d.Size; off += chunkSize {
		b, e := w.readBlock(ctx, part, off)
		if e != nil {
			return m, 0, e
		}
		payload.Write(b)
	}
	if e = json.Unmarshal(payload.Bytes(), &m); e != nil {
		return m, 0, errors.New("invalid manifest JSON")
	}
	if e = m.Validate(); e != nil {
		return m, 0, e
	}
	if !validID(m.ID) {
		return m, 0, errors.New("invalid manifest ID")
	}
	if _, e = l.store.Get(m.ID); e == nil {
		return m, 0, errors.New("manifest already exists; use /link")
	}
	for _, p := range m.Parts {
		d, e := w.document(ctx, p.MessageID, true)
		if e != nil || d.Size != p.Size {
			return m, 0, errors.New("manifest references an inaccessible or changed part")
		}
	}
	return m, 0, l.store.Save(m)
}
