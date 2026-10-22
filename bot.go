package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Bot struct {
	cfg    Config
	server *Server
	leech  *Leecher
	http   *http.Client
}
type botUser struct {
	ID int64 `json:"id"`
}
type botDocument struct {
	Size int64  `json:"file_size"`
	Name string `json:"file_name"`
	MIME string `json:"mime_type"`
}
type botMessage struct {
	ID   int     `json:"message_id"`
	From botUser `json:"from"`
	Chat struct {
		ID   int64  `json:"id"`
		Type string `json:"type"`
	} `json:"chat"`
	Text     string       `json:"text"`
	Document *botDocument `json:"document"`
	Video    *botDocument `json:"video"`
	Audio    *botDocument `json:"audio"`
}

func (b *Bot) call(ctx context.Context, method string, in, out any) error {
	body, e := json.Marshal(in)
	if e != nil {
		return e
	}
	r, e := http.NewRequestWithContext(ctx, "POST", "https://api.telegram.org/bot"+b.cfg.PrimaryToken+"/"+method, bytes.NewReader(body))
	if e != nil {
		return errors.New("invalid Telegram request")
	}
	r.Header.Set("Content-Type", "application/json")
	resp, e := b.http.Do(r)
	if e != nil {
		return errors.New("Telegram Bot API unreachable")
	}
	defer resp.Body.Close()
	var envelope struct {
		OK     bool            `json:"ok"`
		Code   int             `json:"error_code"`
		Result json.RawMessage `json:"result"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 8*1024*1024)).Decode(&envelope); e != nil {
		return errors.New("invalid Telegram response")
	}
	if !envelope.OK {
		return fmt.Errorf("Telegram Bot API status %d", envelope.Code)
	}
	if out != nil {
		return json.Unmarshal(envelope.Result, out)
	}
	return nil
}
func (b *Bot) reply(ctx context.Context, chat int64, text string) {
	call, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if e := b.call(call, "sendMessage", map[string]any{"chat_id": chat, "text": text, "link_preview_options": map[string]bool{"is_disabled": true}}, nil); e != nil {
		slog.Warn("command reply failed")
	}
}
func (b *Bot) links(m Manifest) string {
	return "Ready: " + m.Name + "\n\nWatch: " + b.server.link("/watch/", m.ID) + "\n\nStream: " + b.server.link("/inline/", m.ID) + "\n\nDownload: " + b.server.link("/dl/", m.ID)
}
func (b *Bot) Check(ctx context.Context) error {
	var me botUser
	if e := b.call(ctx, "getMe", map[string]any{}, &me); e != nil {
		return e
	}
	var webhook struct {
		URL string `json:"url"`
	}
	if e := b.call(ctx, "getWebhookInfo", map[string]any{}, &webhook); e != nil {
		return e
	}
	if webhook.URL != "" {
		return errors.New("primary bot has an existing webhook; remove it before polling")
	}
	var chat struct {
		Type string `json:"type"`
	}
	if e := b.call(ctx, "getChat", map[string]any{"chat_id": b.cfg.ChannelID}, &chat); e != nil {
		return errors.New("primary bot cannot access storage channel")
	}
	if chat.Type != "channel" && chat.Type != "supergroup" {
		return errors.New("storage chat must be a channel or supergroup")
	}
	var member struct {
		Status  string `json:"status"`
		CanPost bool   `json:"can_post_messages"`
	}
	if e := b.call(ctx, "getChatMember", map[string]any{"chat_id": b.cfg.ChannelID, "user_id": me.ID}, &member); e != nil {
		return errors.New("cannot verify primary channel permissions")
	}
	if chat.Type == "channel" && member.Status != "creator" && (member.Status != "administrator" || !member.CanPost) {
		return errors.New("primary bot needs channel post permission")
	}
	return nil
}
func (b *Bot) Run(ctx context.Context) error {
	if e := b.Check(ctx); e != nil {
		return e
	}
	var offset int
	offsetPath := filepath.Join(b.cfg.DataDir, "bot-offset.json")
	if raw, e := os.ReadFile(offsetPath); e == nil {
		if e = json.Unmarshal(raw, &offset); e != nil {
			return e
		}
	}
	for ctx.Err() == nil {
		var updates []struct {
			ID      int         `json:"update_id"`
			Message *botMessage `json:"message"`
		}
		e := b.call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": 25, "limit": 10, "allowed_updates": []string{"message"}}, &updates)
		if e != nil {
			slog.Warn("command polling retry", "reason", e.Error())
			if e = pause(ctx, 5*time.Second); e != nil {
				return e
			}
			continue
		}
		for _, u := range updates {
			offset = u.ID + 1
			if e = atomicJSON(offsetPath, offset); e != nil {
				return e
			}
			if u.Message == nil || u.Message.From.ID != b.cfg.OwnerID || u.Message.Chat.Type != "private" {
				continue
			}
			b.handle(ctx, *u.Message)
		}
	}
	return ctx.Err()
}
func (b *Bot) handle(ctx context.Context, m botMessage) {
	fields := strings.Fields(m.Text)
	cmd := ""
	if len(fields) > 0 {
		cmd = strings.Split(fields[0], "@")[0]
	}
	switch cmd {
	case "/start", "/help":
		b.reply(ctx, m.Chat.ID, "Send a document or video to create a link.\n/leech https://… — fetch a remote file\n/status — worker and job status\n/cancel — cancel the active leech\n/resume JOB_ID — resume a saved job\n/job JOB_ID — inspect a saved job\n/link FILE_ID — retrieve existing links\n/restore MESSAGE_ID — restore a channel manifest")
	case "/status":
		b.reply(ctx, m.Chat.ID, fmt.Sprintf("Workers: %d/%d ready\n%s", b.server.pool.Stats()["ready"], b.server.pool.Stats()["total"], b.leech.Status()))
	case "/cancel":
		if b.leech.Cancel() {
			b.reply(ctx, m.Chat.ID, "Cancellation requested.")
		} else {
			b.reply(ctx, m.Chat.ID, "No active leech job.")
		}
	case "/leech", "/resume":
		if len(fields) != 2 {
			b.reply(ctx, m.Chat.ID, "Usage: "+cmd+" "+map[string]string{"/leech": "https://example.com/file", "/resume": "JOB_ID"}[cmd])
			return
		}
		raw, resume := "", ""
		if cmd == "/leech" {
			raw = fields[1]
			if e := validateURL(raw); e != nil {
				b.reply(ctx, m.Chat.ID, e.Error())
				return
			}
		} else {
			resume = fields[1]
		}
		e := b.leech.Start(ctx, raw, resume, func(text string) {
			if strings.HasPrefix(text, "Ready: ") {
				id := strings.TrimPrefix(text, "Ready: ")
				if manifest, e := b.server.store.Get(id); e == nil {
					text = b.links(manifest)
				}
			}
			b.reply(ctx, m.Chat.ID, text)
		})
		if e != nil {
			b.reply(ctx, m.Chat.ID, e.Error())
		}
	case "/job":
		if len(fields) != 2 || !validID(fields[1]) {
			b.reply(ctx, m.Chat.ID, "Usage: /job JOB_ID")
			return
		}
		raw, e := os.ReadFile(b.leech.jobPath(fields[1]))
		if e != nil {
			b.reply(ctx, m.Chat.ID, "Job not found.")
			return
		}
		var j Job
		if json.Unmarshal(raw, &j) != nil {
			return
		}
		done := 0
		for _, p := range j.Parts {
			if p.MessageID > 0 {
				done++
			}
		}
		b.reply(ctx, m.Chat.ID, fmt.Sprintf("Job: %s\nState: %s\nParts: %d/%d\nLast result: %s", j.ID, j.State, done, len(j.Parts), j.Error))
	case "/restore":
		if len(fields) != 2 {
			b.reply(ctx, m.Chat.ID, "Usage: /restore MANIFEST_MESSAGE_ID")
			return
		}
		mid, e := strconv.Atoi(fields[1])
		if e != nil || mid < 1 {
			b.reply(ctx, m.Chat.ID, "Invalid message ID.")
			return
		}
		call, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		manifest, backupID, e := b.leech.restore(call, mid)
		if e != nil {
			b.reply(ctx, m.Chat.ID, "Manifest restoration failed: "+safeError(e))
			return
		}
		text := b.links(manifest)
		if backupID > 0 {
			text += fmt.Sprintf("\n\nRecovery manifest saved in the storage channel as message %d.", backupID)
		}
		b.reply(ctx, m.Chat.ID, text)

	case "/link":
		if len(fields) != 2 {
			b.reply(ctx, m.Chat.ID, "Usage: /link FILE_ID")
			return
		}
		manifest, e := b.server.store.Get(fields[1])
		if e != nil {
			b.reply(ctx, m.Chat.ID, "File not found.")
			return
		}
		b.reply(ctx, m.Chat.ID, b.links(manifest))
	default:
		doc := m.Document
		if doc == nil {
			doc = m.Video
		}
		if doc == nil {
			doc = m.Audio
		}
		if doc == nil {
			return
		}
		if doc.Size <= 0 {
			b.reply(ctx, m.Chat.ID, "This file has no usable size.")
			return
		}
		var copied struct {
			ID int `json:"message_id"`
		}
		if e := b.call(ctx, "copyMessage", map[string]any{"chat_id": b.cfg.ChannelID, "from_chat_id": m.Chat.ID, "message_id": m.ID}, &copied); e != nil {
			b.reply(ctx, m.Chat.ID, "Could not copy this file into storage.")
			return
		}
		id, e := randomID()
		if e != nil {
			return
		}
		name := doc.Name
		if name == "" {
			name = "file-" + strconv.Itoa(copied.ID)
		}
		mt := doc.MIME
		if mt == "" {
			mt = "application/octet-stream"
		}
		manifest := Manifest{Version: 1, ID: id, Name: name, MIME: mt, Size: doc.Size, Created: time.Now().UTC(), Parts: []Part{{MessageID: copied.ID, Size: doc.Size}}}
		if e = b.server.store.Save(manifest); e != nil {
			b.reply(ctx, m.Chat.ID, "File copied, but manifest storage failed.")
			return
		}
		b.reply(ctx, m.Chat.ID, b.links(manifest))
	}
}
