package main

import (
	"fmt"
	"net/http"
)

func (s *Server) home(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ready, total := 0, 0
	if s.pool != nil {
		stats := s.pool.Stats()
		ready, total = stats["ready"], stats["total"]
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = fmt.Fprintf(w, `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="dark"><title>Telegram Stream Cloud</title><style>*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px;background:#101218;color:#eef0f7;font:16px system-ui,sans-serif}.card{width:min(100%%,620px);padding:clamp(28px,6vw,52px);border:1px solid #30374a;border-radius:18px;background:#171b26;box-shadow:0 24px 80px #0005}.brand{font-size:12px;letter-spacing:.16em;color:#aeb7cf}h1{font-size:clamp(30px,7vw,48px);line-height:1.1;margin:26px 0 14px}p{color:#bac2d6;line-height:1.7}.actions{display:flex;gap:12px;flex-wrap:wrap;margin:28px 0}.actions a{padding:12px 17px;border-radius:9px;background:#718bff;color:#080f28;text-decoration:none;font-weight:650}.actions a.secondary{background:#252c3d;color:#eef0f7;border:1px solid #414b64}.status{font-size:13px;color:#9ca8c4}.dot{display:inline-block;width:8px;height:8px;border-radius:50%%;background:#62d7a0;margin-right:8px}</style></head><body><main class="card"><div class="brand">TELEGRAM STREAM CLOUD</div><h1>Turn files into links.</h1><p>Send a video or document to the Telegram bot to get a watch page, direct stream link, and download link.</p><div class="actions"><a href="https://t.me/f2l_iseshubot">Open Telegram bot</a><a class="secondary" href="/health">Service health</a></div><div class="status"><span class="dot"></span>Service online · %d of %d worker bots ready</div></main></body></html>`, ready, total)
}
