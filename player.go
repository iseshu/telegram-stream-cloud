package main

import (
	"bytes"
	"embed"
	"html/template"
	"net/http"
	"strings"
	"time"
)

//go:embed web
var playerFiles embed.FS
var playerTemplate = template.Must(template.ParseFS(playerFiles, "web/player.html"))

func playerAsset(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/assets/")
	// Only expose the player assets; never directory listings or the template.
	switch name {
	case "player.css", "player.js", "vendor/jplayer/plyr.css", "vendor/jplayer/plyr.min.js", "vendor/jplayer/plyr.svg":
	default:
		http.NotFound(w, r)
		return
	}
	data, err := playerFiles.ReadFile("web/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "public, max-age=3600, must-revalidate")
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}
