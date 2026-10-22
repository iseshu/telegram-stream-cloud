package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestJPlayerPageAndAssets(t *testing.T) {
	s, m, _ := testServer(t)
	page := httptest.NewRecorder()
	s.Handler().ServeHTTP(page, httptest.NewRequest("GET", s.link("/watch/", m.ID), nil))
	if page.Code != 200 {
		t.Fatal(page.Code)
	}
	for _, want := range []string{"/assets/vendor/jplayer/plyr.min.js", "data-stream=", s.link("/inline/", m.ID), s.link("/dl/", m.ID)} {
		if !strings.Contains(page.Body.String(), want) {
			t.Errorf("page missing %q", want)
		}
	}
	if strings.Contains(page.Header().Get("Content-Security-Policy"), "script-src 'unsafe-inline'") {
		t.Fatal("inline scripts allowed")
	}
	for name, mime := range map[string]string{"player.js": "javascript", "player.css": "text/css", "vendor/jplayer/plyr.min.js": "javascript", "vendor/jplayer/plyr.css": "text/css", "vendor/jplayer/plyr.svg": "image/svg+xml"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/assets/"+name, nil))
		if w.Code != 200 || w.Body.Len() == 0 || !strings.Contains(w.Header().Get("Content-Type"), mime) {
			t.Errorf("asset %s: %d %s", name, w.Code, w.Header())
		}
		h := httptest.NewRecorder()
		s.Handler().ServeHTTP(h, httptest.NewRequest("HEAD", "/assets/"+name, nil))
		if h.Code != 200 || h.Body.Len() != 0 {
			t.Errorf("HEAD %s failed", name)
		}
	}
	for _, name := range []string{"", "player.html", ".env", "vendor/jplayer/"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/assets/"+name, nil))
		if w.Code != 404 {
			t.Errorf("unexpected exposed asset %s", name)
		}
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/assets/player.js", nil))
	if w.Code != 405 {
		t.Fatal(w.Code)
	}
}
