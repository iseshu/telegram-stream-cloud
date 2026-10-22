package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicAddresses(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "100.100.100.200", "::1", "::ffff:127.0.0.1", "fc00::1", "2002:7f00:1::", "64:ff9b::7f00:1", "192.0.2.1"} {
		if publicIP(net.ParseIP(ip)) {
			t.Fatalf("allowed %s", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "2606:4700:4700::1111"} {
		if !publicIP(net.ParseIP(ip)) {
			t.Fatalf("blocked %s", ip)
		}
	}
}
func TestRemoteProbeWithoutHead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			w.WriteHeader(405)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Range", "bytes 0-0/123")
		w.WriteHeader(206)
		w.Write([]byte{'a'})
	}))
	defer srv.Close()
	src, e := inspectRemote(context.Background(), srv.Client(), srv.URL+"/file.bin")
	if e != nil || src.Size != 123 || !src.Ranges || !src.stable() {
		t.Fatalf("%+v %v", src, e)
	}
}
func TestRemoteIgnoredRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		if r.Method != "HEAD" {
			io.WriteString(w, "hello")
		}
	}))
	defer srv.Close()
	src, e := inspectRemote(context.Background(), srv.Client(), srv.URL+"/file.bin")
	if e != nil || src.Ranges || src.Size != 5 {
		t.Fatalf("%+v %v", src, e)
	}
}
func TestRemoteChangedValidator(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"changed"`)
		w.Header().Set("Content-Range", "bytes 0-0/2")
		w.WriteHeader(206)
		io.WriteString(w, "a")
	}))
	defer srv.Close()
	src := Remote{URL: srv.URL, Size: 2, ETag: `"original"`}
	if body, e := src.open(context.Background(), srv.Client(), 0, 1, true); e == nil {
		body.Close()
		t.Fatal("accepted changed source")
	}
}
func TestSSRFBlockedAtDial(t *testing.T) {
	c := remoteClient()
	defer c.CloseIdleConnections()
	_, e := c.Get("http://127.0.0.1:1/")
	if e == nil || !strings.Contains(e.Error(), "blocked") {
		t.Fatalf("unexpected %v", e)
	}
}
func TestSourceName(t *testing.T) {
	if got := sourceName("https://example.com/file", `attachment; filename="../../secret.txt"`); got != "secret.txt" {
		t.Fatal(got)
	}
}
func TestContentRangeRejectsGarbage(t *testing.T) {
	for _, v := range []string{"bytes 0-1/1", "bytes 0-0/4 trailing", "bytes -1-0/4", "items 0-0/4"} {
		if _, _, _, e := contentRange(v); e == nil {
			t.Fatal(v)
		}
	}
}
