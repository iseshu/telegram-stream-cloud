package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"strings"
	"time"
)

type idleConn struct{ net.Conn }

func (c idleConn) Read(b []byte) (int, error) {
	_ = c.SetReadDeadline(time.Now().Add(45 * time.Second))
	return c.Conn.Read(b)
}

var blockedNetworks = []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "::/96", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/32", "2001:db8::/32", "2002::/16", "fc00::/7", "fe80::/10", "ff00::/8"}

func publicIP(ip net.IP) bool {
	a, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	a = a.Unmap()
	if !a.IsGlobalUnicast() {
		return false
	}
	for _, s := range blockedNetworks {
		if netip.MustParsePrefix(s).Contains(a) {
			return false
		}
	}
	return true
}
func validateURL(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("only public HTTP(S) URLs without embedded credentials are supported")
	}
	return nil
}
func remoteClient() *http.Client {
	tr := &http.Transport{Proxy: nil, DisableCompression: true, MaxIdleConns: 64, MaxIdleConnsPerHost: 32, IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: 30 * time.Second, TLSHandshakeTimeout: 15 * time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		ips, e := net.DefaultResolver.LookupIPAddr(ctx, host)
		if e != nil {
			return nil, errors.New("source DNS lookup failed")
		}
		if len(ips) == 0 {
			return nil, errors.New("source has no addresses")
		}
		for _, ip := range ips {
			if !publicIP(ip.IP) {
				return nil, errors.New("private or reserved source address blocked")
			}
		}
		var last error
		for _, ip := range ips {
			c, e := (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if e == nil {
				return idleConn{c}, nil
			}
			last = e
		}
		return nil, last
	}}
	return &http.Client{Transport: tr, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return errors.New("too many redirects")
		}
		return validateURL(r.URL.String())
	}}
}

type Remote struct {
	URL, Name, MIME, ETag, Modified string
	Size                            int64
	Ranges                          bool
}

func requestSource(ctx context.Context, c *http.Client, method, raw, br string) (*http.Response, error) {
	if e := validateURL(raw); e != nil {
		return nil, e
	}
	r, e := http.NewRequestWithContext(ctx, method, raw, nil)
	if e != nil {
		return nil, e
	}
	r.Header.Set("Accept-Encoding", "identity")
	r.Header.Set("User-Agent", "FileStream/1.0")
	if br != "" {
		r.Header.Set("Range", br)
	}
	resp, e := c.Do(r)
	if e != nil {
		return nil, errors.New("source request failed")
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		resp.Body.Close()
		return nil, errors.New("encoded sources are unsupported")
	}
	return resp, nil
}
func contentRange(s string) (start, end, total int64, err error) {
	if _, err = fmt.Sscanf(s, "bytes %d-%d/%d", &start, &end, &total); err != nil {
		return
	}
	if start < 0 || end < start || total <= end || s != fmt.Sprintf("bytes %d-%d/%d", start, end, total) {
		err = errors.New("invalid Content-Range")
	}
	return
}
func inspectRemote(ctx context.Context, c *http.Client, raw string) (Remote, error) {
	out := Remote{URL: raw}
	head, e := requestSource(ctx, c, "HEAD", raw, "")
	if e == nil {
		head.Body.Close()
		if head.StatusCode >= 200 && head.StatusCode < 300 {
			out.Size = head.ContentLength
			out.MIME = head.Header.Get("Content-Type")
			out.ETag = head.Header.Get("ETag")
			out.Modified = head.Header.Get("Last-Modified")
			out.Name = sourceName(raw, head.Header.Get("Content-Disposition"))
		}
	}
	// A probe confirms range semantics even when Accept-Ranges is absent or misleading.
	probe, e := requestSource(ctx, c, "GET", raw, "bytes=0-0")
	if e != nil {
		return out, e
	}
	defer probe.Body.Close()
	switch probe.StatusCode {
	case 206:
		a, b, n, e := contentRange(probe.Header.Get("Content-Range"))
		if e != nil || a != 0 || b != 0 {
			return out, errors.New("source returned invalid range probe")
		}
		buf, e := io.ReadAll(io.LimitReader(probe.Body, 2))
		if e != nil || len(buf) != 1 {
			return out, errors.New("source returned invalid probe body")
		}
		out.Size = n
		out.Ranges = true
	case 200:
		out.Size = probe.ContentLength
		out.Ranges = false
	default:
		return out, fmt.Errorf("source returned HTTP %d", probe.StatusCode)
	}
	out.ETag = probe.Header.Get("ETag")
	out.Modified = probe.Header.Get("Last-Modified")
	if v := probe.Header.Get("Content-Type"); v != "" {
		out.MIME = v
	}
	out.Name = sourceName(raw, probe.Header.Get("Content-Disposition"))
	if out.MIME == "" {
		out.MIME = "application/octet-stream"
	}
	if out.Size <= 0 {
		return out, errors.New("source must advertise a positive content length")
	}
	return out, nil
}
func sourceName(raw, disposition string) string {
	u, _ := url.Parse(raw)
	name := path.Base(u.Path)
	if _, p, e := mime.ParseMediaType(disposition); e == nil && p["filename"] != "" {
		name = p["filename"]
	}
	name = path.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, name)
	if name == "" || name == "." || name == "/" {
		name = "download.bin"
	}
	if len(name) > 180 {
		name = "download" + path.Ext(name)
	}
	return name
}
func (r Remote) stable() bool {
	return (r.ETag != "" && !strings.HasPrefix(r.ETag, "W/")) || r.Modified != ""
}
func (r Remote) matches(v Remote) bool {
	if r.Size != v.Size {
		return false
	}
	if r.ETag != "" && !strings.HasPrefix(r.ETag, "W/") {
		return r.ETag == v.ETag
	}
	return r.Modified != "" && r.Modified == v.Modified
}
func (r Remote) open(ctx context.Context, c *http.Client, start, length int64, ranged bool) (io.ReadCloser, error) {
	br := ""
	if ranged {
		br = fmt.Sprintf("bytes=%d-%d", start, start+length-1)
	}
	resp, e := requestSource(ctx, c, "GET", r.URL, br)
	if e != nil {
		return nil, e
	}
	fail := func(msg string) (io.ReadCloser, error) { resp.Body.Close(); return nil, errors.New(msg) }
	if r.ETag != "" && !strings.HasPrefix(r.ETag, "W/") && resp.Header.Get("ETag") != r.ETag {
		return fail("source ETag changed")
	}
	if r.Modified != "" && resp.Header.Get("Last-Modified") != r.Modified {
		return fail("source modification time changed")
	}
	if ranged {
		a, b, n, e := contentRange(resp.Header.Get("Content-Range"))
		if resp.StatusCode != 206 || e != nil || a != start || b != start+length-1 || n != r.Size {
			return fail("source ignored or changed requested range")
		}
	} else {
		if resp.StatusCode != 200 || resp.ContentLength != r.Size {
			return fail("source size changed")
		}
	}
	if ranged && resp.ContentLength >= 0 && resp.ContentLength != length {
		return fail("source range length mismatch")
	}
	return resp.Body, nil
}
