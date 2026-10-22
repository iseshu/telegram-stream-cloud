package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	CDNCacheSeconds                   int64
	APIID                             int
	APIHash, PrimaryToken             string
	WorkerTokens                      []string
	ChannelID, OwnerID                int64
	Address, BaseURL, DataDir         string
	CacheBytes, MemoryBytes, PartSize int64
	Prefetch, MaxStreams              int
}

// Environment variables take precedence over the optional local dotenv file.
func loadConfig(path string) (Config, error) {
	values := map[string]string{}
	f, err := os.Open(path)
	if err == nil {
		defer f.Close()
		s := bufio.NewScanner(f)
		for s.Scan() {
			l := strings.TrimSpace(s.Text())
			if l == "" || strings.HasPrefix(l, "#") {
				continue
			}
			l = strings.TrimPrefix(l, "export ")
			k, v, ok := strings.Cut(l, "=")
			if !ok {
				return Config{}, errors.New("invalid dotenv line")
			}
			v = strings.TrimSpace(v)
			if len(v) >= 2 && ((v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'')) {
				v = v[1 : len(v)-1]
			} else {
				v = strings.SplitN(v, " #", 2)[0]
			}
			values[strings.TrimSpace(k)] = v
		}
		if err := s.Err(); err != nil {
			return Config{}, err
		}
	} else if !os.IsNotExist(err) {
		return Config{}, err
	}
	get := func(k, d string) string {
		if v, ok := os.LookupEnv(k); ok {
			return v
		}
		if v, ok := values[k]; ok {
			return v
		}
		return d
	}
	number := func(k string, d int64) (int64, error) {
		n, e := strconv.ParseInt(get(k, strconv.FormatInt(d, 10)), 10, 64)
		if e != nil {
			return 0, fmt.Errorf("invalid %s", k)
		}
		return n, nil
	}
	var c Config
	n, e := number("TELEGRAM_API_ID", 0)
	if e != nil {
		return c, e
	}
	c.APIID = int(n)
	c.APIHash = get("TELEGRAM_API_HASH", "")
	c.PrimaryToken = get("TELEGRAM_BOT_TOKEN", "")
	c.ChannelID, e = number("TELEGRAM_CHANNEL_ID", 0)
	if e != nil {
		return c, e
	}
	c.OwnerID, e = number("OWNER_ID", 0)
	if e != nil {
		return c, e
	}
	re := regexp.MustCompile(`\d+:[A-Za-z0-9_-]+`)
	seen := map[string]bool{c.PrimaryToken: true}
	for _, t := range re.FindAllString(get("MULTI_BOT_TOKENS", ""), -1) {
		if !seen[t] {
			c.WorkerTokens = append(c.WorkerTokens, t)
			seen[t] = true
		}
	}
	c.BaseURL = strings.TrimRight(get("BASE_URL", "http://localhost:8080"), "/")
	u, e := url.Parse(c.BaseURL)
	if e != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return c, errors.New("BASE_URL must be an HTTP(S) URL without credentials, query, or fragment")
	}
	port, e := number("PORT", 8080)
	if e != nil || port < 1 || port > 65535 {
		return c, errors.New("invalid PORT")
	}
	c.Address = net.JoinHostPort(get("BIND_ADDRESS", "0.0.0.0"), strconv.FormatInt(port, 10))
	c.DataDir = get("DATA_DIR", "data")
	c.CacheBytes, e = number("CACHE_SIZE_MB", 512)
	if e != nil || c.CacheBytes < 0 || c.CacheBytes > 1024 {
		return c, errors.New("CACHE_SIZE_MB must be 0..1024")
	}
	c.CacheBytes *= 1024 * 1024
	c.MemoryBytes, e = number("MEMORY_LIMIT_MB", 1800)
	if e != nil || c.MemoryBytes < 256 || c.MemoryBytes > 65536 {
		return c, errors.New("invalid MEMORY_LIMIT_MB")
	}
	c.MemoryBytes *= 1024 * 1024
	c.PartSize, e = number("LEECH_PART_SIZE_MB", 1600)
	if e != nil || c.PartSize < 1 || c.PartSize > 1600 {
		return c, errors.New("LEECH_PART_SIZE_MB must be 1..1600")
	}
	c.PartSize *= 1000 * 1000
	n, e = number("PREFETCH_CHUNKS", 4)
	if e != nil || n < 1 || n > 16 {
		return c, errors.New("PREFETCH_CHUNKS must be 1..16")
	}
	c.Prefetch = int(n)
	n, e = number("MAX_STREAMS", 32)
	if e != nil || n < 1 || n > 256 {
		return c, errors.New("MAX_STREAMS must be 1..256")
	}
	c.MaxStreams = int(n)
	c.CDNCacheSeconds, e = number("CDN_CACHE_SECONDS", 0)
	if e != nil || c.CDNCacheSeconds < 0 || c.CDNCacheSeconds > 31536000 {
		return c, errors.New("CDN_CACHE_SECONDS must be 0..31536000")
	}

	if c.APIID <= 0 || c.APIHash == "" || !re.MatchString(c.PrimaryToken) || c.ChannelID == 0 || c.OwnerID <= 0 {
		return c, errors.New("required Telegram credentials, channel, or owner are missing")
	}
	budget := c.CacheBytes + int64(c.MaxStreams*(c.Prefetch+2)+len(c.WorkerTokens)*4)*chunkSize + 256*1024*1024
	if budget > c.MemoryBytes {
		return c, errors.New("cache, stream buffers and workers exceed memory budget; reduce PREFETCH_CHUNKS, MAX_STREAMS or CACHE_SIZE_MB")
	}
	if e = os.MkdirAll(filepath.Join(c.DataDir, "sessions"), 0700); e != nil {
		return c, e
	}
	return c, nil
}
