package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

var version = "2.1-go"

func main() {
	showVersion := flag.Bool("version", false, "print release version")
	check := flag.Bool("check", false, "validate configuration and Telegram access without polling or sending messages")
	env := flag.String("env", ".env", "dotenv path")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if e := run(*env, *check); e != nil {
		slog.Error("service stopped", "reason", e.Error())
		os.Exit(1)
	}
}
func run(env string, check bool) error {
	c, e := loadConfig(env)
	if e != nil {
		return e
	}
	debug.SetMemoryLimit(c.MemoryBytes)
	debug.SetGCPercent(70)
	lock, e := os.OpenFile(filepath.Join(c.DataDir, "service.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return errors.New("another service instance is using this data directory")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	store, e := newStore(c.DataDir)
	if e != nil {
		return e
	}
	key, e := signingKey(c.DataDir)
	if e != nil {
		return e
	}
	pool := newPool(c)
	server := &Server{cfg: c, store: store, key: key, pool: pool, source: pool, ctx: ctx, cache: newCache(c.CacheBytes), slots: make(chan struct{}, c.MaxStreams), flights: map[string]*flight{}}
	leech := &Leecher{cfg: c, pool: pool, store: store, disk: make(chan struct{}, 1)}
	if e = leech.Init(); e != nil {
		return e
	}
	bot := &Bot{cfg: c, server: server, leech: leech, http: &http.Client{Timeout: 40 * time.Second}}
	var wg sync.WaitGroup
	for _, w := range pool.workers {
		wg.Add(1)
		go func(w *Worker) { defer wg.Done(); w.Run(ctx) }(w)
	}
	defer func() { cancel(); wg.Wait() }()
	if check {
		test, done := context.WithTimeout(ctx, 90*time.Second)
		defer done()
		if e = bot.Check(test); e != nil {
			return e
		}
		for pool.Stats()["ready"] < len(pool.workers) {
			if e = pause(test, 500*time.Millisecond); e != nil {
				return fmt.Errorf("Telegram check incomplete: %d/%d workers ready", pool.Stats()["ready"], len(pool.workers))
			}
		}
		slog.Info("configuration and Telegram access verified", "workers", len(pool.workers))
		return nil
	}
	httpServer := &http.Server{Addr: c.Address, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024}
	errs := make(chan error, 2)
	go func() { errs <- httpServer.ListenAndServe() }()
	go func() { errs <- bot.Run(ctx) }()
	slog.Info("streaming origin started", "address", c.Address, "workers", len(pool.workers))
	select {
	case <-ctx.Done():
	case e = <-errs:
		if errors.Is(e, http.ErrServerClosed) || errors.Is(e, context.Canceled) {
			e = nil
		}
	}
	cancel()
	leech.Cancel()
	leech.wg.Wait()
	shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	_ = httpServer.Shutdown(shutdown)
	return e
}
