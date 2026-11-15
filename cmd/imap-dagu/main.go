package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kyleparisi/imap-dagu/internal/config"
	"github.com/kyleparisi/imap-dagu/internal/dagu"
	"github.com/kyleparisi/imap-dagu/internal/mailbox"
	"github.com/kyleparisi/imap-dagu/internal/watch"
)

func main() {
	if err := run(); err != nil {
		slog.Error("imap-dagu stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "imap-dagu.toml", "configuration file")
	check := flag.Bool("check", false, "authenticate, verify IDLE and read-only INBOX; do not queue or write state")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	password := os.Getenv(cfg.IMAP.PasswordEnv)
	if password == "" {
		return errors.New("configured IMAP password environment variable is empty")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *check {
		m, err := mailbox.Connect(ctx, cfg.IMAP, password)
		if err != nil {
			return err
		}
		defer m.Close()
		fmt.Println("TLS, authentication, IDLE capability, and read-only INBOX: OK")
		return nil
	}
	store := watch.Store{Dir: cfg.StateDir}
	unlock, err := store.Lock()
	if err != nil {
		return err
	}
	defer unlock()
	queue := dagu.Client{Config: cfg.Dagu}
	processor := watch.Processor{Store: store, Source: cfg.Source(), Account: cfg.Account, Backfill: cfg.Backfill, Enqueue: func(ctx context.Context, e watch.Event) error {
		if err := queue.Enqueue(ctx, e); err != nil {
			return err
		}
		slog.Info("event accepted by Dagu", "event_id", e.ID, "uid", e.UID)
		return nil
	}}
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := session(ctx, cfg, password, processor)
		if errors.Is(err, watch.ErrState) {
			return err
		}
		if ctx.Err() != nil {
			break
		}
		// Avoid repeatedly logging in when a mailbox is busy but Dagu is unavailable.
		if time.Since(started) > time.Minute {
			delay = time.Second
		}
		slog.Warn("connection or delivery failed; retrying", "error", err, "retry_in", delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		delay = min(delay*2, time.Minute)
	}
	return nil
}
func session(ctx context.Context, cfg config.Config, password string, p watch.Processor) error {
	m, err := mailbox.Connect(ctx, cfg.IMAP, password)
	if err != nil {
		return err
	}
	defer m.Close()
	slog.Info("watching INBOX", "account", cfg.Account)
	for {
		if err = p.Sync(ctx, m); err != nil {
			return err
		}
		if err = m.Wait(ctx); err != nil {
			return err
		}
	}
}
