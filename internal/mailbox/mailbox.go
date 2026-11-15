package mailbox

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/kyleparisi/imap-dagu/internal/config"
	"github.com/kyleparisi/imap-dagu/internal/watch"
)

func Connect(ctx context.Context, cfg config.IMAP, password string) (*watch.Mailbox, error) {
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	raw, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return nil, errors.New("IMAP connection failed")
	}
	conn := tls.Client(raw, &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12})
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if err = conn.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, errors.New("IMAP TLS handshake failed")
	}
	return open(ctx, cfg, password, conn)
}

func open(ctx context.Context, cfg config.IMAP, password string, conn net.Conn) (*watch.Mailbox, error) {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	failed := true
	defer func() {
		if failed {
			stop()
			conn.Close()
		}
	}()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	c, err := client.New(conn)
	if err != nil {
		return nil, errors.New("IMAP greeting failed")
	}
	c.ErrorLog = log.New(io.Discard, "", 0)
	c.Timeout = 30 * time.Second
	if err = c.Login(cfg.Username, password); err != nil {
		return nil, errors.New("IMAP authentication failed")
	}
	caps, err := c.Capability()
	if err != nil {
		return nil, errors.New("IMAP capabilities failed")
	}
	if !caps["IDLE"] {
		return nil, errors.New("server does not advertise IDLE; polling fallback is disabled")
	}
	updates := make(chan client.Update)
	signal := make(chan struct{}, 1)
	c.Updates = updates
	go func() {
		for {
			select {
			case <-c.LoggedOut():
				return
			case <-updates:
				select {
				case signal <- struct{}{}:
				default:
				}
			}
		}
	}()
	status, err := c.Select("INBOX", true)
	if err != nil {
		return nil, errors.New("read-only INBOX selection failed")
	}
	failed = false
	return &watch.Mailbox{UIDValidity: status.UidValidity, UIDNext: status.UidNext,
		Close: func() error { stop(); return c.Terminate() },
		Search: func(after uint32) ([]uint32, error) {
			set := new(imap.SeqSet)
			set.AddRange(after+1, 0)
			criteria := imap.NewSearchCriteria()
			criteria.Uid = set
			uids, err := c.UidSearch(criteria)
			if err != nil {
				return nil, errors.New("IMAP UID search failed")
			}
			return uids, nil
		},
		Wait: func(ctx context.Context) error {
			stopIdle := make(chan struct{})
			done := make(chan error, 1)
			c.Timeout = 11 * time.Minute
			go func() { done <- c.Idle(stopIdle, &client.IdleOptions{PollInterval: -1}) }()
			timer := time.NewTimer(10 * time.Minute)
			defer timer.Stop()
			var idleErr error
			select {
			case <-ctx.Done():
				close(stopIdle)
				_ = c.Terminate()
				<-done
				idleErr = ctx.Err()
			case <-signal:
				close(stopIdle)
				idleErr = <-done
			case <-timer.C:
				close(stopIdle)
				idleErr = <-done
			case idleErr = <-done:
			}
			c.Timeout = 30 * time.Second
			if idleErr != nil {
				return fmt.Errorf("IMAP IDLE ended: %w", idleErr)
			}
			return nil
		},
	}, nil
}
