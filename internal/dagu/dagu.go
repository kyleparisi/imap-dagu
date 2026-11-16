package dagu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/kyleparisi/imap-dagu/internal/config"
	"github.com/kyleparisi/imap-dagu/internal/watch"
)

type Client struct {
	Config config.Dagu
	// Run defaults to a bounded subprocess. Failures include a bounded stderr tail.
	Run func(context.Context, string, []string) ([]byte, error)
}

func (c Client) Enqueue(ctx context.Context, e watch.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	run := c.Run
	if run == nil {
		run = commandOutput
	}
	invoke := func(ctx context.Context, args ...string) ([]byte, error) {
		child, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if c.Config.Home != "" {
			args = append([]string{"--dagu-home", c.Config.Home}, args...)
		}
		data, err := run(child, c.Config.Bin, args)
		return data, commandError(err)
	}
	delivered := func(ctx context.Context) (bool, error) {
		data, err := invoke(ctx, "history", c.Config.DAG, "--run-id", e.ID, "--format", "json", "--from", "1970-01-01")
		if err != nil {
			return false, fmt.Errorf("query Dagu history: %w", err)
		}
		var runs []struct {
			ID     string `json:"dagRunId"`
			Status string `json:"status"`
		}
		if err = json.Unmarshal(data, &runs); err != nil {
			return false, fmt.Errorf("decode Dagu history: %w", err)
		}
		for _, r := range runs {
			if r.ID != e.ID {
				continue
			}
			// Only a run that has started proves it made it through the queue.
			// Dagu writes "queued" status BEFORE writing the queue item.
			switch r.Status {
			case "running", "waiting", "succeeded", "partially_succeeded", "failed":
				return true, nil
			default:
				return false, fmt.Errorf("%w: Dagu run %s has status %q; delivery is unconfirmed; inspect the run and queue before restarting (see README recovery)", watch.ErrState, e.ID, r.Status)
			}
		}
		return false, nil
	}
	ok, err := delivered(ctx)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// Let an in-flight enqueue finish during graceful shutdown. The IMAP
	// connection closes immediately, but the caller still saves the accepted UID.
	deliveryCtx := context.WithoutCancel(ctx)
	_, err = invoke(deliveryCtx, "enqueue", "--run-id", e.ID, c.Config.DAG, "--",
		"EVENT_ID="+e.ID, "ACCOUNT="+e.Account, "MAILBOX=INBOX",
		"UIDVALIDITY="+strconv.FormatUint(uint64(e.UIDValidity), 10), "UID="+strconv.FormatUint(uint64(e.UID), 10))
	if err == nil {
		return nil
	}
	ok, lookupErr := delivered(deliveryCtx)
	if lookupErr != nil {
		return fmt.Errorf("enqueue failed (%v); recovery check: %w", err, lookupErr)
	}
	if ok {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return fmt.Errorf("%w: Dagu run %s exists without a visible started run; inspect the incomplete attempt before restarting (see README recovery): %v", watch.ErrState, e.ID, err)
	}
	return fmt.Errorf("Dagu did not confirm queue acceptance for %s: %w", e.ID, err)
}

func commandError(err error) error {
	if err == nil {
		return nil
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return err
	}
	tail := exit.Stderr
	if len(tail) > 1024 {
		tail = tail[len(tail)-1024:]
	}
	text := strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, string(tail)))
	if text == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, text)
}

func commandOutput(ctx context.Context, bin string, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	// Terminal Ctrl-C targets the foreground process group. Keep an enqueue
	// outside that group so the listener can wait for it during shutdown.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd.Output()
}
