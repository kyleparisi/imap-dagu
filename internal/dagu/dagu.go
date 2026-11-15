package dagu

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"github.com/kyleparisi/imap-dagu/internal/config"
	"github.com/kyleparisi/imap-dagu/internal/watch"
)

type Client struct {
	Config config.Dagu
	// Run defaults to a bounded subprocess; stderr never enters application logs.
	Run func(context.Context, string, []string) ([]byte, error)
}

func (c Client) Enqueue(ctx context.Context, e watch.Event) error {
	run := c.Run
	if run == nil {
		run = func(ctx context.Context, bin string, args []string) ([]byte, error) {
			return exec.CommandContext(ctx, bin, args...).Output()
		}
	}
	invoke := func(args ...string) ([]byte, error) {
		child, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if c.Config.Home != "" {
			args = append([]string{"--dagu-home", c.Config.Home}, args...)
		}
		return run(child, c.Config.Bin, args)
	}
	accepted := func() bool {
		data, err := invoke("history", c.Config.DAG, "--run-id", e.ID, "--format", "json", "--from", "1970-01-01")
		if err != nil {
			return false
		}
		var runs []struct {
			ID     string `json:"dagRunId"`
			Status string `json:"status"`
		}
		if json.Unmarshal(data, &runs) != nil {
			return false
		}
		for _, r := range runs {
			if r.ID != e.ID {
				continue
			}
			// A partial, not_started attempt does not prove queue acceptance.
			switch r.Status {
			case "queued", "running", "waiting", "succeeded", "partially_succeeded", "failed", "aborted", "rejected":
				return true
			}
		}
		return false
	}
	if accepted() {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := invoke("enqueue", "--run-id", e.ID, c.Config.DAG, "--",
		"EVENT_ID="+e.ID, "ACCOUNT="+e.Account, "MAILBOX=INBOX",
		"UIDVALIDITY="+strconv.FormatUint(uint64(e.UIDValidity), 10), "UID="+strconv.FormatUint(uint64(e.UID), 10))
	if err == nil {
		return nil
	}
	// Acceptance may have succeeded even when the command timed out or lost its reply.
	if accepted() {
		return nil
	}
	return fmt.Errorf("dagu did not confirm queue acceptance: %w", err)
}
