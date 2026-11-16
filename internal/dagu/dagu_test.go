package dagu

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/kyleparisi/imap-dagu/internal/config"
	"github.com/kyleparisi/imap-dagu/internal/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueue(t *testing.T) {
	subject := Client.Enqueue
	t.Run("Uses the same run ID and numeric UID parameters without a shell", func(t *testing.T) {
		event := watch.NewEvent("support", 7, 42)
		calls := 0
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received", Home: "/queues"}, Run: func(ctx context.Context, bin string, args []string) ([]byte, error) {
			calls++
			assert.Equal(t, "dagu", bin)
			_, ok := ctx.Deadline()
			assert.True(t, ok)
			if calls == 1 {
				assert.Equal(t, []string{"--dagu-home", "/queues", "history", "email-received", "--run-id", event.ID, "--format", "json", "--from", "1970-01-01"}, args)
				return []byte(`[]`), nil
			}
			assert.Equal(t, []string{"--dagu-home", "/queues", "enqueue", "--run-id", event.ID, "email-received", "--", "EVENT_ID=" + event.ID, "ACCOUNT=support", "MAILBOX=INBOX", "UIDVALIDITY=7", "UID=42"}, args)
			return nil, nil
		}}
		require.NoError(t, subject(c, context.Background(), event))
		assert.Equal(t, 2, calls)
	})
	t.Run("A crash after enqueue is recovered without a second queue entry", func(t *testing.T) {
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			assert.Equal(t, "history", args[0])
			return []byte(`[{"dagRunId":"` + watch.NewEvent("support", 7, 42).ID + `","status":"succeeded"}]`), nil
		}}
		require.NoError(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)))
	})
	t.Run("Uncertain queue acceptance is checked before retrying", func(t *testing.T) {
		calls := 0
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			calls++
			if calls == 1 {
				return []byte(`[]`), nil
			}
			if calls == 3 {
				assert.Equal(t, "history", args[0])
				return []byte(`[{"dagRunId":"` + watch.NewEvent("support", 7, 42).ID + `","status":"succeeded"}]`), nil
			}
			return nil, errors.New("interrupted")
		}}
		require.NoError(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)))
		assert.Equal(t, 3, calls)
	})
	t.Run("A missing queue never acknowledges the event", func(t *testing.T) {
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(context.Context, string, []string) ([]byte, error) { return nil, errors.New("unavailable") }}
		require.ErrorContains(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)), "query Dagu history")
	})
	t.Run("A partial attempt does not falsely acknowledge delivery", func(t *testing.T) {
		event := watch.NewEvent("support", 7, 42)
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			if args[0] == "history" {
				return []byte(`[{"dagRunId":"` + event.ID + `","status":"not_started"}]`), nil
			}
			return nil, errors.New("partial enqueue failed")
		}}
		require.ErrorIs(t, subject(c, context.Background(), event), watch.ErrState)
	})
	t.Run("Real Dagu queues once and ambiguous recovery preserves the cursor", func(t *testing.T) {
		if os.Getenv("IMAP_DAGU_INTEGRATION") != "1" {
			t.Skip("set IMAP_DAGU_INTEGRATION=1 to test installed Dagu")
		}
		bin, err := exec.LookPath("dagu")
		require.NoError(t, err)
		home := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(home, "dags"), 0700))
		require.NoError(t, os.WriteFile(filepath.Join(home, "config.yaml"), []byte("queues:\n  enabled: true\n"), 0600))
		body := `name: email-received
params:
  - EVENT_ID: ""
  - ACCOUNT: ""
  - MAILBOX: ""
  - UIDVALIDITY: ""
  - UID: ""
steps:
  - name: record
    command: echo accepted
`
		require.NoError(t, os.WriteFile(filepath.Join(home, "dags", "email-received.yaml"), []byte(body), 0600))
		c := Client{Config: config.Dagu{Bin: bin, DAG: "email-received", Home: home}}
		event := watch.NewEvent("support", 7, 42)
		require.NoError(t, subject(c, context.Background(), event))
		// Simulate interruption after status is written but before the queue item.
		removed := 0
		require.NoError(t, filepath.WalkDir(filepath.Join(home, "data", "queue"), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasPrefix(d.Name(), "item_") {
				removed++
				return os.Remove(path)
			}
			return nil
		}))
		require.Positive(t, removed, "fixture must contain a real queue item")
		require.ErrorIs(t, subject(c, context.Background(), event), watch.ErrState)
		out, err := exec.Command(bin, "--dagu-home", home, "history", "email-received", "--format", "json").Output()
		require.NoError(t, err)
		assert.Contains(t, string(out), event.ID)
		assert.Contains(t, string(out), `"queued"`, "status alone still claims queued")
		// Simulate interruption even earlier, after dag.json but before status.
		removed = 0
		require.NoError(t, filepath.WalkDir(filepath.Join(home, "data", "dag-runs"), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && d.Name() == "status.jsonl" {
				removed++
				return os.Remove(path)
			}
			return nil
		}))
		require.Positive(t, removed)
		err = subject(c, context.Background(), event)
		require.ErrorIs(t, err, watch.ErrState)
		assert.ErrorContains(t, err, "incomplete attempt")
		// Dagu's unique run ID rejects a second independently attempted enqueue.
		require.Error(t, exec.Command(bin, "--dagu-home", home, "enqueue", "--run-id", event.ID, "email-received").Run())
	})
}

func TestInterruptedEnqueue(t *testing.T) {
	subject := Client.Enqueue
	t.Run("Graceful shutdown does not kill an enqueue already started", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(child context.Context, _ string, args []string) ([]byte, error) {
			calls++
			if args[0] == "history" {
				return []byte(`[]`), nil
			}
			cancel()
			assert.NoError(t, child.Err(), "enqueue survives parent cancellation")
			_, ok := child.Deadline()
			assert.True(t, ok, "enqueue still has its own deadline")
			return nil, nil
		}}
		require.NoError(t, subject(c, ctx, watch.NewEvent("support", 7, 42)))
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		assert.Equal(t, 2, calls)
	})
	t.Run("Queued history without a queue item is never acknowledged", func(t *testing.T) {
		e := watch.NewEvent("support", 7, 42)
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			assert.Equal(t, "history", args[0], "do not create another job")
			return []byte(`[{"dagRunId":"` + e.ID + `","status":"queued"}]`), nil
		}}
		require.ErrorIs(t, subject(c, context.Background(), e), watch.ErrState)
	})
	t.Run("Orphan attempt without history stops instead of retrying forever", func(t *testing.T) {
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			if args[0] == "history" {
				return []byte(`[]`), nil
			}
			return nil, errors.New("dag-run already exists")
		}}
		err := subject(c, context.Background(), watch.NewEvent("support", 7, 42))
		require.ErrorIs(t, err, watch.ErrState)
		assert.ErrorContains(t, err, "incomplete attempt")
	})
	t.Run("Uncertain enqueue that leaves only queued status needs attention", func(t *testing.T) {
		e := watch.NewEvent("support", 7, 42)
		calls := 0
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			calls++
			if calls == 1 {
				return []byte(`[]`), nil
			}
			if args[0] == "enqueue" {
				return nil, context.DeadlineExceeded
			}
			return []byte(`[{"dagRunId":"` + e.ID + `","status":"queued"}]`), nil
		}}
		require.ErrorIs(t, subject(c, context.Background(), e), watch.ErrState)
		assert.Equal(t, 3, calls)
	})
	t.Run("Failed history lookup does not trigger an enqueue", func(t *testing.T) {
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			assert.Equal(t, "history", args[0])
			return nil, errors.New("history unavailable")
		}}
		require.ErrorContains(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)), "history unavailable")
	})
	t.Run("Dagu errors retain bounded useful diagnostics", func(t *testing.T) {
		process := exec.Command("sh", "-c", "exit 1")
		err := process.Run()
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		exit.Stderr = []byte(strings.Repeat("x", 2000) + "\x1b\nError: queues are disabled in configuration\n")
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			if args[0] == "history" {
				return []byte(`[]`), nil
			}
			return nil, exit
		}}
		err = subject(c, context.Background(), watch.NewEvent("support", 7, 42))
		require.ErrorContains(t, err, "queues are disabled")
		assert.Less(t, len(err.Error()), 1300)
		assert.NotContains(t, err.Error(), "\x1b")
	})
}

func TestCommandOutput(t *testing.T) {
	subject := commandOutput
	t.Run("Dagu children are isolated from terminal Ctrl-C", func(t *testing.T) {
		out, err := subject(context.Background(), "sh", []string{"-c", "ps -o pgid= -p $$"})
		require.NoError(t, err)
		group, err := strconv.Atoi(strings.TrimSpace(string(out)))
		require.NoError(t, err)
		assert.NotEqual(t, syscall.Getpgrp(), group)
	})
}
