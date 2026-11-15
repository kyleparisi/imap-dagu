package dagu

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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
				return nil, errors.New("not found")
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
			return []byte(`[{"dagRunId":"` + watch.NewEvent("support", 7, 42).ID + `","status":"queued"}]`), nil
		}}
		require.NoError(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)))
	})
	t.Run("Uncertain queue acceptance is checked before retrying", func(t *testing.T) {
		calls := 0
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			calls++
			if calls == 3 {
				assert.Equal(t, "history", args[0])
				return []byte(`[{"dagRunId":"` + watch.NewEvent("support", 7, 42).ID + `","status":"queued"}]`), nil
			}
			return nil, errors.New("interrupted")
		}}
		require.NoError(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)))
		assert.Equal(t, 3, calls)
	})
	t.Run("A missing queue never acknowledges the event", func(t *testing.T) {
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(context.Context, string, []string) ([]byte, error) { return nil, errors.New("unavailable") }}
		require.ErrorContains(t, subject(c, context.Background(), watch.NewEvent("support", 7, 42)), "did not confirm")
	})
	t.Run("A partial attempt does not falsely acknowledge delivery", func(t *testing.T) {
		event := watch.NewEvent("support", 7, 42)
		c := Client{Config: config.Dagu{Bin: "dagu", DAG: "email-received"}, Run: func(_ context.Context, _ string, args []string) ([]byte, error) {
			if args[0] == "history" {
				return []byte(`[{"dagRunId":"` + event.ID + `","status":"not_started"}]`), nil
			}
			return nil, errors.New("partial enqueue failed")
		}}
		require.ErrorContains(t, subject(c, context.Background(), event), "did not confirm")
	})
	t.Run("Real Dagu accepts one job across repeated delivery", func(t *testing.T) {
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
		require.NoError(t, subject(c, context.Background(), event))
		out, err := exec.Command(bin, "--dagu-home", home, "history", "email-received", "--format", "json").Output()
		require.NoError(t, err)
		assert.Contains(t, string(out), event.ID)
		// Dagu's unique run ID rejects a second independently attempted enqueue.
		require.Error(t, exec.Command(bin, "--dagu-home", home, "enqueue", "--run-id", event.ID, "email-received").Run())
	})
}
