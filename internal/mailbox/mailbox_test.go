package mailbox

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/kyleparisi/imap-dagu/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	subject := open
	t.Run("IDLE receives arrival and resumes UID search without changing flags", func(t *testing.T) {
		local, remote := net.Pipe()
		defer remote.Close()
		arrival := make(chan struct{})
		idling := make(chan struct{}, 10)
		commands := make(chan string, 20)
		go func() {
			defer remote.Close()
			w := bufio.NewWriter(remote)
			r := bufio.NewScanner(remote)
			send := func(s string) { _, _ = w.WriteString(s); _ = w.Flush() }
			send("* OK IMAP ready\r\n")
			for r.Scan() {
				line := r.Text()
				commands <- line
				fields := strings.Fields(line)
				if len(fields) < 2 {
					return
				}
				tag := fields[0]
				switch strings.ToUpper(fields[1]) {
				case "CAPABILITY":
					send("* CAPABILITY IMAP4rev1 IDLE\r\n" + tag + " OK capabilities\r\n")
				case "LOGIN":
					send(tag + " OK authenticated\r\n")
				case "EXAMINE":
					send("* FLAGS (\\Seen)\r\n* 0 EXISTS\r\n* OK [UIDVALIDITY 7] valid\r\n* OK [UIDNEXT 1] next\r\n" + tag + " OK [READ-ONLY] selected\r\n")
				case "IDLE":
					send("+ idling\r\n")
					idling <- struct{}{}
					<-arrival
					send("* 1 EXISTS\r\n")
					if !r.Scan() || r.Text() != "DONE" {
						return
					}
					send(tag + " OK idle ended\r\n")
				case "UID":
					send("* SEARCH 1\r\n" + tag + " OK searched\r\n")
				default:
					send(tag + " BAD unexpected command\r\n")
				}
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		m, err := subject(ctx, config.IMAP{Username: "support@example.com"}, "test-password", local)
		require.NoError(t, err)
		defer m.Close()
		assert.Equal(t, uint32(7), m.UIDValidity)
		assert.Equal(t, uint32(1), m.UIDNext)
		done := make(chan error, 1)
		go func() { done <- m.Wait(ctx) }()
		select {
		case <-idling:
		case <-ctx.Done():
			t.Fatal("never entered IDLE")
		}
		close(arrival)
		require.NoError(t, <-done)
		uids, err := m.Search(0)
		require.NoError(t, err)
		assert.Equal(t, []uint32{1}, uids)
		for len(commands) > 0 {
			command := <-commands
			assert.NotContains(t, command, " SELECT ")
			assert.NotContains(t, command, " STORE ")
			assert.NotContains(t, command, " FETCH ")
		}
	})
	t.Run("Authentication errors never reveal server text or password", func(t *testing.T) {
		local, remote := net.Pipe()
		defer remote.Close()
		go func() {
			defer remote.Close()
			r := bufio.NewScanner(remote)
			fmt.Fprint(remote, "* OK [CAPABILITY IMAP4rev1 IDLE] ready\r\n")
			if r.Scan() {
				tag := strings.Fields(r.Text())[0]
				fmt.Fprint(remote, tag+" NO sensitive-server-text\r\n")
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := subject(ctx, config.IMAP{Username: "support@example.com"}, "secret-test", local)
		require.EqualError(t, err, "IMAP authentication failed")
	})
}
