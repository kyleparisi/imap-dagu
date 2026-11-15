package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const valid = `account = "support"
state_dir = "/var/lib/imap-dagu/support"
[imap]
host = "imap.example.com"
username = "support@example.com"
password_env = "IMAP_PASSWORD"
[dagu]
dag = "email-received"
`

func file(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(p, []byte(s), 0600))
	return p
}
func TestLoad(t *testing.T) {
	subject := Load
	t.Run("Applies defaults without reading credentials", func(t *testing.T) {
		c, err := subject(file(t, valid))
		require.NoError(t, err)
		assert.Equal(t, 993, c.IMAP.Port)
		assert.Equal(t, "dagu", c.Dagu.Bin)
		assert.False(t, c.Backfill)
	})
	t.Run("Rejects typos", func(t *testing.T) {
		_, err := subject(file(t, valid+"typo = true\n"))
		require.ErrorContains(t, err, "unknown configuration keys")
	})
	t.Run("Rejects unsafe account names used as Dagu parameters", func(t *testing.T) {
		_, err := subject(file(t, strings.Replace(valid, `account = "support"`, `account = "support; echo bad"`, 1)))
		require.Error(t, err)
	})
	t.Run("Rejects relative state paths", func(t *testing.T) {
		_, err := subject(file(t, strings.Replace(valid, `"/var/lib/imap-dagu/support"`, `"relative"`, 1)))
		require.ErrorContains(t, err, "absolute path")
	})
	t.Run("Password rotation preserves source identity", func(t *testing.T) {
		c, err := subject(file(t, valid))
		require.NoError(t, err)
		other := c
		other.IMAP.PasswordEnv = "ROTATED_PASSWORD"
		assert.Equal(t, c.Source(), other.Source())
		other.IMAP.Username = "sales@example.com"
		assert.NotEqual(t, c.Source(), other.Source())
	})
}
