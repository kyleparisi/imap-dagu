package main

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/kyleparisi/imap-dagu/internal/watch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExitCode(t *testing.T) {
	subject := exitCode
	t.Run("Operator errors stop systemd restart loops", func(t *testing.T) {
		assert.Equal(t, 78, subject(fmt.Errorf("delivery failed: %w", watch.ErrState)))
	})
	t.Run("Transient failures remain restartable", func(t *testing.T) { assert.Equal(t, 1, subject(errors.New("network unavailable"))) })
	t.Run("Successful exit stays successful", func(t *testing.T) { assert.Zero(t, subject(nil)) })
}

func TestTakePassword(t *testing.T) {
	subject := takePassword
	t.Run("Credentials remain usable but are removed from child environments", func(t *testing.T) {
		t.Setenv("IMAP_DAGU_TEST_PASSWORD", "test-password")
		password, err := subject("IMAP_DAGU_TEST_PASSWORD")
		require.NoError(t, err)
		assert.Equal(t, "test-password", password)
		_, present := os.LookupEnv("IMAP_DAGU_TEST_PASSWORD")
		assert.False(t, present)
	})
	t.Run("Empty credentials give an actionable error", func(t *testing.T) {
		t.Setenv("IMAP_DAGU_TEST_PASSWORD", "")
		_, err := subject("IMAP_DAGU_TEST_PASSWORD")
		require.ErrorContains(t, err, "password environment variable is empty")
	})
}
