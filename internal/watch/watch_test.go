package watch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSync(t *testing.T) {
	subject := Processor.Sync
	t.Run("First start skips history but catches messages arriving after selection", func(t *testing.T) {
		store := Store{Dir: t.TempDir()}
		p := Processor{Store: store, Source: "source", Account: "support", Enqueue: func(_ context.Context, e Event) error { assert.Equal(t, uint32(9), e.UID); return nil }}
		m := &Mailbox{UIDValidity: 7, UIDNext: 9, Search: func(after uint32) ([]uint32, error) { assert.Equal(t, uint32(8), after); return []uint32{8, 9}, nil }}
		require.NoError(t, subject(p, context.Background(), m))
		saved, err := store.Load()
		require.NoError(t, err)
		assert.Equal(t, uint32(9), saved.LastUID)
	})
	t.Run("Backfill sorts UIDs and filters duplicates and reversed ranges", func(t *testing.T) {
		store := Store{Dir: t.TempDir()}
		var accepted []uint32
		p := Processor{Store: store, Source: "source", Account: "support", Backfill: true, Enqueue: func(_ context.Context, e Event) error {
			accepted = append(accepted, e.UID)
			assert.Equal(t, NewEvent("support", 7, e.UID), e)
			return nil
		}}
		m := &Mailbox{UIDValidity: 7, UIDNext: 10, Search: func(uint32) ([]uint32, error) { return []uint32{9, 2, 2, 0}, nil }}
		require.NoError(t, subject(p, context.Background(), m))
		require.NoError(t, subject(p, context.Background(), m))
		assert.Equal(t, []uint32{2, 9}, accepted)
	})
	t.Run("Failed enqueue does not advance past the failed UID", func(t *testing.T) {
		store := Store{Dir: t.TempDir()}
		var ids []string
		p := Processor{Store: store, Source: "source", Account: "support", Backfill: true, Enqueue: func(_ context.Context, e Event) error {
			ids = append(ids, e.ID)
			if e.UID == 4 {
				return errors.New("queue unavailable")
			}
			return nil
		}}
		m := &Mailbox{UIDValidity: 7, UIDNext: 5, Search: func(uint32) ([]uint32, error) { return []uint32{3, 4}, nil }}
		require.ErrorContains(t, subject(p, context.Background(), m), "queue unavailable")
		saved, err := store.Load()
		require.NoError(t, err)
		assert.Equal(t, uint32(3), saved.LastUID)
		p.Enqueue = func(_ context.Context, e Event) error {
			assert.Equal(t, ids[1], e.ID)
			assert.Equal(t, uint32(4), e.UID)
			return nil
		}
		require.NoError(t, subject(p, context.Background(), m))
		saved, err = store.Load()
		require.NoError(t, err)
		assert.Equal(t, uint32(4), saved.LastUID)
	})
	t.Run("Changed UID namespace stops without replaying old messages", func(t *testing.T) {
		store := Store{Dir: t.TempDir()}
		require.NoError(t, store.Save(State{Version: 1, Source: "source", UIDValidity: 7, LastUID: 9}))
		p := Processor{Store: store, Source: "source", Account: "support"}
		err := subject(p, context.Background(), &Mailbox{UIDValidity: 8, UIDNext: 10})
		require.ErrorIs(t, err, ErrState)
		saved, err := store.Load()
		require.NoError(t, err)
		assert.Equal(t, uint32(7), saved.UIDValidity)
	})
	t.Run("Changed source cannot reuse a cursor", func(t *testing.T) {
		store := Store{Dir: t.TempDir()}
		require.NoError(t, store.Save(State{Version: 1, Source: "old", UIDValidity: 7}))
		require.ErrorIs(t, subject(Processor{Store: store, Source: "new"}, context.Background(), &Mailbox{UIDValidity: 7, UIDNext: 10}), ErrState)
	})
	t.Run("Canceled work does not enqueue or advance", func(t *testing.T) {
		store := Store{Dir: t.TempDir()}
		require.NoError(t, store.Save(State{Version: 1, Source: "source", UIDValidity: 7, LastUID: 2}))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		m := &Mailbox{UIDValidity: 7, UIDNext: 4, Search: func(uint32) ([]uint32, error) { return []uint32{3}, nil }}
		require.ErrorIs(t, subject(Processor{Store: store, Source: "source"}, ctx, m), context.Canceled)
		saved, err := store.Load()
		require.NoError(t, err)
		assert.Equal(t, uint32(2), saved.LastUID)
	})
}

func TestStore(t *testing.T) {
	subject := Store.Load
	t.Run("Invalid state stops instead of skipping mail", func(t *testing.T) {
		s := Store{Dir: t.TempDir()}
		require.NoError(t, s.Save(State{Version: 2, Source: "source", UIDValidity: 7}))
		_, err := subject(s)
		require.ErrorIs(t, err, ErrState)
	})
	t.Run("Only one process can own a state directory", func(t *testing.T) {
		s := Store{Dir: t.TempDir()}
		release, err := s.Lock()
		require.NoError(t, err)
		_, err = s.Lock()
		require.Error(t, err)
		release()
		release, err = s.Lock()
		require.NoError(t, err)
		release()
	})
}

func TestEventID(t *testing.T) {
	subject := NewEvent
	t.Run("Fits Dagu run IDs and distinguishes mail identities", func(t *testing.T) {
		e := subject("support", 7, 42)
		assert.Less(t, len(e.ID), 64)
		assert.Equal(t, e, subject("support", 7, 42))
		assert.NotEqual(t, e.ID, subject("sales", 7, 42).ID)
		assert.NotEqual(t, e.ID, subject("support", 8, 42).ID)
		assert.NotEqual(t, e.ID, subject("support", 7, 43).ID)
	})
}
