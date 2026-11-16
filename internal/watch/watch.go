package watch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
)

type Event struct {
	ID          string
	Account     string
	UIDValidity uint32
	UID         uint32
}

func NewEvent(account string, validity, uid uint32) Event {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/INBOX/%d/%d", account, validity, uid)))
	// Dagu run IDs must be shorter than 64 characters. Retain 192 hash bits.
	return Event{ID: fmt.Sprintf("mail-%x", sum[:24]), Account: account, UIDValidity: validity, UID: uid}
}

type Mailbox struct {
	UIDValidity uint32
	UIDNext     uint32
	Search      func(after uint32) ([]uint32, error)
	Wait        func(context.Context) error
	Close       func() error
}
type Processor struct {
	Store    Store
	Source   string
	Account  string
	Backfill bool
	Enqueue  func(context.Context, Event) error
}

// Sync advances only after durable enqueue. Retrying a UID uses the same event ID.
func (p Processor) Sync(ctx context.Context, m *Mailbox) error {
	if m.UIDValidity == 0 || m.UIDNext == 0 {
		return fmt.Errorf("server omitted UIDVALIDITY or UIDNEXT")
	}
	state, err := p.Store.Load()
	if err != nil {
		return err
	}
	if state == nil {
		state = &State{Version: 1, Source: p.Source, UIDValidity: m.UIDValidity}
		if !p.Backfill {
			state.LastUID = m.UIDNext - 1
		}
		if err = p.Store.Save(*state); err != nil {
			return err
		}
	}
	if state.Source != p.Source {
		return fmt.Errorf("%w: state belongs to another source or DAG", ErrState)
	}
	if state.UIDValidity != m.UIDValidity {
		return fmt.Errorf("%w: UIDVALIDITY changed; choose a fresh state directory and first-run policy", ErrState)
	}
	if state.LastUID == math.MaxUint32 {
		return nil
	}
	uids, err := m.Search(state.LastUID)
	if err != nil {
		return err
	}
	sort.Slice(uids, func(i, j int) bool { return uids[i] < uids[j] })
	for _, uid := range uids {
		// IMAP's n:* range may return a UID below n when n is above the highest UID.
		if uid <= state.LastUID {
			continue
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		event := NewEvent(p.Account, state.UIDValidity, uid)
		if err = p.Enqueue(ctx, event); err != nil {
			return fmt.Errorf("enqueue event %s: %w", event.ID, err)
		}
		state.LastUID = uid
		if err = p.Store.Save(*state); err != nil {
			return err
		}
	}
	return nil
}
