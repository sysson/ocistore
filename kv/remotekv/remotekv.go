// Package remotekv turns a networked key-value service with an atomic,
// revision-guarded multi-key commit into a serializable kv.Store.
//
// Every committed transaction advances a single head revision. A transaction
// records the head before reading and commits only if it is unchanged, so a
// concurrent writer forces a retry instead of a lost update. Read-only views
// re-check the head after reading and retry when it moved, giving them a
// consistent snapshot without native snapshot support.
package remotekv

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/sysson/ocistore/kv"
)

// Backend is the minimal remote contract a driver implements.
type Backend interface {
	// Get returns the latest value of key, or kv.ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
	// Scan has the semantics of kv.Reader.Scan over latest values.
	Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error
	// Revision returns the current head revision, 0 when nothing was
	// committed yet.
	Revision(ctx context.Context) (uint64, error)
	// Commit atomically applies writes and advances the head, only if the
	// head is still revision. Otherwise it returns kv.ErrConflict and applies
	// nothing.
	Commit(ctx context.Context, revision uint64, writes []kv.Write) error
	Close() error
}

// MaxAttempts bounds optimistic retries of a single transaction.
const MaxAttempts = 64

type Store struct {
	backend Backend
	onClose []func()
}

var _ kv.Store = (*Store)(nil)

func New(backend Backend) *Store {
	return &Store{backend: backend}
}

// OnClose registers fn to run after the backend is closed, for resources
// such as connections that the driver opened on the caller's behalf.
func (s *Store) OnClose(fn func()) {
	s.onClose = append(s.onClose, fn)
}

func (s *Store) Close() error {
	err := s.backend.Close()
	for _, fn := range s.onClose {
		fn()
	}
	return err
}

func (s *Store) View(ctx context.Context, fn func(kv.Reader) error) error {
	var lastErr error
	for attempt := range MaxAttempts {
		before, err := s.backend.Revision(ctx)
		if err != nil {
			return err
		}
		fnErr := fn(reader{backend: s.backend})
		after, err := s.backend.Revision(ctx)
		if err != nil {
			return err
		}
		if before == after {
			return fnErr
		}
		lastErr = fnErr
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
	return errors.Join(fmt.Errorf("metadata view did not observe a stable revision: %w", kv.ErrConflict), lastErr)
}

func (s *Store) Update(ctx context.Context, fn func(kv.Txn) error) error {
	for attempt := range MaxAttempts {
		revision, err := s.backend.Revision(ctx)
		if err != nil {
			return err
		}
		buffer := kv.NewBuffer(reader{backend: s.backend})
		fnErr := fn(buffer)
		if fnErr != nil {
			// The failure may come from reads that raced a commit; only
			// report it when the head shows the reads were consistent.
			current, err := s.backend.Revision(ctx)
			if err != nil {
				return err
			}
			if current == revision {
				return fnErr
			}
		} else {
			writes := buffer.Writes()
			if len(writes) == 0 {
				current, err := s.backend.Revision(ctx)
				if err != nil {
					return err
				}
				if current == revision {
					return nil
				}
			} else {
				err := s.backend.Commit(ctx, revision, writes)
				if err == nil {
					return nil
				}
				if !errors.Is(err, kv.ErrConflict) {
					return err
				}
			}
		}
		if err := backoff(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("metadata transaction retried %d times: %w", MaxAttempts, kv.ErrConflict)
}

func backoff(ctx context.Context, attempt int) error {
	delay := time.Duration(1+min(attempt, 8)) * time.Millisecond
	delay += time.Duration(rand.Int64N(int64(delay)))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type reader struct {
	backend Backend
}

func (r reader) Get(ctx context.Context, key string) ([]byte, error) {
	return r.backend.Get(ctx, key)
}

func (r reader) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	if err := kv.CheckPrefix(prefix); err != nil {
		return err
	}
	return r.backend.Scan(ctx, prefix, after, fn)
}
