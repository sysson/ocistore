// Package memkv is an in-process kv.Store. Its contents are lost when the
// process exits.
package memkv

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/sysson/ocistore/kv"
)

// Config selects the in-memory driver. It has no settings.
type Config struct{}

var _ kv.Config = Config{}

func (Config) Validate() error { return nil }

func (Config) Open(context.Context) (kv.Store, error) { return New(), nil }

// Store keeps sorted keys in memory. Writers are serialized, so transactions
// never conflict.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	keys []string
}

var _ kv.Store = (*Store)(nil)

func New() *Store {
	return &Store{data: map[string][]byte{}}
}

func (s *Store) Close() error { return nil }

func (s *Store) View(ctx context.Context, fn func(kv.Reader) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fn(reader{s})
}

func (s *Store) Update(ctx context.Context, fn func(kv.Txn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	buffer := kv.NewBuffer(reader{s})
	if err := fn(buffer); err != nil {
		return err
	}
	for _, write := range buffer.Writes() {
		i, found := slices.BinarySearch(s.keys, write.Key)
		if write.Delete {
			if found {
				s.keys = slices.Delete(s.keys, i, i+1)
				delete(s.data, write.Key)
			}
			continue
		}
		if !found {
			s.keys = slices.Insert(s.keys, i, write.Key)
		}
		s.data[write.Key] = write.Value
	}
	return nil
}

type reader struct{ s *Store }

func (r reader) Get(ctx context.Context, key string) ([]byte, error) {
	value, ok := r.s.data[key]
	if !ok {
		return nil, kv.ErrNotFound
	}
	return slices.Clone(value), nil
}

func (r reader) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	if err := kv.CheckPrefix(prefix); err != nil {
		return err
	}
	start := max(after, prefix)
	i := sort.SearchStrings(r.s.keys, start)
	for ; i < len(r.s.keys); i++ {
		key := r.s.keys[i]
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		if key <= after {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		more, err := fn(key, slices.Clone(r.s.data[key]))
		if err != nil || !more {
			return err
		}
	}
	return nil
}
