package remotekv_test

import (
	"context"
	"sync"
	"testing"

	"github.com/sysson/ocistore/kv"
	"github.com/sysson/ocistore/kv/kvtest"
	"github.com/sysson/ocistore/kv/memkv"
	"github.com/sysson/ocistore/kv/remotekv"
)

// fakeBackend emulates a remote service: reads are not transactional, and
// commits are guarded only by the head revision.
type fakeBackend struct {
	mu       sync.Mutex
	data     *memkv.Store
	revision uint64
}

func (f *fakeBackend) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := f.data.View(ctx, func(r kv.Reader) error {
		var err error
		value, err = r.Get(ctx, key)
		return err
	})
	return value, err
}

func (f *fakeBackend) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	type entry struct {
		key   string
		value []byte
	}
	var entries []entry
	if err := f.data.View(ctx, func(r kv.Reader) error {
		return r.Scan(ctx, prefix, after, func(k string, v []byte) (bool, error) {
			entries = append(entries, entry{k, v})
			return true, nil
		})
	}); err != nil {
		return err
	}
	for _, e := range entries {
		more, err := fn(e.key, e.value)
		if err != nil || !more {
			return err
		}
	}
	return nil
}

func (f *fakeBackend) Revision(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.revision, nil
}

func (f *fakeBackend) Commit(ctx context.Context, revision uint64, writes []kv.Write) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if revision != f.revision {
		return kv.ErrConflict
	}
	f.revision++
	return f.data.Update(ctx, func(tx kv.Txn) error {
		for _, w := range writes {
			if w.Delete {
				_ = tx.Delete(w.Key)
			} else {
				_ = tx.Put(w.Key, w.Value)
			}
		}
		return nil
	})
}

func (f *fakeBackend) Close() error { return nil }

func TestConformance(t *testing.T) {
	kvtest.Run(t, remotekv.New(&fakeBackend{data: memkv.New()}))
}
