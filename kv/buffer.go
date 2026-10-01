package kv

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
)

// Buffer is a Txn that records writes in memory on top of a base Reader.
// Drivers without native read-your-writes transactions use it and commit
// Writes when the transaction function succeeds.
type Buffer struct {
	base   Reader
	writes map[string][]byte
	// deleted marks keys whose pending write is a delete.
	deleted map[string]bool
}

// Write is a pending mutation. A nil Value with Delete set removes Key.
type Write struct {
	Key    string
	Value  []byte
	Delete bool
}

// NewBuffer returns a write buffer over base.
func NewBuffer(base Reader) *Buffer {
	return &Buffer{base: base, writes: map[string][]byte{}, deleted: map[string]bool{}}
}

func (b *Buffer) Get(ctx context.Context, key string) ([]byte, error) {
	if b.deleted[key] {
		return nil, ErrNotFound
	}
	if value, ok := b.writes[key]; ok {
		return slices.Clone(value), nil
	}
	return b.base.Get(ctx, key)
}

func (b *Buffer) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("kv: empty key")
	}
	delete(b.deleted, key)
	b.writes[key] = slices.Clone(value)
	if b.writes[key] == nil {
		b.writes[key] = []byte{}
	}
	return nil
}

func (b *Buffer) Delete(key string) error {
	delete(b.writes, key)
	b.deleted[key] = true
	return nil
}

// Writes returns pending mutations sorted by key.
func (b *Buffer) Writes() []Write {
	writes := make([]Write, 0, len(b.writes)+len(b.deleted))
	for key, value := range b.writes {
		writes = append(writes, Write{Key: key, Value: value})
	}
	for key := range b.deleted {
		writes = append(writes, Write{Key: key, Delete: true})
	}
	sort.Slice(writes, func(i, j int) bool { return writes[i].Key < writes[j].Key })
	return writes
}

func (b *Buffer) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	if err := CheckPrefix(prefix); err != nil {
		return err
	}
	pending := make([]string, 0)
	for key := range b.writes {
		if strings.HasPrefix(key, prefix) && key > after {
			pending = append(pending, key)
		}
	}
	for key := range b.deleted {
		if strings.HasPrefix(key, prefix) && key > after {
			pending = append(pending, key)
		}
	}
	sort.Strings(pending)

	stopped := false
	emitPendingBefore := func(limit string, inclusive bool) (bool, error) {
		for len(pending) > 0 && (pending[0] < limit || (inclusive && pending[0] == limit)) {
			key := pending[0]
			pending = pending[1:]
			if b.deleted[key] {
				continue
			}
			more, err := fn(key, slices.Clone(b.writes[key]))
			if err != nil || !more {
				return false, err
			}
		}
		return true, nil
	}

	err := b.base.Scan(ctx, prefix, after, func(key string, value []byte) (bool, error) {
		more, err := emitPendingBefore(key, false)
		if err != nil || !more {
			stopped = true
			return false, err
		}
		if len(pending) > 0 && pending[0] == key {
			// The pending write replaces or removes the stored value.
			more, err := emitPendingBefore(key, true)
			if err != nil || !more {
				stopped = true
			}
			return more, err
		}
		more, err = fn(key, value)
		if !more {
			stopped = true
		}
		return more, err
	})
	if err != nil || stopped {
		return err
	}
	for len(pending) > 0 {
		key := pending[0]
		pending = pending[1:]
		if b.deleted[key] {
			continue
		}
		more, err := fn(key, slices.Clone(b.writes[key]))
		if err != nil || !more {
			return err
		}
	}
	return nil
}
