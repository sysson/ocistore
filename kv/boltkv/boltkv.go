// Package boltkv is a kv.Store backed by a local bbolt database. bbolt holds
// an exclusive file lock, so only one registry replica can use it.
package boltkv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/sysson/ocistore/kv"
	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("kv")

// Config selects the bbolt driver.
type Config struct {
	// Path is the absolute database file path. Its directory is created with
	// mode 0750 and the file with mode 0600.
	Path string `json:"path"`
}

var _ kv.Config = Config{}

func (c Config) Validate() error {
	if c.Path == "" {
		return errors.New("bbolt.path must not be empty")
	}
	if !filepath.IsAbs(c.Path) {
		return fmt.Errorf("bbolt.path %q must be absolute", c.Path)
	}
	return nil
}

func (c Config) Open(context.Context) (kv.Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	path := filepath.Clean(c.Path)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("creating bbolt metadata directory: %w", err)
	}
	return Open(path, 0o600)
}

type Store struct {
	db *bolt.DB
}

var _ kv.Store = (*Store)(nil)

func Open(path string, mode os.FileMode) (*Store, error) {
	db, err := bolt.Open(path, mode, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("opening bbolt metadata database: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initializing bbolt metadata database: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) View(ctx context.Context, fn func(kv.Reader) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.View(func(tx *bolt.Tx) error { return fn(txn{tx.Bucket(bucketName)}) })
}

func (s *Store) Update(ctx context.Context, fn func(kv.Txn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error { return fn(txn{tx.Bucket(bucketName)}) })
}

type txn struct{ b *bolt.Bucket }

func (t txn) Get(_ context.Context, key string) ([]byte, error) {
	value := t.b.Get([]byte(key))
	if value == nil {
		return nil, kv.ErrNotFound
	}
	return slices.Clone(value), nil
}

func (t txn) Put(key string, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	return t.b.Put([]byte(key), value)
}

func (t txn) Delete(key string) error {
	return t.b.Delete([]byte(key))
}

func (t txn) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	if err := kv.CheckPrefix(prefix); err != nil {
		return err
	}
	start := max(after, prefix)
	// Collect before calling fn, since fn may write through the same bucket
	// and bbolt cursors are invalidated by writes.
	type entry struct {
		key   string
		value []byte
	}
	const page = 256
	for {
		entries := make([]entry, 0, page)
		c := t.b.Cursor()
		for k, v := c.Seek([]byte(start)); k != nil && bytes.HasPrefix(k, []byte(prefix)) && len(entries) < page; k, v = c.Next() {
			if string(k) <= after {
				continue
			}
			entries = append(entries, entry{string(k), slices.Clone(v)})
		}
		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err := fn(e.key, e.value)
			if err != nil || !more {
				return err
			}
		}
		if len(entries) < page {
			return nil
		}
		after = entries[len(entries)-1].key
		start = after
	}
}
