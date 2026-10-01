// Package kv defines the ordered, transactional key-value contract that
// registry metadata is stored in. Each driver has its own typed configuration;
// all metadata backends share one indexed layout and MetadataStore implementation.
package kv

import (
	"context"
	"errors"
	"strings"
)

var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("kv: key not found")

	// ErrConflict indicates that a concurrent commit invalidated the reads of
	// an optimistic transaction. Store.Update retries on it internally.
	ErrConflict = errors.New("kv: transaction conflict")
)

// Separator joins the parts of a key. Parts never contain it, which keeps
// part-wise ordering identical to byte ordering and makes every prefix scan
// part-aligned.
const Separator = "\x00"

// Reader reads a consistent view of the store.
type Reader interface {
	// Get returns the value for key, or ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)

	// Scan calls fn, in ascending key order, for each key starting with
	// prefix and strictly greater than after (when after is not empty).
	// Scanning stops when fn returns false or an error. Prefix must be empty
	// or end with Separator.
	Scan(ctx context.Context, prefix, after string, fn func(key string, value []byte) (bool, error)) error
}

// Txn is a read-write transaction. Writes are visible to later reads in the
// same transaction and are committed atomically.
type Txn interface {
	Reader
	Put(key string, value []byte) error
	Delete(key string) error
}

// Store is an ordered key-value store with serializable transactions.
//
// The functions passed to View and Update may run more than once when a
// driver detects a concurrent change, so they must not have side effects
// beyond the transaction and must reset any results they collect.
type Store interface {
	View(ctx context.Context, fn func(Reader) error) error
	Update(ctx context.Context, fn func(Txn) error) error
	Close() error
}

// Key joins parts into a key. Parts must not contain Separator.
func Key(parts ...string) string {
	return strings.Join(parts, Separator)
}

// Prefix returns the scan prefix matching every key that extends parts.
func Prefix(parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	return Key(parts...) + Separator
}

// Split returns the parts of key.
func Split(key string) []string {
	return strings.Split(key, Separator)
}

// ValidPart reports whether s can be used as a key part.
func ValidPart(s string) bool {
	return !strings.Contains(s, Separator)
}

// CheckPrefix validates a scan prefix.
func CheckPrefix(prefix string) error {
	if prefix != "" && !strings.HasSuffix(prefix, Separator) {
		return errors.New("kv: scan prefix must end with the key separator")
	}
	return nil
}

// Config is a driver's typed configuration. Validate checks the settings
// without contacting anything; Open connects and returns the store.
type Config interface {
	Validate() error
	Open(ctx context.Context) (Store, error)
}
