// Package kvtest is a conformance suite every kv.Store driver must pass.
package kvtest

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/sysson/ocistore/kv"
)

// Run exercises store. The store must start empty.
func Run(t *testing.T, store kv.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("GetPutDelete", func(t *testing.T) {
		key := kv.Key("basic", "a")
		if err := store.View(ctx, func(r kv.Reader) error {
			_, err := r.Get(ctx, key)
			if !errors.Is(err, kv.ErrNotFound) {
				t.Fatalf("Get(missing) error = %v, want ErrNotFound", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		mustUpdate(t, store, func(tx kv.Txn) error { return tx.Put(key, []byte("one")) })
		if got := mustGet(t, store, key); got != "one" {
			t.Fatalf("Get = %q, want one", got)
		}
		mustUpdate(t, store, func(tx kv.Txn) error { return tx.Put(key, []byte{}) })
		if got := mustGet(t, store, key); got != "" {
			t.Fatalf("Get(empty value) = %q", got)
		}
		mustUpdate(t, store, func(tx kv.Txn) error { return tx.Delete(key) })
		if err := store.View(ctx, func(r kv.Reader) error {
			_, err := r.Get(ctx, key)
			if !errors.Is(err, kv.ErrNotFound) {
				t.Fatalf("Get(deleted) error = %v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ScanOrderPrefixAfter", func(t *testing.T) {
		keys := []string{
			kv.Key("scan", "a/b"), kv.Key("scan", "a-c"), kv.Key("scan", "a"),
			kv.Key("scan", "a", "nested"), kv.Key("scan", "b"), kv.Key("scanner", "x"),
		}
		mustUpdate(t, store, func(tx kv.Txn) error {
			for _, key := range keys {
				if err := tx.Put(key, []byte(key)); err != nil {
					return err
				}
			}
			return nil
		})
		got := scanAll(t, store, kv.Prefix("scan"), "")
		want := []string{
			kv.Key("scan", "a"), kv.Key("scan", "a", "nested"), kv.Key("scan", "a-c"),
			kv.Key("scan", "a/b"), kv.Key("scan", "b"),
		}
		assertKeys(t, got, want)
		assertKeys(t, scanAll(t, store, kv.Prefix("scan"), kv.Key("scan", "a-c")), want[3:])
		assertKeys(t, scanAll(t, store, kv.Prefix("scan", "a"), ""), []string{kv.Key("scan", "a", "nested")})

		var first []string
		if err := store.View(ctx, func(r kv.Reader) error {
			first = nil
			return r.Scan(ctx, kv.Prefix("scan"), "", func(key string, _ []byte) (bool, error) {
				first = append(first, key)
				return len(first) < 2, nil
			})
		}); err != nil {
			t.Fatal(err)
		}
		assertKeys(t, first, want[:2])
	})

	t.Run("ReadYourWrites", func(t *testing.T) {
		mustUpdate(t, store, func(tx kv.Txn) error {
			for _, k := range []string{"1", "3", "5"} {
				if err := tx.Put(kv.Key("ryw", k), []byte("old")); err != nil {
					return err
				}
			}
			return nil
		})
		mustUpdate(t, store, func(tx kv.Txn) error {
			if err := tx.Put(kv.Key("ryw", "2"), []byte("new")); err != nil {
				return err
			}
			if err := tx.Put(kv.Key("ryw", "3"), []byte("new")); err != nil {
				return err
			}
			if err := tx.Delete(kv.Key("ryw", "5")); err != nil {
				return err
			}
			if err := tx.Put(kv.Key("ryw", "6"), []byte("new")); err != nil {
				return err
			}
			value, err := tx.Get(ctx, kv.Key("ryw", "3"))
			if err != nil || string(value) != "new" {
				t.Fatalf("Get(written) = %q, %v", value, err)
			}
			if _, err := tx.Get(ctx, kv.Key("ryw", "5")); !errors.Is(err, kv.ErrNotFound) {
				t.Fatalf("Get(deleted in txn) error = %v", err)
			}
			var got []string
			err = tx.Scan(ctx, kv.Prefix("ryw"), "", func(key string, value []byte) (bool, error) {
				got = append(got, kv.Split(key)[1]+"="+string(value))
				return true, nil
			})
			if err != nil {
				return err
			}
			assertKeys(t, got, []string{"1=old", "2=new", "3=new", "6=new"})
			return nil
		})
	})

	t.Run("RollbackOnError", func(t *testing.T) {
		failure := errors.New("abort")
		err := store.Update(ctx, func(tx kv.Txn) error {
			if err := tx.Put(kv.Key("rollback", "x"), []byte("x")); err != nil {
				return err
			}
			return failure
		})
		if !errors.Is(err, failure) {
			t.Fatalf("Update error = %v, want abort", err)
		}
		assertKeys(t, scanAll(t, store, kv.Prefix("rollback"), ""), nil)
	})

	t.Run("ConcurrentIncrements", func(t *testing.T) {
		const workers = 8
		const perWorker = 10
		key := kv.Key("counter", "value")
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for range workers {
			wg.Go(func() {
				for range perWorker {
					err := store.Update(ctx, func(tx kv.Txn) error {
						n := 0
						value, err := tx.Get(ctx, key)
						if err == nil {
							n, err = strconv.Atoi(string(value))
							if err != nil {
								return err
							}
						} else if !errors.Is(err, kv.ErrNotFound) {
							return err
						}
						return tx.Put(key, []byte(strconv.Itoa(n+1)))
					})
					if err != nil {
						errs <- err
						return
					}
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if got := mustGet(t, store, key); got != strconv.Itoa(workers*perWorker) {
			t.Fatalf("counter = %s, want %d", got, workers*perWorker)
		}
	})

	t.Run("RejectsUnalignedPrefix", func(t *testing.T) {
		err := store.View(ctx, func(r kv.Reader) error {
			return r.Scan(ctx, "scan", "", func(string, []byte) (bool, error) { return true, nil })
		})
		if err == nil {
			t.Fatal("Scan accepted a prefix without separator")
		}
	})
}

func mustUpdate(t *testing.T, store kv.Store, fn func(kv.Txn) error) {
	t.Helper()
	if err := store.Update(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
}

func mustGet(t *testing.T, store kv.Store, key string) string {
	t.Helper()
	var value []byte
	if err := store.View(context.Background(), func(r kv.Reader) error {
		var err error
		value, err = r.Get(context.Background(), key)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return string(value)
}

func scanAll(t *testing.T, store kv.Store, prefix, after string) []string {
	t.Helper()
	var keys []string
	if err := store.View(context.Background(), func(r kv.Reader) error {
		keys = nil
		return r.Scan(context.Background(), prefix, after, func(key string, _ []byte) (bool, error) {
			keys = append(keys, key)
			return true, nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

func assertKeys(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("keys = %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("keys = %q, want %q", got, want)
		}
	}
}
