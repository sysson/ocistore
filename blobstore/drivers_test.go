package blobstore_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sysson/ocistore/blobstore"
	"github.com/sysson/ocistore/blobstore/azureblob"
	"github.com/sysson/ocistore/blobstore/fileblob"
	"github.com/sysson/ocistore/blobstore/gcsblob"
	"github.com/sysson/ocistore/blobstore/memblob"
	"github.com/sysson/ocistore/blobstore/s3blob"
)

type driver interface {
	Validate() error
	URL() string
	Open(context.Context) (*blobstore.Store, error)
}

func TestDrivers(t *testing.T) {
	tests := map[string]struct {
		cfg  driver
		want string
	}{
		"file": {fileblob.Config{Path: filepath.Join(t.TempDir(), "blobs")}, ""},
		"mem":  {memblob.Config{}, "mem://"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if err := test.cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			if test.want != "" && test.cfg.URL() != test.want {
				t.Fatalf("URL() = %s, want %s", test.cfg.URL(), test.want)
			}
			store, err := test.cfg.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_ = store.Close()
		})
	}
}

func TestCleanupStaged(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "blobs")
	store, err := fileblob.Open(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := os.MkdirAll(filepath.Join(root, "pending"), 0o750); err != nil {
		t.Fatal(err)
	}
	oldKey := filepath.Join(root, "pending", "orphan")
	newKey := filepath.Join(root, "pending", "active")
	for _, key := range []string{oldKey, newKey} {
		if err := os.WriteFile(key, []byte("staged"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := time.Now().Add(-time.Hour)
	if err := os.Chtimes(oldKey, cutoff.Add(-time.Hour), cutoff.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.CleanupStaged(ctx, cutoff); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old staged object still exists: %v", err)
	}
	if _, err := os.Stat(newKey); err != nil {
		t.Fatalf("recent staged object was removed: %v", err)
	}
}

func TestInvalidDrivers(t *testing.T) {
	for name, cfg := range map[string]driver{
		"relative file": fileblob.Config{Path: "blobs"},
		"empty file":    fileblob.Config{},
	} {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}

func TestCloudDrivers(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AZURE_STORAGE_KEY", "dGVzdA==")
	for name, test := range map[string]struct {
		cfg  driver
		want string
	}{
		"s3": {s3blob.Config{Bucket: "registry-test", Prefix: "registry", Region: "eu-west-1", Endpoint: "http://127.0.0.1:1",
			UsePathStyle: true, DisableHTTPS: true, SSEType: "aws:kms", KMSKeyID: "key"},
			"s3://registry-test?disable_https=true&endpoint=http%3A%2F%2F127.0.0.1%3A1&kmskeyid=key&prefix=registry%2F&region=eu-west-1&ssetype=aws%3Akms&use_path_style=true"},
		"gcs":   {gcsblob.Config{Bucket: "registry-blobs", Prefix: "a/b/", Anonymous: true}, "gs://registry-blobs?anonymous=true&prefix=a%2Fb%2F"},
		"azure": {azureblob.Config{Container: "registry-test", AccountName: "acct", Protocol: "http", Domain: "127.0.0.1:1"}, "azblob://registry-test?domain=127.0.0.1%3A1&protocol=http&storage_account=acct"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := test.cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			if got := test.cfg.URL(); got != test.want {
				t.Fatalf("URL() = %s, want %s", got, test.want)
			}
			store, err := test.cfg.Open(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_ = store.Close()
		})
	}
}

func TestInvalidCloudDrivers(t *testing.T) {
	for name, cfg := range map[string]driver{
		"s3 bucket":       s3blob.Config{Bucket: "Bad_Bucket"},
		"s3 endpoint":     s3blob.Config{Bucket: "registry-test", Endpoint: "minio:9000"},
		"s3 sse":          s3blob.Config{Bucket: "registry-test", SSEType: "rot13"},
		"s3 kms no sse":   s3blob.Config{Bucket: "registry-test", KMSKeyID: "k"},
		"s3 prefix":       s3blob.Config{Bucket: "registry-test", Prefix: "/abs"},
		"gcs bucket":      gcsblob.Config{},
		"azure container": azureblob.Config{Container: "Bad--name"},
		"azure protocol":  azureblob.Config{Container: "registry-test", Protocol: "ftp"},
	} {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}
