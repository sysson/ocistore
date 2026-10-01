package ocistore

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/docker/oci"
	"github.com/docker/oci/ocidigest"
	"github.com/sysson/ocistore/blobstore"
	"github.com/sysson/ocistore/blobstore/fileblob"
	"github.com/sysson/ocistore/kv/boltkv"
	"github.com/sysson/ocistore/kvmeta"
)

func TestBlobDeleteAndGarbageCollection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	contentStore, err := fileblob.Open(ctx, filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contentStore.Close() }()
	metadataStore, err := kvmeta.Open(ctx, boltkv.Config{Path: filepath.Join(dir, "metadata.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metadataStore.Close() }()

	registry, err := New(contentStore, metadataStore)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("garbage collect me")
	digest := ocidigest.FromBytes(content)
	descriptor, err := registry.PushBlob(ctx, "team/app", oci.Descriptor{
		Digest: digest,
		Size:   int64(len(content)),
	}, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.MediaType != "application/octet-stream" {
		t.Fatalf("blob media type = %q, want application/octet-stream", descriptor.MediaType)
	}
	if err := registry.DeleteBlob(ctx, "team/app", digest); err != nil {
		t.Fatal(err)
	}
	if err := registry.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := contentStore.Size(ctx, digest); !errors.Is(err, blobstore.ErrObjectUnknown) {
		t.Fatalf("content size error = %v, want ErrObjectUnknown", err)
	}
	if repositories, err := metadataStore.Repositories(ctx, "", 10); err != nil || len(repositories) != 0 {
		t.Fatalf("repositories after deleting final blob = %v, %v; want none", repositories, err)
	}
	if _, err := registry.ResolveBlob(ctx, "team/app", digest); !errors.Is(err, oci.ErrNameUnknown) {
		t.Fatalf("ResolveBlob error = %v, want ErrNameUnknown", err)
	}
}

func TestGarbageCollectionFindsUnqueuedBlobs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	contentStore, err := fileblob.Open(ctx, filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contentStore.Close() }()
	metadataStore, err := kvmeta.Open(ctx, boltkv.Config{Path: filepath.Join(dir, "metadata.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = metadataStore.Close() }()
	registry, err := New(contentStore, metadataStore)
	if err != nil {
		t.Fatal(err)
	}

	live := []byte("referenced")
	liveDigest := ocidigest.FromBytes(live)
	if _, err := registry.PushBlob(ctx, "team/app", oci.Descriptor{Digest: liveDigest, Size: int64(len(live))}, bytes.NewReader(live)); err != nil {
		t.Fatal(err)
	}
	orphan := []byte("interrupted before metadata commit")
	orphanDigest := ocidigest.FromBytes(orphan)
	if err := contentStore.PutBlob(ctx, orphanDigest, int64(len(orphan)), bytes.NewReader(orphan)); err != nil {
		t.Fatal(err)
	}
	if err := registry.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := contentStore.Size(ctx, orphanDigest); !errors.Is(err, blobstore.ErrObjectUnknown) {
		t.Fatalf("orphan size error = %v, want ErrObjectUnknown", err)
	}
	if _, err := contentStore.Size(ctx, liveDigest); err != nil {
		t.Fatalf("referenced blob was collected: %v", err)
	}
	interrupted := []byte("deleted before completing claim")
	interruptedDigest := ocidigest.FromBytes(interrupted)
	if err := contentStore.PutBlob(ctx, interruptedDigest, int64(len(interrupted)), bytes.NewReader(interrupted)); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := metadataStore.ClaimGarbage(ctx, interruptedDigest); err != nil || !claimed {
		t.Fatalf("claim unqueued blob: claimed %v, error %v", claimed, err)
	}
	if err := contentStore.DeleteBlob(ctx, interruptedDigest); err != nil {
		t.Fatal(err)
	}
	if err := registry.CollectGarbage(ctx); err != nil {
		t.Fatalf("resuming garbage collection after deletion: %v", err)
	}
	if candidates, err := metadataStore.GarbageCandidates(ctx, 10); err != nil || len(candidates) != 0 {
		t.Fatalf("candidates after recovery = %v, %v; want none", candidates, err)
	}
}
