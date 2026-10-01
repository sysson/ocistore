package kvmeta_test

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/docker/oci"
	"github.com/docker/oci/ocidigest"
	"github.com/sysson/ocistore/backend"
	"github.com/sysson/ocistore/kv"
	"github.com/sysson/ocistore/kv/boltkv"
	"github.com/sysson/ocistore/kv/etcdkv"
	"github.com/sysson/ocistore/kv/kvtest"
	"github.com/sysson/ocistore/kv/memkv"
	"github.com/sysson/ocistore/kv/natskv"
	"github.com/sysson/ocistore/kvmeta"
)

func TestMetadataMem(t *testing.T) {
	runSuite(t, func(t *testing.T) kv.Store { return memkv.New() })
}

func TestMetadataBolt(t *testing.T) {
	runSuite(t, func(t *testing.T) kv.Store {
		store, err := boltkv.Open(filepath.Join(t.TempDir(), "meta.db"), 0o600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func runSuite(t *testing.T, open func(*testing.T) kv.Store) {
	newStore := func(t *testing.T) *kvmeta.Store {
		store, err := kvmeta.New(context.Background(), open(t))
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	t.Run("GarbageClaimFencing", func(t *testing.T) { testGarbageClaim(t, newStore(t)) })
	t.Run("EmptyRepositoryPruning", func(t *testing.T) { testEmptyRepositoryPruning(t, newStore(t)) })
	t.Run("ImageGraphIndexes", func(t *testing.T) { testImageGraph(t, newStore(t)) })
	t.Run("ExpiryOrdering", func(t *testing.T) { testExpiry(t, newStore(t)) })
	t.Run("IndexWithMissingChildren", func(t *testing.T) { testIndexMissingChildren(t, newStore(t)) })
}

type fixture struct {
	t     *testing.T
	store *kvmeta.Store
	ctx   context.Context
}

func TestSchemaInitialization(t *testing.T) {
	ctx := t.Context()
	raw := memkv.New()
	if _, err := kvmeta.New(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := raw.View(ctx, func(reader kv.Reader) error {
		version, err := reader.Get(ctx, kv.Key("schema"))
		if err != nil {
			return err
		}
		if string(version) != "1" {
			t.Errorf("schema version = %q, want 1", version)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := kvmeta.New(ctx, raw); err != nil {
		t.Fatal(err)
	}
	if err := raw.Update(ctx, func(tx kv.Txn) error {
		return tx.Put(kv.Key("schema"), []byte("unsupported"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := kvmeta.New(ctx, raw); err == nil {
		t.Fatal("New() with unsupported schema = nil error")
	}
}

func testEmptyRepositoryPruning(t *testing.T, store *kvmeta.Store) {
	ctx := context.Background()
	f := fixture{t: t, store: store, ctx: ctx}
	uploadBlob := f.blob("with-upload", "upload-content")
	if err := store.CreateUpload(ctx, backend.UploadSession{ID: "upload", Repository: "with-upload", StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	reservedBlob := f.blob("with-reservation", "reserved-content")
	reservation, err := store.ReserveContent(ctx, "with-reservation", oci.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    ocidigest.FromBytes([]byte("reserved")),
		Size:      int64(len("reserved")),
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	blob := f.blob("with-blob", "content")
	manifest := f.manifest("with-manifest", "manifest", backend.ManifestRecord{Tags: []string{"latest"}})

	if _, err := store.DeleteBlob(ctx, "with-upload", uploadBlob.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteBlob(ctx, "with-reservation", reservedBlob.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteBlob(ctx, "with-blob", blob.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteManifest(ctx, "with-manifest", manifest.Digest); err != nil {
		t.Fatal(err)
	}
	repositories, err := store.Repositories(ctx, "", 10)
	if err != nil || !slices.Equal(repositories, []string{"with-reservation", "with-upload"}) {
		t.Fatalf("repositories with active state = %v, %v; want reservation and upload repositories", repositories, err)
	}

	if err := store.DeleteUpload(ctx, "with-upload", "upload"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseReservation(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	repositories, err = store.Repositories(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(repositories) != 0 {
		t.Fatalf("repositories after deleting final content = %v, want none", repositories)
	}

	desc := oci.Descriptor{MediaType: "application/octet-stream", Digest: ocidigest.FromBytes([]byte("recreated")), Size: int64(len("recreated"))}
	reservation, err = store.ReserveContent(ctx, "recreated", desc, time.Now())
	if err != nil {
		t.Fatalf("ReserveContent recreates repository: %v", err)
	}
	if err := store.ReleaseReservation(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	repositories, err = store.Repositories(ctx, "", 10)
	if err != nil || len(repositories) != 0 {
		t.Fatalf("repositories after releasing recreated reservation = %v, %v; want none", repositories, err)
	}
}

func (f fixture) blob(repo, content string) oci.Descriptor {
	f.t.Helper()
	desc := oci.Descriptor{MediaType: "application/octet-stream", Digest: ocidigest.FromBytes([]byte(content)), Size: int64(len(content))}
	if err := f.store.EnsureRepository(f.ctx, repo); err != nil {
		f.t.Fatal(err)
	}
	reservation, err := f.store.ReserveContent(f.ctx, repo, desc, time.Now())
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.RecordBlob(f.ctx, reservation); err != nil {
		f.t.Fatal(err)
	}
	return desc
}

func (f fixture) manifest(repo, content string, record backend.ManifestRecord) oci.Descriptor {
	f.t.Helper()
	desc := oci.Descriptor{MediaType: oci.MediaTypeImageManifest, Digest: ocidigest.FromBytes([]byte(content)), Size: int64(len(content))}
	if record.Descriptor.MediaType != "" {
		desc.MediaType = record.Descriptor.MediaType
	}
	record.Descriptor = desc
	reservation, err := f.store.ReserveContent(f.ctx, repo, desc, time.Now())
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := f.store.PutManifest(f.ctx, reservation, record); err != nil {
		f.t.Fatal(err)
	}
	return desc
}

func testGarbageClaim(t *testing.T, store *kvmeta.Store) {
	ctx := context.Background()
	f := fixture{t, store, ctx}
	desc := f.blob("team/app", "content")
	digest := desc.Digest

	if _, claimed, err := store.ClaimGarbage(ctx, digest); err != nil || claimed {
		t.Fatalf("claim reachable digest = claimed %v, error %v; want false, nil", claimed, err)
	}
	result, err := store.DeleteBlob(ctx, "team/app", digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.GarbageCandidates) != 1 || result.GarbageCandidates[0] != digest {
		t.Fatalf("garbage candidates = %v, want [%s]", result.GarbageCandidates, digest)
	}
	lease, claimed, err := store.ClaimGarbage(ctx, digest)
	if err != nil || !claimed || lease == nil {
		t.Fatalf("claim deleted digest = lease %v, claimed %v, error %v", lease, claimed, err)
	}
	again, _, err := store.ClaimGarbage(ctx, digest)
	if err != nil || again.Token != lease.Token {
		t.Fatalf("repeated claim = %v, %v; want same token", again, err)
	}
	if _, err := store.ReserveContent(ctx, "team/app", desc, time.Now()); !errors.Is(err, backend.ErrGCLocked) {
		t.Fatalf("ReserveContent error = %v, want ErrGCLocked", err)
	}
	if err := store.CompleteGarbage(ctx, backend.GarbageLease{Digest: digest, Token: "wrong"}); err == nil {
		t.Fatal("CompleteGarbage with a mismatched token succeeded")
	}
	if err := store.CompleteGarbage(ctx, *lease); err != nil {
		t.Fatal(err)
	}
	candidates, err := store.GarbageCandidates(ctx, 10)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("garbage candidates after completion = %v, %v; want none", candidates, err)
	}
}

func testImageGraph(t *testing.T, store *kvmeta.Store) {
	ctx := context.Background()
	f := fixture{t, store, ctx}

	shared := f.blob("team/app", "shared-layer")
	own := f.blob("team/app", "own-layer")
	config := f.blob("team/app", "config")
	image := f.manifest("team/app", "image", backend.ManifestRecord{
		References: []oci.Digest{config.Digest, shared.Digest, own.Digest},
		Config:     &config,
		Layers:     []oci.Descriptor{shared, own},
		Tags:       []string{"v1", "latest"},
	})
	index := f.manifest("team/app", "index", backend.ManifestRecord{
		Descriptor: oci.Descriptor{MediaType: oci.MediaTypeImageIndex},
		References: []oci.Digest{image.Digest},
		Manifests:  []oci.Descriptor{image},
		Tags:       []string{"multi"},
	})
	other := f.blob("team/other", "shared-layer")
	otherConfig := f.blob("team/other", "other-config")
	f.manifest("team/other", "other-image", backend.ManifestRecord{
		References: []oci.Digest{otherConfig.Digest, other.Digest},
		Config:     &otherConfig,
		Layers:     []oci.Descriptor{other},
		Tags:       []string{"v1"},
	})
	subject := image.Digest
	signature := f.manifest("team/app", "signature", backend.ManifestRecord{
		Subject:      &subject,
		ArtifactType: "application/vnd.dev.cosign",
		Annotations:  map[string]string{"k": "v"},
	})

	record, err := store.Manifest(ctx, "team/app", image.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(record.Tags, []string{"latest", "v1"}) || record.Config == nil || len(record.Layers) != 2 || record.PushedAt.IsZero() {
		t.Fatalf("manifest record = %+v", record)
	}

	dependents, err := store.Dependents(ctx, shared.Digest, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dependents) != 2 || dependents[0].Repository != "team/app" || dependents[1].Repository != "team/other" {
		t.Fatalf("dependents of shared layer = %+v", dependents)
	}
	dependents, err = store.Dependents(ctx, image.Digest, 10)
	if err != nil || len(dependents) != 1 || dependents[0].Manifest != index.Digest {
		t.Fatalf("dependents of image = %+v, %v", dependents, err)
	}
	locations, err := store.Locations(ctx, shared.Digest)
	if err != nil || len(locations) != 2 {
		t.Fatalf("locations = %+v, %v", locations, err)
	}

	referrers, err := store.Referrers(ctx, "team/app", image.Digest, "", "", 10)
	if err != nil || len(referrers) != 1 || referrers[0].Digest != signature.Digest ||
		referrers[0].ArtifactType != "application/vnd.dev.cosign" || referrers[0].Annotations["k"] != "v" {
		t.Fatalf("referrers = %+v, %v", referrers, err)
	}
	if filtered, _ := store.Referrers(ctx, "team/app", image.Digest, "other/type", "", 10); len(filtered) != 0 {
		t.Fatalf("filtered referrers = %+v", filtered)
	}

	tags, err := store.TagRecords(ctx, "team/app", "", 10)
	if err != nil || len(tags) != 3 || tags[0].Tag != "latest" || tags[1].Tag != "multi" || tags[1].Digest != index.Digest {
		t.Fatalf("tag records = %+v, %v", tags, err)
	}
	manifests, err := store.Manifests(ctx, "team/app", "", 10)
	if err != nil || len(manifests) != 3 {
		t.Fatalf("manifests = %d, %v", len(manifests), err)
	}
	blobs, err := store.Blobs(ctx, "team/app", "", 2)
	if err != nil || len(blobs) != 2 {
		t.Fatalf("blobs page = %d, %v", len(blobs), err)
	}

	// Retagging moves the reverse index.
	f.manifest("team/app", "index", backend.ManifestRecord{
		Descriptor: oci.Descriptor{MediaType: oci.MediaTypeImageIndex},
		References: []oci.Digest{image.Digest},
		Manifests:  []oci.Descriptor{image},
		Tags:       []string{"latest"},
	})
	if tags, _ := store.TagsFor(ctx, "team/app", image.Digest); !slices.Equal(tags, []string{"v1"}) {
		t.Fatalf("tags for image after retag = %v", tags)
	}

	if _, err := store.DeleteBlob(ctx, "team/app", own.Digest); !errors.Is(err, oci.ErrReferenced) {
		t.Fatalf("DeleteBlob(referenced) error = %v", err)
	}
	if _, err := store.DeleteManifest(ctx, "team/app", index.Digest); err != nil {
		t.Fatal(err)
	}
	result, err := store.DeleteManifest(ctx, "team/app", image.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(result.GarbageCandidates, image.Digest) {
		t.Fatalf("candidates after deleting image = %v", result.GarbageCandidates)
	}
	if referrers, _ := store.Referrers(ctx, "team/app", image.Digest, "", "", 10); len(referrers) != 1 {
		t.Fatalf("referrer entry must remain until the referrer is deleted: %+v", referrers)
	}
	if tags, _ := store.TagsFor(ctx, "team/app", image.Digest); len(tags) != 0 {
		t.Fatalf("tags remain after manifest delete: %v", tags)
	}
	if dependents, _ := store.Dependents(ctx, own.Digest, 10); len(dependents) != 0 {
		t.Fatalf("dependents remain after manifest delete: %+v", dependents)
	}
	result, err = store.DeleteBlob(ctx, "team/app", shared.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.GarbageCandidates) != 0 {
		t.Fatalf("shared layer still held by team/other became a candidate: %v", result.GarbageCandidates)
	}
}

func testExpiry(t *testing.T, store *kvmeta.Store) {
	ctx := context.Background()
	if err := store.EnsureRepository(ctx, "team/app"); err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	for i, id := range []string{"c", "a", "b"} {
		if err := store.CreateUpload(ctx, backend.UploadSession{ID: backend.UploadID(id), Repository: "team/app", StartedAt: base.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	expired, err := store.ListExpiredUploads(ctx, base.Add(1500*time.Millisecond), 10)
	if err != nil || len(expired) != 2 || expired[0].ID != "c" || expired[1].ID != "a" {
		t.Fatalf("expired uploads = %+v, %v", expired, err)
	}
	if err := store.DeleteUpload(ctx, "team/app", "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upload(ctx, "team/other", "a"); !errors.Is(err, backend.ErrUploadUnknown) {
		t.Fatalf("Upload(wrong repository) error = %v", err)
	}
	expired, _ = store.ListExpiredUploads(ctx, base.Add(time.Hour), 10)
	if len(expired) != 2 {
		t.Fatalf("uploads after delete = %+v", expired)
	}

	desc := oci.Descriptor{MediaType: "application/octet-stream", Digest: ocidigest.FromBytes([]byte("x")), Size: 1}
	reservation, err := store.ReserveContent(ctx, "team/app", desc, base)
	if err != nil {
		t.Fatal(err)
	}
	if reserved, _ := store.Reserved(ctx, desc.Digest); !reserved {
		t.Fatal("digest not reported as reserved")
	}
	reservations, err := store.ListExpiredReservations(ctx, base.Add(time.Second), 10)
	if err != nil || len(reservations) != 1 || reservations[0].ID != reservation.ID {
		t.Fatalf("expired reservations = %+v, %v", reservations, err)
	}
	if err := store.ReleaseReservation(ctx, reservation); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseReservation(ctx, reservation); err != nil {
		t.Fatalf("second release = %v, want idempotent", err)
	}
	candidates, _ := store.GarbageCandidates(ctx, 10)
	if !slices.Contains(candidates, desc.Digest) {
		t.Fatalf("released unreferenced digest not queued: %v", candidates)
	}
}

func TestMetadataNATS(t *testing.T) {
	runSuite(t, func(t *testing.T) kv.Store {
		return open(t, natskv.Config{Servers: []string{kvtest.StartNATS(t)}, Stream: "META"})
	})
}

func TestMetadataEtcd(t *testing.T) {
	runSuite(t, func(t *testing.T) kv.Store {
		return open(t, etcdkv.Config{Endpoints: []string{kvtest.StartEtcd(t)}, Prefix: "/meta"})
	})
}

func open(t *testing.T, cfg kv.Config) kv.Store {
	t.Helper()
	store, err := cfg.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testIndexMissingChildren(t *testing.T, store *kvmeta.Store) {
	ctx := context.Background()
	f := fixture{t, store, ctx}
	layer := f.blob("app", "layer")
	config := f.blob("app", "config")
	amd64 := f.manifest("app", "amd64", backend.ManifestRecord{
		References: []oci.Digest{layer.Digest, config.Digest},
		Config:     &config,
		Layers:     []oci.Descriptor{layer},
	})
	arm64 := oci.Descriptor{MediaType: oci.MediaTypeImageManifest, Digest: ocidigest.FromBytes([]byte("arm64")), Size: 5}
	index := f.manifest("app", "index", backend.ManifestRecord{
		Descriptor: oci.Descriptor{MediaType: oci.MediaTypeImageIndex},
		References: []oci.Digest{amd64.Digest, arm64.Digest},
		Manifests:  []oci.Descriptor{amd64, arm64},
		Tags:       []string{"latest"},
	})
	if resolved, err := store.ResolveTag(ctx, "app", "latest"); err != nil || resolved.Digest != index.Digest {
		t.Fatalf("ResolveTag = %v, %v; want %s", resolved.Digest, err, index.Digest)
	}
	dependents, err := store.Dependents(ctx, arm64.Digest, 10)
	if err != nil || len(dependents) != 1 || dependents[0].Manifest != index.Digest {
		t.Fatalf("missing child dependents = %v, %v; want the index", dependents, err)
	}

	// The missing child can be pushed later and stays protected by the index.
	pushed := f.manifest("app", "arm64", backend.ManifestRecord{
		References: []oci.Digest{layer.Digest, config.Digest},
		Config:     &config,
		Layers:     []oci.Descriptor{layer},
	})
	if pushed.Digest != arm64.Digest {
		t.Fatalf("pushed child digest = %s, want %s", pushed.Digest, arm64.Digest)
	}

	// Only index children are optional: image manifests still need their blobs.
	missing := oci.Descriptor{MediaType: "application/octet-stream", Digest: ocidigest.FromBytes([]byte("missing")), Size: 7}
	desc := oci.Descriptor{MediaType: oci.MediaTypeImageManifest, Digest: ocidigest.FromBytes([]byte("broken")), Size: 6}
	reservation, err := store.ReserveContent(ctx, "app", desc, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.PutManifest(ctx, reservation, backend.ManifestRecord{
		Descriptor: desc,
		References: []oci.Digest{missing.Digest},
		Layers:     []oci.Descriptor{missing},
	})
	if !errors.Is(err, oci.ErrBlobUnknown) {
		t.Fatalf("PutManifest with missing layer error = %v, want ErrBlobUnknown", err)
	}

	result, err := store.DeleteManifest(ctx, "app", index.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(result.GarbageCandidates, arm64.Digest) || slices.Contains(result.GarbageCandidates, amd64.Digest) {
		t.Fatalf("garbage candidates = %v, stored children must stay live", result.GarbageCandidates)
	}
}
