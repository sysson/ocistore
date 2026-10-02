// Package backend defines the storage and metadata contracts used by the
// standalone registry implementation.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/docker/oci"
)

var (
	// ErrUploadUnknown indicates that an upload session does not exist in the
	// requested repository.
	ErrUploadUnknown = errors.New("registry backend: upload unknown")

	// ErrUploadOffset indicates that an upload append used a stale offset.
	ErrUploadOffset = errors.New("registry backend: upload offset mismatch")

	// ErrReservationUnknown indicates that a content reservation does not
	// exist or has already been consumed.
	ErrReservationUnknown = errors.New("registry backend: content reservation unknown")

	// ErrGCLocked indicates that content is claimed by garbage collection and
	// cannot acquire new references until collection completes.
	ErrGCLocked = errors.New("registry backend: content is being garbage collected")
)

// ContentStore stores immutable content by digest and temporary upload data.
//
// Content is global by digest; repository visibility and reachability are
// controlled by MetadataStore. Callers must validate repository names and
// digests before using this interface. Before publishing content with PutBlob
// or CommitUpload, callers must acquire a matching MetadataStore reservation.
// Both methods must verify the supplied digest and size before making content
// visible.
type ContentStore interface {
	// Open returns a reader for the complete immutable object identified by
	// digest. The caller must close the reader.
	Open(ctx context.Context, digest oci.Digest) (io.ReadCloser, error)

	// OpenRange returns bytes in [start, end). A negative end means through
	// EOF. It returns an error for a negative start or an end before start.
	OpenRange(ctx context.Context, digest oci.Digest, start, end int64) (io.ReadCloser, error)

	// Size returns the size of an immutable object.
	Size(ctx context.Context, digest oci.Digest) (int64, error)

	// PutBlob consumes content, verifies both its digest and size, and makes
	// it visible atomically. A failed write must not expose partial content.
	// The caller must hold a matching metadata reservation until it records
	// the repository reference or releases the reservation.
	PutBlob(ctx context.Context, digest oci.Digest, size int64, content io.Reader) error

	// DeleteBlob removes immutable content. It is called only after a
	// successful MetadataStore.ClaimGarbage and must be safe to retry.
	DeleteBlob(ctx context.Context, digest oci.Digest) error

	// StartUpload creates an empty staging area for a globally unique ID.
	// Repeating the call for an existing ID must not reset staged data.
	StartUpload(ctx context.Context, id UploadID) error

	// UploadSize returns the authoritative number of bytes staged for id.
	UploadSize(ctx context.Context, id UploadID) (int64, error)

	// AppendUpload appends content only when expectedOffset equals the current
	// upload size. The offset check and append are serialized. It returns the
	// actual resulting offset, including when reading content fails partway;
	// callers can resume from that offset. An offset mismatch must not mutate
	// staged data and returns ErrUploadOffset.
	AppendUpload(ctx context.Context, id UploadID, expectedOffset int64, content io.Reader) (int64, error)

	// CommitUpload verifies the staged size and digest, then atomically
	// publishes the immutable object. On verification failure, no object is
	// published and the staged upload remains available for cancellation.
	// The caller must hold a matching metadata reservation until it records
	// the repository reference or releases the reservation.
	// On success, the object is visible but the staging area remains until
	// CancelUpload. This lets the caller retry metadata finalization after an
	// interrupted request without retransmitting the upload.
	CommitUpload(ctx context.Context, id UploadID, digest oci.Digest, size int64) error

	// CancelUpload removes staged data. It is safe to retry.
	CancelUpload(ctx context.Context, id UploadID) error
}

// ContentLister optionally lets garbage collection discover objects that were
// published but never recorded in metadata after an interrupted write.
type ContentLister interface {
	ListBlobs(ctx context.Context, visit func(oci.Digest) error) error
}

// UploadID is an opaque, globally unique identifier for a resumable upload.
type UploadID string

// UploadSession binds an upload identifier to exactly one repository.
type UploadSession struct {
	ID         UploadID
	Repository string
	StartedAt  time.Time
}

// ContentReservation protects a digest from garbage collection while bytes
// are being committed and repository metadata is finalized.
type ContentReservation struct {
	ID         string
	Repository string
	Descriptor oci.Descriptor
	ReservedAt time.Time
}

// ManifestRecord contains a manifest descriptor and the structure parsed from
// its content, so metadata queries never need to read manifest bytes.
// References are the manifest's direct blob and child-manifest dependencies
// held by the registry (descriptors with external URLs are excluded), not its
// transitive closure. Dependencies retain descriptors for direct blob
// references, including formats that do not use image layers. Details stores
// parser-specific JSON. Subject is the optional OCI subject relationship used
// to index referrers. On PutManifest, Tags are installed atomically with the
// record; on reads they are the tags currently pointing at the manifest.
type ManifestRecord struct {
	Descriptor   oci.Descriptor
	References   []oci.Digest
	Dependencies []oci.Descriptor
	Subject      *oci.Digest
	ArtifactType string
	Tags         []string

	// Config and Layers are set for image manifests, Manifests for indexes.
	Config      *oci.Descriptor   `json:",omitempty"`
	Layers      []oci.Descriptor  `json:",omitempty"`
	Manifests   []oci.Descriptor  `json:",omitempty"`
	Annotations map[string]string `json:",omitempty"`
	Details     json.RawMessage   `json:",omitempty"`
	PushedAt    time.Time
}

// BlobRecord is a blob present in a repository.
type BlobRecord struct {
	Repository string
	Descriptor oci.Descriptor
	PushedAt   time.Time
}

// TagRecord is a tag and the manifest it currently names.
type TagRecord struct {
	Repository string
	Tag        string
	Digest     oci.Digest
	UpdatedAt  time.Time
}

// Dependent is a manifest that directly references some content.
type Dependent struct {
	Repository string
	Manifest   oci.Digest
}

// ContentKind distinguishes how content is present in a repository.
type ContentKind string

const (
	ContentBlob     ContentKind = "blob"
	ContentManifest ContentKind = "manifest"
)

// Location is a repository in which content is present.
type Location struct {
	Repository string
	Kind       ContentKind
}

// MetadataQuerier exposes the indexes a MetadataStore maintains, for the
// GraphQL layer and image operations. Results are advisory snapshots: they
// never authorize deletion, which garbage collection re-checks itself.
type MetadataQuerier interface {
	// Manifests, Blobs, and TagRecords list a repository's content sorted by
	// digest (or tag) strictly after after, up to a positive limit.
	Manifests(ctx context.Context, repository string, after oci.Digest, limit int) ([]ManifestRecord, error)
	Blobs(ctx context.Context, repository string, after oci.Digest, limit int) ([]BlobRecord, error)
	TagRecords(ctx context.Context, repository, after string, limit int) ([]TagRecord, error)

	// TagsFor returns the tags naming digest in repository, sorted.
	TagsFor(ctx context.Context, repository string, digest oci.Digest) ([]string, error)

	// Dependents returns the manifests, in any repository, that directly
	// reference digest, sorted by repository then manifest digest, up to a
	// positive limit.
	Dependents(ctx context.Context, digest oci.Digest, limit int) ([]Dependent, error)

	// Locations returns the repositories in which digest is present as a
	// blob or manifest.
	Locations(ctx context.Context, digest oci.Digest) ([]Location, error)

	// Reserved reports whether an upload or push currently holds digest.
	Reserved(ctx context.Context, digest oci.Digest) (bool, error)
}

// MutationResult identifies content that may have become unreferenced as a
// result of a metadata mutation. These are hints, not authorization to delete:
// a garbage collector must claim each digest and re-check reachability.
type MutationResult struct {
	GarbageCandidates []oci.Digest
}

// GarbageLease is a durable claim for content that metadata has verified is
// unreachable. The token fences completion to the claim that was granted.
type GarbageLease struct {
	Digest oci.Digest
	Token  string
}

// MetadataStore owns repository membership, manifests, tags, referrer
// indexes, upload sessions, and garbage-collection state.
//
// Every mutation that changes multiple metadata records must be atomic. A
// manifest and all of its tags, dependency edges, and subject/referrer index
// entry become visible together or not at all. Digests are global content
// identities, but blob/manifest membership is repository-scoped. Repository
// names and digests must be validated before calls; implementations must also
// reject invalid input using the corresponding oci.ErrNameInvalid,
// oci.ErrDigestInvalid, or oci.ErrSizeInvalid sentinel.
type MetadataStore interface {
	// EnsureRepository creates an empty repository if it does not exist.
	// It is idempotent and is used when starting an upload.
	EnsureRepository(ctx context.Context, repository string) error

	// Repositories and Tags return up to limit sorted entries strictly after
	// after. The caller must supply a positive limit.
	Repositories(ctx context.Context, after string, limit int) ([]string, error)
	Tags(ctx context.Context, repository, after string, limit int) ([]string, error)

	// Blob returns the descriptor for a blob present in repository.
	Blob(ctx context.Context, repository string, digest oci.Digest) (oci.Descriptor, error)

	// Manifest returns a manifest record present in repository.
	Manifest(ctx context.Context, repository string, digest oci.Digest) (ManifestRecord, error)

	// ResolveTag returns the descriptor currently named by tag in repository.
	ResolveTag(ctx context.Context, repository, tag string) (oci.Descriptor, error)

	// Referrers returns descriptors whose subject is digest, optionally
	// filtered by artifactType. Results are sorted by digest and paginated
	// strictly after after.
	Referrers(ctx context.Context, repository string, digest oci.Digest, artifactType, after string, limit int) ([]oci.Descriptor, error)

	// ReserveContent atomically creates a pending repository-scoped reference
	// and prevents garbage collection from claiming the digest. It must reject
	// an active garbage-collection claim with ErrGCLocked. The reservation is
	// not visible through read methods until consumed by RecordBlob or
	// PutManifest.
	ReserveContent(ctx context.Context, repository string, descriptor oci.Descriptor, reservedAt time.Time) (ContentReservation, error)

	// RecordBlob consumes a reservation and makes a repository-scoped blob
	// reference visible atomically. The content must already be committed to
	// ContentStore, and the reservation must match repository and descriptor.
	RecordBlob(ctx context.Context, reservation ContentReservation) error

	// PutManifest atomically installs a manifest, its direct dependency
	// references, subject/referrer relationship, and tag mappings. Existing
	// tag mappings are replaced in the same transaction. Referenced content
	// must already be committed and available in the repository, except the
	// child manifests of an index, which may be absent (a single-platform pull
	// keeps the full index so its digest matches upstream). It consumes
	// the reservation matching manifest.Descriptor. It returns candidates no
	// longer reachable after the change.
	PutManifest(ctx context.Context, reservation ContentReservation, manifest ManifestRecord) (MutationResult, error)

	// ListExpiredReservations returns up to limit reservations made before
	// cutoff, ordered by reservation time and then ID. A recovery worker may
	// release them after confirming no operation is still finalizing them.
	ListExpiredReservations(ctx context.Context, cutoff time.Time, limit int) ([]ContentReservation, error)

	// ReleaseReservation removes a pending reference. It is idempotent for a
	// reservation already released, but must reject one already consumed.
	ReleaseReservation(ctx context.Context, reservation ContentReservation) error

	// DeleteTag removes only the tag mapping; it does not delete the manifest.
	// It returns candidates that may have become unreachable.
	DeleteTag(ctx context.Context, repository, tag string) (MutationResult, error)

	// DeleteBlob removes the repository's direct blob reference. It must
	// reject removal while the blob is a dependency of any retained manifest
	// in that repository. It returns candidates that may have become
	// unreachable across all repositories.
	DeleteBlob(ctx context.Context, repository string, digest oci.Digest) (MutationResult, error)

	// DeleteManifest removes the manifest, its tag mappings, and its subject
	// index entry from repository metadata. It does not recursively delete
	// descendants; it returns candidates after reachability is recalculated.
	DeleteManifest(ctx context.Context, repository string, digest oci.Digest) (MutationResult, error)

	// CreateUpload registers a new repository-scoped upload session. The
	// repository must exist and the ID must not already be in use.
	CreateUpload(ctx context.Context, session UploadSession) error

	// Upload returns the session only when id belongs to repository.
	Upload(ctx context.Context, repository string, id UploadID) (UploadSession, error)

	// ListExpiredUploads returns up to limit sessions started before cutoff,
	// ordered by start time and then ID.
	ListExpiredUploads(ctx context.Context, cutoff time.Time, limit int) ([]UploadSession, error)

	// DeleteUpload removes a repository-scoped upload session.
	DeleteUpload(ctx context.Context, repository string, id UploadID) error

	// ClaimGarbage atomically rechecks reachability across every repository.
	// If digest is reachable, it returns (nil, false, nil). Otherwise it
	// creates or returns the durable claim and blocks new references to digest
	// until CompleteGarbage succeeds. Pending content reservations also block
	// a claim. Repeated claims must return the same token so interrupted
	// collection can be resumed safely.
	ClaimGarbage(ctx context.Context, digest oci.Digest) (*GarbageLease, bool, error)

	// GarbageCandidates returns queued digests, ordered lexically, that may be
	// eligible for collection. Each candidate must still be passed through
	// ClaimGarbage before bytes are removed.
	GarbageCandidates(ctx context.Context, limit int) ([]oci.Digest, error)

	// CompleteGarbage clears the claim after ContentStore.DeleteBlob succeeds.
	// It must reject a lease with a token that does not match the active claim.
	// It also removes the digest from the candidate queue.
	CompleteGarbage(ctx context.Context, lease GarbageLease) error
}
