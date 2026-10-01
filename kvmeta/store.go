// Package kvmeta implements backend.MetadataStore and backend.MetadataQuerier
// over any kv.Store. Every relationship the GraphQL layer and garbage
// collector query is materialized as an ordered index key, written in the
// same transaction as the record it describes:
//
//	repo/<repo>                                  repository record
//	tag/<repo>/<tag>                             tag -> manifest digest
//	tagref/<repo>/<digest>/<tag>                 manifest -> tags
//	manifest/<repo>/<digest>                     parsed manifest record
//	blob/<repo>/<digest>                         blob record
//	dependent/<digest>/<repo>/<manifest>         content -> manifests referencing it
//	location/<digest>/<repo>/<kind>              content -> repositories holding it
//	referrer/<repo>/<subject>/<digest>           subject -> referrer descriptor
//	reservation/<id>, reservation-digest/<digest>/<id>, reservation-time/<ts>/<id>
//	upload/<id>, upload-time/<ts>/<id>
//	pending/<repo>/<kind>/<id>                   repository -> open uploads and reservations
//	gc-queue/<digest>, gc-claim/<digest>
//
// Parts are joined with kv.Separator, so each index is a part-aligned prefix
// scan on every driver.
package kvmeta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/docker/oci"
	"github.com/docker/oci/ociref"
	"github.com/sysson/ocistore/backend"
	"github.com/sysson/ocistore/kv"
)

const (
	nsRepo              = "repo"
	nsTag               = "tag"
	nsTagRef            = "tagref"
	nsManifest          = "manifest"
	nsBlob              = "blob"
	nsDependent         = "dependent"
	nsLocation          = "location"
	nsReferrer          = "referrer"
	nsReservation       = "reservation"
	nsReservationDigest = "reservation-digest"
	nsReservationTime   = "reservation-time"
	nsUpload            = "upload"
	nsUploadTime        = "upload-time"
	nsPending           = "pending"
	nsGCQueue           = "gc-queue"
	nsGCClaim           = "gc-claim"
	nsSchema            = "schema"

	pendingUpload      = "upload"
	pendingReservation = "reservation"

	schemaVersion = "1"
)

// Store is the metadata store over a kv.Store.
type Store struct {
	kv  kv.Store
	now func() time.Time
}

var (
	_ backend.MetadataStore   = (*Store)(nil)
	_ backend.MetadataQuerier = (*Store)(nil)
)

var uploadIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// New returns a metadata store over store, initializing or checking the
// schema version.
func New(ctx context.Context, store kv.Store) (*Store, error) {
	s := &Store{kv: store, now: time.Now}
	err := store.Update(ctx, func(tx kv.Txn) error {
		value, err := tx.Get(ctx, kv.Key(nsSchema))
		if errors.Is(err, kv.ErrNotFound) {
			return tx.Put(kv.Key(nsSchema), []byte(schemaVersion))
		}
		if err != nil {
			return err
		}
		if string(value) != schemaVersion {
			return fmt.Errorf("unsupported registry metadata schema %q (want %s)", value, schemaVersion)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("initializing registry metadata: %w", err)
	}
	return s, nil
}

// Open opens the kv driver cfg selects and returns a metadata store over it.
func Open(ctx context.Context, cfg kv.Config) (*Store, error) {
	store, err := cfg.Open(ctx)
	if err != nil {
		return nil, err
	}
	s, err := New(ctx, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.kv.Close() }

type repoRecord struct {
	CreatedAt time.Time
}

type tagRecord struct {
	Digest    oci.Digest
	UpdatedAt time.Time
}

type blobRecord struct {
	Descriptor oci.Descriptor
	PushedAt   time.Time
}

func (s *Store) EnsureRepository(ctx context.Context, name string) error {
	return s.kv.Update(ctx, func(tx kv.Txn) error {
		return s.ensureRepository(ctx, tx, name)
	})
}

func (s *Store) ensureRepository(ctx context.Context, tx kv.Txn, name string) error {
	if !ociref.IsValidRepository(name) {
		return oci.ErrNameInvalid
	}
	if _, err := tx.Get(ctx, kv.Key(nsRepo, name)); err == nil {
		return nil
	} else if !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	return putJSON(tx, kv.Key(nsRepo, name), repoRecord{CreatedAt: s.now()})
}

func (s *Store) Repositories(ctx context.Context, after string, limit int) ([]string, error) {
	if limit < 1 {
		return nil, errors.New("repository page limit must be positive")
	}
	var result []string
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		return r.Scan(ctx, kv.Prefix(nsRepo), afterKey(after, nsRepo), func(key string, _ []byte) (bool, error) {
			result = append(result, kv.Split(key)[1])
			return len(result) < limit, nil
		})
	})
	return result, err
}

func (s *Store) Tags(ctx context.Context, name, after string, limit int) ([]string, error) {
	records, err := s.TagRecords(ctx, name, after, limit)
	if err != nil {
		return nil, err
	}
	tags := make([]string, len(records))
	for i, record := range records {
		tags[i] = record.Tag
	}
	return tags, nil
}

func (s *Store) TagRecords(ctx context.Context, name, after string, limit int) ([]backend.TagRecord, error) {
	if limit < 1 {
		return nil, errors.New("tag page limit must be positive")
	}
	var result []backend.TagRecord
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		return r.Scan(ctx, kv.Prefix(nsTag, name), afterKey(after, nsTag, name), func(key string, value []byte) (bool, error) {
			var record tagRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return false, err
			}
			result = append(result, backend.TagRecord{Repository: name, Tag: kv.Split(key)[2], Digest: record.Digest, UpdatedAt: record.UpdatedAt})
			return len(result) < limit, nil
		})
	})
	return result, err
}

func (s *Store) Blob(ctx context.Context, name string, digest oci.Digest) (oci.Descriptor, error) {
	var result oci.Descriptor
	err := s.kv.View(ctx, func(r kv.Reader) error {
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		var record blobRecord
		if err := getJSON(ctx, r, kv.Key(nsBlob, name, string(digest)), &record); err != nil {
			return notFound(err, oci.ErrBlobUnknown)
		}
		result = record.Descriptor
		return nil
	})
	return result, err
}

func (s *Store) Blobs(ctx context.Context, name string, after oci.Digest, limit int) ([]backend.BlobRecord, error) {
	if limit < 1 {
		return nil, errors.New("blob page limit must be positive")
	}
	var result []backend.BlobRecord
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		return r.Scan(ctx, kv.Prefix(nsBlob, name), afterKey(string(after), nsBlob, name), func(_ string, value []byte) (bool, error) {
			var record blobRecord
			if err := json.Unmarshal(value, &record); err != nil {
				return false, err
			}
			result = append(result, backend.BlobRecord{Repository: name, Descriptor: record.Descriptor, PushedAt: record.PushedAt})
			return len(result) < limit, nil
		})
	})
	return result, err
}

func (s *Store) Manifest(ctx context.Context, name string, digest oci.Digest) (backend.ManifestRecord, error) {
	var result backend.ManifestRecord
	err := s.kv.View(ctx, func(r kv.Reader) error {
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		record, err := readManifest(ctx, r, name, digest)
		if err != nil {
			return err
		}
		result = record
		return nil
	})
	return result, err
}

func (s *Store) Manifests(ctx context.Context, name string, after oci.Digest, limit int) ([]backend.ManifestRecord, error) {
	if limit < 1 {
		return nil, errors.New("manifest page limit must be positive")
	}
	var result []backend.ManifestRecord
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		var digests []oci.Digest
		if err := r.Scan(ctx, kv.Prefix(nsManifest, name), afterKey(string(after), nsManifest, name), func(key string, _ []byte) (bool, error) {
			digests = append(digests, oci.Digest(kv.Split(key)[2]))
			return len(digests) < limit, nil
		}); err != nil {
			return err
		}
		for _, digest := range digests {
			record, err := readManifest(ctx, r, name, digest)
			if err != nil {
				return err
			}
			result = append(result, record)
		}
		return nil
	})
	return result, err
}

func (s *Store) TagsFor(ctx context.Context, name string, digest oci.Digest) ([]string, error) {
	var result []string
	err := s.kv.View(ctx, func(r kv.Reader) error {
		var err error
		result, err = tagsFor(ctx, r, name, digest)
		return err
	})
	return result, err
}

func (s *Store) ResolveTag(ctx context.Context, name, tag string) (oci.Descriptor, error) {
	var result oci.Descriptor
	err := s.kv.View(ctx, func(r kv.Reader) error {
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		var record tagRecord
		if err := getJSON(ctx, r, kv.Key(nsTag, name, tag), &record); err != nil {
			return notFound(err, oci.ErrManifestUnknown)
		}
		var manifest backend.ManifestRecord
		if err := getJSON(ctx, r, kv.Key(nsManifest, name, string(record.Digest)), &manifest); err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				return fmt.Errorf("tag %q references absent manifest %s", tag, record.Digest)
			}
			return err
		}
		result = manifest.Descriptor
		return nil
	})
	return result, err
}

func (s *Store) Referrers(ctx context.Context, name string, digest oci.Digest, artifactType, after string, limit int) ([]oci.Descriptor, error) {
	if limit < 1 {
		return nil, errors.New("referrer page limit must be positive")
	}
	var result []oci.Descriptor
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		if err := requireRepository(ctx, r, name); err != nil {
			return err
		}
		return r.Scan(ctx, kv.Prefix(nsReferrer, name, string(digest)), afterKey(after, nsReferrer, name, string(digest)), func(_ string, value []byte) (bool, error) {
			var desc oci.Descriptor
			if err := json.Unmarshal(value, &desc); err != nil {
				return false, err
			}
			if artifactType == "" || desc.ArtifactType == artifactType {
				result = append(result, desc)
			}
			return len(result) < limit, nil
		})
	})
	return result, err
}

func (s *Store) Dependents(ctx context.Context, digest oci.Digest, limit int) ([]backend.Dependent, error) {
	if limit < 1 {
		return nil, errors.New("dependent page limit must be positive")
	}
	var result []backend.Dependent
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		return r.Scan(ctx, kv.Prefix(nsDependent, string(digest)), "", func(key string, _ []byte) (bool, error) {
			parts := kv.Split(key)
			result = append(result, backend.Dependent{Repository: parts[2], Manifest: oci.Digest(parts[3])})
			return len(result) < limit, nil
		})
	})
	return result, err
}

func (s *Store) Locations(ctx context.Context, digest oci.Digest) ([]backend.Location, error) {
	var result []backend.Location
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		return r.Scan(ctx, kv.Prefix(nsLocation, string(digest)), "", func(key string, _ []byte) (bool, error) {
			parts := kv.Split(key)
			result = append(result, backend.Location{Repository: parts[2], Kind: backend.ContentKind(parts[3])})
			return true, nil
		})
	})
	return result, err
}

func (s *Store) Reserved(ctx context.Context, digest oci.Digest) (bool, error) {
	var reserved bool
	err := s.kv.View(ctx, func(r kv.Reader) error {
		var err error
		reserved, err = exists(ctx, r, kv.Prefix(nsReservationDigest, string(digest)))
		return err
	})
	return reserved, err
}

func (s *Store) ReserveContent(ctx context.Context, name string, desc oci.Descriptor, reservedAt time.Time) (backend.ContentReservation, error) {
	if !ociref.IsValidRepository(name) {
		return backend.ContentReservation{}, oci.ErrNameInvalid
	}
	if err := validateDescriptor(desc); err != nil {
		return backend.ContentReservation{}, err
	}
	id, err := newID()
	if err != nil {
		return backend.ContentReservation{}, err
	}
	reservation := backend.ContentReservation{ID: id, Repository: name, Descriptor: desc, ReservedAt: reservedAt}
	err = s.kv.Update(ctx, func(tx kv.Txn) error {
		if err := s.ensureRepository(ctx, tx, name); err != nil {
			return err
		}
		if _, err := tx.Get(ctx, kv.Key(nsGCClaim, string(desc.Digest))); err == nil {
			return backend.ErrGCLocked
		} else if !errors.Is(err, kv.ErrNotFound) {
			return err
		}
		if err := putJSON(tx, kv.Key(nsReservation, id), reservation); err != nil {
			return err
		}
		if err := tx.Put(kv.Key(nsReservationDigest, string(desc.Digest), id), nil); err != nil {
			return err
		}
		if err := tx.Put(kv.Key(nsPending, name, pendingReservation, id), nil); err != nil {
			return err
		}
		return tx.Put(kv.Key(nsReservationTime, timeKey(reservedAt), id), nil)
	})
	return reservation, err
}

func (s *Store) RecordBlob(ctx context.Context, reservation backend.ContentReservation) error {
	now := s.now()
	return s.kv.Update(ctx, func(tx kv.Txn) error {
		reserved, err := takeReservation(ctx, tx, reservation)
		if err != nil {
			return err
		}
		if err := requireRepository(ctx, tx, reserved.Repository); err != nil {
			return err
		}
		digest := string(reserved.Descriptor.Digest)
		record := blobRecord{Descriptor: reserved.Descriptor, PushedAt: now}
		var existing blobRecord
		if err := getJSON(ctx, tx, kv.Key(nsBlob, reserved.Repository, digest), &existing); err == nil {
			record.PushedAt = existing.PushedAt
		} else if !errors.Is(err, kv.ErrNotFound) {
			return err
		}
		if err := putJSON(tx, kv.Key(nsBlob, reserved.Repository, digest), record); err != nil {
			return err
		}
		return tx.Put(kv.Key(nsLocation, digest, reserved.Repository, string(backend.ContentBlob)), nil)
	})
}

func (s *Store) PutManifest(ctx context.Context, reservation backend.ContentReservation, record backend.ManifestRecord) (backend.MutationResult, error) {
	for _, tag := range record.Tags {
		if !ociref.IsValidTag(tag) {
			return backend.MutationResult{}, fmt.Errorf("invalid tag %q: %w", tag, oci.ErrNameInvalid)
		}
	}
	for _, reference := range record.References {
		if err := reference.Validate(); err != nil {
			return backend.MutationResult{}, fmt.Errorf("invalid manifest dependency: %w: %v", oci.ErrDigestInvalid, err)
		}
	}
	if record.Subject != nil {
		if err := record.Subject.Validate(); err != nil {
			return backend.MutationResult{}, fmt.Errorf("invalid manifest subject: %w: %v", oci.ErrDigestInvalid, err)
		}
	}
	now := s.now()
	result := backend.MutationResult{GarbageCandidates: []oci.Digest{}}
	err := s.kv.Update(ctx, func(tx kv.Txn) error {
		result.GarbageCandidates = []oci.Digest{}
		reserved, err := takeReservation(ctx, tx, reservation)
		if err != nil {
			return err
		}
		if !sameContentDescriptor(reserved.Descriptor, record.Descriptor) {
			return backend.ErrReservationUnknown
		}
		repo := reserved.Repository
		if err := requireRepository(ctx, tx, repo); err != nil {
			return err
		}
		for _, reference := range record.References {
			ok, err := present(ctx, tx, repo, reference)
			if err != nil {
				return err
			}
			if !ok && !optionalReference(record, reference) {
				return fmt.Errorf("manifest dependency %s: %w", reference, oci.ErrBlobUnknown)
			}
		}

		digest := string(record.Descriptor.Digest)
		stored := cloneRecord(record)
		stored.Tags = nil
		stored.PushedAt = now
		var existing backend.ManifestRecord
		if err := getJSON(ctx, tx, kv.Key(nsManifest, repo, digest), &existing); err == nil {
			stored.PushedAt = existing.PushedAt
		} else if !errors.Is(err, kv.ErrNotFound) {
			return err
		}
		if err := putJSON(tx, kv.Key(nsManifest, repo, digest), stored); err != nil {
			return err
		}
		if err := tx.Put(kv.Key(nsLocation, digest, repo, string(backend.ContentManifest)), nil); err != nil {
			return err
		}
		for _, reference := range record.References {
			if err := tx.Put(kv.Key(nsDependent, string(reference), repo, digest), nil); err != nil {
				return err
			}
		}
		if record.Subject != nil {
			referrer := oci.Descriptor{
				MediaType:    record.Descriptor.MediaType,
				Digest:       record.Descriptor.Digest,
				Size:         record.Descriptor.Size,
				ArtifactType: record.ArtifactType,
				Annotations:  record.Annotations,
			}
			if err := putJSON(tx, kv.Key(nsReferrer, repo, string(*record.Subject), digest), referrer); err != nil {
				return err
			}
		}
		for _, tag := range record.Tags {
			if err := setTag(ctx, tx, repo, tag, record.Descriptor.Digest, now); err != nil {
				return err
			}
		}
		return nil
	})
	return result, err
}

// setTag points tag at digest, replacing any previous mapping and its
// reverse index entry.
func setTag(ctx context.Context, tx kv.Txn, repo, tag string, digest oci.Digest, now time.Time) error {
	var previous tagRecord
	if err := getJSON(ctx, tx, kv.Key(nsTag, repo, tag), &previous); err == nil {
		if previous.Digest == digest {
			return nil
		}
		if err := tx.Delete(kv.Key(nsTagRef, repo, string(previous.Digest), tag)); err != nil {
			return err
		}
	} else if !errors.Is(err, kv.ErrNotFound) {
		return err
	}
	if err := putJSON(tx, kv.Key(nsTag, repo, tag), tagRecord{Digest: digest, UpdatedAt: now}); err != nil {
		return err
	}
	return tx.Put(kv.Key(nsTagRef, repo, string(digest), tag), nil)
}

func (s *Store) ListExpiredReservations(ctx context.Context, cutoff time.Time, limit int) ([]backend.ContentReservation, error) {
	if limit < 1 {
		return nil, errors.New("reservation page limit must be positive")
	}
	var result []backend.ContentReservation
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		var ids []string
		cutoffKey := timeKey(cutoff)
		if err := r.Scan(ctx, kv.Prefix(nsReservationTime), "", func(key string, _ []byte) (bool, error) {
			parts := kv.Split(key)
			if parts[1] >= cutoffKey {
				return false, nil
			}
			ids = append(ids, parts[2])
			return len(ids) < limit, nil
		}); err != nil {
			return err
		}
		for _, id := range ids {
			var reservation backend.ContentReservation
			if err := getJSON(ctx, r, kv.Key(nsReservation, id), &reservation); err != nil {
				return err
			}
			result = append(result, reservation)
		}
		return nil
	})
	return result, err
}

func (s *Store) ReleaseReservation(ctx context.Context, reservation backend.ContentReservation) error {
	return s.kv.Update(ctx, func(tx kv.Txn) error {
		if _, err := tx.Get(ctx, kv.Key(nsReservation, reservation.ID)); errors.Is(err, kv.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		reserved, err := takeReservation(ctx, tx, reservation)
		if err != nil {
			return err
		}
		if _, err := enqueueIfUnreachable(ctx, tx, reserved.Descriptor.Digest); err != nil {
			return err
		}
		return s.pruneRepositoryIfEmpty(ctx, tx, reserved.Repository)
	})
}

func (s *Store) DeleteTag(ctx context.Context, name, tag string) (backend.MutationResult, error) {
	err := s.kv.Update(ctx, func(tx kv.Txn) error {
		if err := requireRepository(ctx, tx, name); err != nil {
			return err
		}
		var record tagRecord
		if err := getJSON(ctx, tx, kv.Key(nsTag, name, tag), &record); err != nil {
			return notFound(err, oci.ErrManifestUnknown)
		}
		if err := tx.Delete(kv.Key(nsTag, name, tag)); err != nil {
			return err
		}
		return tx.Delete(kv.Key(nsTagRef, name, string(record.Digest), tag))
	})
	return backend.MutationResult{}, err
}

func (s *Store) DeleteBlob(ctx context.Context, name string, digest oci.Digest) (backend.MutationResult, error) {
	var result backend.MutationResult
	err := s.kv.Update(ctx, func(tx kv.Txn) error {
		result = backend.MutationResult{}
		if err := requireRepository(ctx, tx, name); err != nil {
			return err
		}
		if _, err := tx.Get(ctx, kv.Key(nsBlob, name, string(digest))); err != nil {
			return notFound(err, oci.ErrBlobUnknown)
		}
		referenced, err := exists(ctx, tx, kv.Prefix(nsDependent, string(digest), name))
		if err != nil {
			return err
		}
		if referenced {
			return oci.ErrReferenced
		}
		if err := tx.Delete(kv.Key(nsBlob, name, string(digest))); err != nil {
			return err
		}
		if err := tx.Delete(kv.Key(nsLocation, string(digest), name, string(backend.ContentBlob))); err != nil {
			return err
		}
		queued, err := enqueueIfUnreachable(ctx, tx, digest)
		if queued {
			result.GarbageCandidates = append(result.GarbageCandidates, digest)
		}
		if err != nil {
			return err
		}
		return s.pruneRepositoryIfEmpty(ctx, tx, name)
	})
	return result, err
}

func (s *Store) DeleteManifest(ctx context.Context, name string, digest oci.Digest) (backend.MutationResult, error) {
	var result backend.MutationResult
	err := s.kv.Update(ctx, func(tx kv.Txn) error {
		result = backend.MutationResult{}
		if err := requireRepository(ctx, tx, name); err != nil {
			return err
		}
		var record backend.ManifestRecord
		if err := getJSON(ctx, tx, kv.Key(nsManifest, name, string(digest)), &record); err != nil {
			return notFound(err, oci.ErrManifestUnknown)
		}
		tags, err := tagsFor(ctx, tx, name, digest)
		if err != nil {
			return err
		}
		for _, tag := range tags {
			if err := tx.Delete(kv.Key(nsTag, name, tag)); err != nil {
				return err
			}
			if err := tx.Delete(kv.Key(nsTagRef, name, string(digest), tag)); err != nil {
				return err
			}
		}
		if err := tx.Delete(kv.Key(nsManifest, name, string(digest))); err != nil {
			return err
		}
		if err := tx.Delete(kv.Key(nsLocation, string(digest), name, string(backend.ContentManifest))); err != nil {
			return err
		}
		if record.Subject != nil {
			if err := tx.Delete(kv.Key(nsReferrer, name, string(*record.Subject), string(digest))); err != nil {
				return err
			}
		}
		for _, reference := range record.References {
			if err := tx.Delete(kv.Key(nsDependent, string(reference), name, string(digest))); err != nil {
				return err
			}
		}
		for _, candidate := range append([]oci.Digest{digest}, record.References...) {
			queued, err := enqueueIfUnreachable(ctx, tx, candidate)
			if err != nil {
				return err
			}
			if queued && !slices.Contains(result.GarbageCandidates, candidate) {
				result.GarbageCandidates = append(result.GarbageCandidates, candidate)
			}
		}
		return s.pruneRepositoryIfEmpty(ctx, tx, name)
	})
	return result, err
}

func (s *Store) CreateUpload(ctx context.Context, session backend.UploadSession) error {
	if !validUploadID(session.ID) || !ociref.IsValidRepository(session.Repository) {
		return oci.ErrNameInvalid
	}
	return s.kv.Update(ctx, func(tx kv.Txn) error {
		if err := s.ensureRepository(ctx, tx, session.Repository); err != nil {
			return err
		}
		if _, err := tx.Get(ctx, kv.Key(nsUpload, string(session.ID))); err == nil {
			return errors.New("upload ID already exists")
		} else if !errors.Is(err, kv.ErrNotFound) {
			return err
		}
		if err := putJSON(tx, kv.Key(nsUpload, string(session.ID)), session); err != nil {
			return err
		}
		if err := tx.Put(kv.Key(nsPending, session.Repository, pendingUpload, string(session.ID)), nil); err != nil {
			return err
		}
		return tx.Put(kv.Key(nsUploadTime, timeKey(session.StartedAt), string(session.ID)), nil)
	})
}

func (s *Store) Upload(ctx context.Context, name string, id backend.UploadID) (backend.UploadSession, error) {
	var result backend.UploadSession
	err := s.kv.View(ctx, func(r kv.Reader) error {
		session, err := readUpload(ctx, r, name, id)
		result = session
		return err
	})
	return result, err
}

func (s *Store) ListExpiredUploads(ctx context.Context, cutoff time.Time, limit int) ([]backend.UploadSession, error) {
	if limit < 1 {
		return nil, errors.New("upload page limit must be positive")
	}
	var result []backend.UploadSession
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		var ids []string
		cutoffKey := timeKey(cutoff)
		if err := r.Scan(ctx, kv.Prefix(nsUploadTime), "", func(key string, _ []byte) (bool, error) {
			parts := kv.Split(key)
			if parts[1] >= cutoffKey {
				return false, nil
			}
			ids = append(ids, parts[2])
			return len(ids) < limit, nil
		}); err != nil {
			return err
		}
		for _, id := range ids {
			var session backend.UploadSession
			if err := getJSON(ctx, r, kv.Key(nsUpload, id), &session); err != nil {
				return err
			}
			result = append(result, session)
		}
		return nil
	})
	return result, err
}

func (s *Store) DeleteUpload(ctx context.Context, name string, id backend.UploadID) error {
	return s.kv.Update(ctx, func(tx kv.Txn) error {
		session, err := readUpload(ctx, tx, name, id)
		if err != nil {
			return err
		}
		if err := tx.Delete(kv.Key(nsUpload, string(id))); err != nil {
			return err
		}
		if err := tx.Delete(kv.Key(nsUploadTime, timeKey(session.StartedAt), string(id))); err != nil {
			return err
		}
		if err := tx.Delete(kv.Key(nsPending, session.Repository, pendingUpload, string(id))); err != nil {
			return err
		}
		return s.pruneRepositoryIfEmpty(ctx, tx, name)
	})
}

func (s *Store) ClaimGarbage(ctx context.Context, digest oci.Digest) (*backend.GarbageLease, bool, error) {
	token, err := newID()
	if err != nil {
		return nil, false, err
	}
	var lease *backend.GarbageLease
	err = s.kv.Update(ctx, func(tx kv.Txn) error {
		lease = nil
		live, err := reachable(ctx, tx, digest)
		if err != nil {
			return err
		}
		if live {
			return tx.Delete(kv.Key(nsGCQueue, string(digest)))
		}
		existing, err := tx.Get(ctx, kv.Key(nsGCClaim, string(digest)))
		if err == nil {
			lease = &backend.GarbageLease{Digest: digest, Token: string(existing)}
			return nil
		}
		if !errors.Is(err, kv.ErrNotFound) {
			return err
		}
		lease = &backend.GarbageLease{Digest: digest, Token: token}
		if err := tx.Put(kv.Key(nsGCQueue, string(digest)), nil); err != nil {
			return err
		}
		return tx.Put(kv.Key(nsGCClaim, string(digest)), []byte(token))
	})
	if err != nil {
		return nil, false, err
	}
	return lease, lease != nil, nil
}

func (s *Store) GarbageCandidates(ctx context.Context, limit int) ([]oci.Digest, error) {
	if limit < 1 {
		return nil, errors.New("garbage candidate limit must be positive")
	}
	var result []oci.Digest
	err := s.kv.View(ctx, func(r kv.Reader) error {
		result = nil
		return r.Scan(ctx, kv.Prefix(nsGCQueue), "", func(key string, _ []byte) (bool, error) {
			result = append(result, oci.Digest(kv.Split(key)[1]))
			return len(result) < limit, nil
		})
	})
	return result, err
}

func (s *Store) CompleteGarbage(ctx context.Context, lease backend.GarbageLease) error {
	return s.kv.Update(ctx, func(tx kv.Txn) error {
		token, err := tx.Get(ctx, kv.Key(nsGCClaim, string(lease.Digest)))
		if err != nil || string(token) != lease.Token {
			if err != nil && !errors.Is(err, kv.ErrNotFound) {
				return err
			}
			return errors.New("garbage lease token mismatch")
		}
		if err := tx.Delete(kv.Key(nsGCClaim, string(lease.Digest))); err != nil {
			return err
		}
		return tx.Delete(kv.Key(nsGCQueue, string(lease.Digest)))
	})
}

func requireRepository(ctx context.Context, r kv.Reader, name string) error {
	if !ociref.IsValidRepository(name) {
		return oci.ErrNameInvalid
	}
	if _, err := r.Get(ctx, kv.Key(nsRepo, name)); err != nil {
		return notFound(err, oci.ErrNameUnknown)
	}
	return nil
}

func (s *Store) pruneRepositoryIfEmpty(ctx context.Context, tx kv.Txn, name string) error {
	repositoryKey := kv.Key(nsRepo, name)
	if _, err := tx.Get(ctx, repositoryKey); errors.Is(err, kv.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	for _, namespace := range []string{nsTag, nsTagRef, nsManifest, nsBlob, nsReferrer, nsPending} {
		found, err := exists(ctx, tx, kv.Prefix(namespace, name))
		if err != nil {
			return err
		}
		if found {
			return nil
		}
	}
	return tx.Delete(repositoryKey)
}

func readManifest(ctx context.Context, r kv.Reader, name string, digest oci.Digest) (backend.ManifestRecord, error) {
	var record backend.ManifestRecord
	if err := getJSON(ctx, r, kv.Key(nsManifest, name, string(digest)), &record); err != nil {
		return backend.ManifestRecord{}, notFound(err, oci.ErrManifestUnknown)
	}
	tags, err := tagsFor(ctx, r, name, digest)
	if err != nil {
		return backend.ManifestRecord{}, err
	}
	record.Tags = tags
	return record, nil
}

func tagsFor(ctx context.Context, r kv.Reader, name string, digest oci.Digest) ([]string, error) {
	tags := []string{}
	err := r.Scan(ctx, kv.Prefix(nsTagRef, name, string(digest)), "", func(key string, _ []byte) (bool, error) {
		tags = append(tags, kv.Split(key)[3])
		return true, nil
	})
	return tags, err
}

func readUpload(ctx context.Context, r kv.Reader, name string, id backend.UploadID) (backend.UploadSession, error) {
	if !validUploadID(id) {
		return backend.UploadSession{}, backend.ErrUploadUnknown
	}
	var session backend.UploadSession
	if err := getJSON(ctx, r, kv.Key(nsUpload, string(id)), &session); err != nil {
		return backend.UploadSession{}, notFound(err, backend.ErrUploadUnknown)
	}
	if session.Repository != name {
		return backend.UploadSession{}, backend.ErrUploadUnknown
	}
	return session, nil
}

func takeReservation(ctx context.Context, tx kv.Txn, reservation backend.ContentReservation) (backend.ContentReservation, error) {
	if reservation.ID == "" || !kv.ValidPart(reservation.ID) {
		return backend.ContentReservation{}, backend.ErrReservationUnknown
	}
	var reserved backend.ContentReservation
	if err := getJSON(ctx, tx, kv.Key(nsReservation, reservation.ID), &reserved); err != nil {
		return backend.ContentReservation{}, notFound(err, backend.ErrReservationUnknown)
	}
	if reserved.Repository != reservation.Repository || !sameContentDescriptor(reserved.Descriptor, reservation.Descriptor) {
		return backend.ContentReservation{}, backend.ErrReservationUnknown
	}
	for _, key := range []string{
		kv.Key(nsReservation, reserved.ID),
		kv.Key(nsReservationDigest, string(reserved.Descriptor.Digest), reserved.ID),
		kv.Key(nsReservationTime, timeKey(reserved.ReservedAt), reserved.ID),
		kv.Key(nsPending, reserved.Repository, pendingReservation, reserved.ID),
	} {
		if err := tx.Delete(key); err != nil {
			return backend.ContentReservation{}, err
		}
	}
	return reserved, nil
}

// present reports whether digest is a blob or manifest of repo.
func present(ctx context.Context, r kv.Reader, repo string, digest oci.Digest) (bool, error) {
	for _, kind := range []backend.ContentKind{backend.ContentBlob, backend.ContentManifest} {
		if _, err := r.Get(ctx, kv.Key(nsLocation, string(digest), repo, string(kind))); err == nil {
			return true, nil
		} else if !errors.Is(err, kv.ErrNotFound) {
			return false, err
		}
	}
	return false, nil
}

// optionalReference reports whether reference is a child manifest of an
// index. Index children may be absent: a single-platform pull stores the
// index unchanged, so its digest matches upstream, without the manifests for
// other platforms. The dependent entry is still written, so a child pushed
// later is protected by the index.
func optionalReference(record backend.ManifestRecord, reference oci.Digest) bool {
	switch record.Descriptor.MediaType {
	case oci.MediaTypeImageIndex, oci.MediaTypeDockerManifestList:
	default:
		return false
	}
	return slices.ContainsFunc(record.Manifests, func(child oci.Descriptor) bool {
		return child.Digest == reference
	})
}

// reachable reports whether any repository holds or references digest, or a
// pending reservation protects it.
func reachable(ctx context.Context, r kv.Reader, digest oci.Digest) (bool, error) {
	for _, prefix := range []string{
		kv.Prefix(nsLocation, string(digest)),
		kv.Prefix(nsDependent, string(digest)),
		kv.Prefix(nsReservationDigest, string(digest)),
	} {
		found, err := exists(ctx, r, prefix)
		if err != nil || found {
			return found, err
		}
	}
	return false, nil
}

func enqueueIfUnreachable(ctx context.Context, tx kv.Txn, digest oci.Digest) (bool, error) {
	live, err := reachable(ctx, tx, digest)
	if err != nil || live {
		return false, err
	}
	return true, tx.Put(kv.Key(nsGCQueue, string(digest)), nil)
}

func exists(ctx context.Context, r kv.Reader, prefix string) (bool, error) {
	found := false
	err := r.Scan(ctx, prefix, "", func(string, []byte) (bool, error) {
		found = true
		return false, nil
	})
	return found, err
}

func afterKey(after string, parts ...string) string {
	if after == "" {
		return ""
	}
	return kv.Key(append(parts, after)...)
}

// timeKey encodes t so that byte order matches time order.
func timeKey(t time.Time) string {
	nanos := max(t.UnixNano(), 0)
	return fmt.Sprintf("%020d", nanos)
}

func getJSON(ctx context.Context, r kv.Reader, key string, into any) error {
	value, err := r.Get(ctx, key)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(value, into); err != nil {
		return fmt.Errorf("decoding registry metadata %q: %w", kv.Split(key)[0], err)
	}
	return nil
}

func putJSON(tx kv.Txn, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encoding registry metadata: %w", err)
	}
	return tx.Put(key, data)
}

func notFound(err, sentinel error) error {
	if errors.Is(err, kv.ErrNotFound) {
		return sentinel
	}
	return err
}

func validateDescriptor(desc oci.Descriptor) error {
	if err := desc.Digest.Validate(); err != nil {
		return fmt.Errorf("invalid digest: %w: %v", oci.ErrDigestInvalid, err)
	}
	if desc.Size < 0 {
		return oci.ErrSizeInvalid
	}
	return nil
}

func sameContentDescriptor(a, b oci.Descriptor) bool {
	return a.Digest == b.Digest && a.Size == b.Size && a.MediaType == b.MediaType
}

func cloneRecord(record backend.ManifestRecord) backend.ManifestRecord {
	record.References = slices.Clone(record.References)
	record.Tags = slices.Clone(record.Tags)
	record.Layers = slices.Clone(record.Layers)
	record.Manifests = slices.Clone(record.Manifests)
	if record.Subject != nil {
		subject := *record.Subject
		record.Subject = &subject
	}
	if record.Config != nil {
		config := *record.Config
		record.Config = &config
	}
	return record
}

func validUploadID(id backend.UploadID) bool {
	return uploadIDPattern.MatchString(string(id))
}

func newID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generating metadata identifier: %w", err)
	}
	return hex.EncodeToString(data), nil
}
