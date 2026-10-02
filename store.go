package ocistore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/docker/oci"
	"github.com/docker/oci/ocidigest"
	"github.com/docker/oci/ociref"
	"github.com/sysson/ocistore/backend"
	"github.com/sysson/ocistore/blobstore"
)

type Store struct {
	*oci.Funcs
	content                      backend.ContentStore
	metadata                     Metadata
	manifestParsers              []ManifestParser
	allowMissingManifestChildren bool
}

var _ oci.Interface = (*Store)(nil)

func New(content backend.ContentStore, metadata Metadata, options ...Option) (*Store, error) {
	if content == nil {
		return nil, errors.New("registry content store is required")
	}
	if metadata == nil {
		return nil, errors.New("registry metadata store is required")
	}
	configured := storeOptions{}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("registry option is required")
		}
		if err := option(&configured); err != nil {
			return nil, err
		}
	}
	r := &Store{
		content:                      content,
		metadata:                     metadata,
		manifestParsers:              configured.manifestParsers,
		allowMissingManifestChildren: configured.allowMissingManifestChildren,
	}
	r.Funcs = &oci.Funcs{
		GetBlob_:               r.getBlob,
		GetBlobRange_:          r.getBlobRange,
		GetManifest_:           r.getManifest,
		GetTag_:                r.getTag,
		ResolveBlob_:           r.resolveBlob,
		ResolveManifest_:       r.resolveManifest,
		ResolveTag_:            r.resolveTag,
		PushBlob_:              r.pushBlob,
		PushBlobChunked_:       r.pushBlobChunked,
		PushBlobChunkedResume_: r.pushBlobChunkedResume,
		MountBlob_:             r.mountBlob,
		PushManifest_:          r.pushManifest,
		DeleteBlob_:            r.deleteBlob,
		DeleteManifest_:        r.deleteManifest,
		DeleteTag_:             r.deleteTag,
		Repositories_:          r.repositories,
		Tags_:                  r.tags,
		Referrers_:             r.referrers,
	}
	return r, nil
}

func (r *Store) CleanupExpiredUploads(ctx context.Context, cutoff time.Time) error {
	for {
		uploads, err := r.metadata.ListExpiredUploads(ctx, cutoff, 100)
		if err != nil {
			return fmt.Errorf("listing expired uploads: %w", err)
		}
		if len(uploads) == 0 {
			return nil
		}
		for _, upload := range uploads {
			if err := r.content.CancelUpload(ctx, upload.ID); err != nil {
				return fmt.Errorf("canceling expired upload %s: %w", upload.ID, err)
			}
			if err := r.metadata.DeleteUpload(ctx, upload.Repository, upload.ID); err != nil && !errors.Is(err, backend.ErrUploadUnknown) {
				return fmt.Errorf("deleting expired upload metadata %s: %w", upload.ID, err)
			}
		}
	}
}

func (r *Store) CleanupExpiredReservations(ctx context.Context, cutoff time.Time) error {
	for {
		reservations, err := r.metadata.ListExpiredReservations(ctx, cutoff, 100)
		if err != nil {
			return fmt.Errorf("listing expired content reservations: %w", err)
		}
		if len(reservations) == 0 {
			return nil
		}
		for _, reservation := range reservations {
			if err := r.metadata.ReleaseReservation(ctx, reservation); err != nil {
				return fmt.Errorf("releasing expired content reservation %s: %w", reservation.ID, err)
			}
		}
	}
}

func (r *Store) CollectGarbage(ctx context.Context) error {
	for {
		candidates, err := r.metadata.GarbageCandidates(ctx, 100)
		if err != nil {
			return fmt.Errorf("listing garbage candidates: %w", err)
		}
		if len(candidates) == 0 {
			break
		}
		for _, digest := range candidates {
			if err := r.collectDigest(ctx, digest); err != nil {
				return err
			}
		}
	}
	if lister, ok := r.content.(backend.ContentLister); ok {
		var digests []oci.Digest
		if err := lister.ListBlobs(ctx, func(digest oci.Digest) error {
			digests = append(digests, digest)
			return nil
		}); err != nil {
			return fmt.Errorf("listing stored blobs: %w", err)
		}
		for _, digest := range digests {
			if err := r.collectDigest(ctx, digest); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Store) collectDigest(ctx context.Context, digest oci.Digest) error {
	lease, claimed, err := r.metadata.ClaimGarbage(ctx, digest)
	if err != nil {
		return fmt.Errorf("claiming garbage digest %s: %w", digest, err)
	}
	if !claimed {
		return nil
	}
	if lease == nil {
		return fmt.Errorf("metadata store granted garbage claim for %s without a lease", digest)
	}
	if err := r.content.DeleteBlob(ctx, digest); err != nil {
		return fmt.Errorf("deleting garbage digest %s: %w", digest, err)
	}
	if err := r.metadata.CompleteGarbage(ctx, *lease); err != nil {
		return fmt.Errorf("completing garbage collection for %s: %w", digest, err)
	}
	return nil
}

func (r *Store) getBlob(ctx context.Context, repository string, digest oci.Digest) (oci.BlobReader, error) {
	desc, err := r.metadata.Blob(ctx, repository, digest)
	if err != nil {
		return nil, err
	}
	reader, err := r.content.Open(ctx, digest)
	if err != nil {
		return nil, mapContentError(err, false)
	}
	return &blobReader{ReadCloser: reader, descriptor: desc}, nil
}

func (r *Store) getBlobRange(ctx context.Context, repository string, digest oci.Digest, start, end int64) (oci.BlobReader, error) {
	desc, err := r.metadata.Blob(ctx, repository, digest)
	if err != nil {
		return nil, err
	}
	reader, err := r.content.OpenRange(ctx, digest, start, end)
	if err != nil {
		return nil, mapContentError(err, false)
	}
	return &blobReader{ReadCloser: reader, descriptor: desc}, nil
}

func (r *Store) getManifest(ctx context.Context, repository string, digest oci.Digest) (oci.BlobReader, error) {
	record, err := r.metadata.Manifest(ctx, repository, digest)
	if err != nil {
		return nil, err
	}
	reader, err := r.content.Open(ctx, digest)
	if err != nil {
		return nil, mapContentError(err, true)
	}
	return &blobReader{ReadCloser: reader, descriptor: record.Descriptor}, nil
}

func (r *Store) getTag(ctx context.Context, repository, tag string) (oci.BlobReader, error) {
	desc, err := r.metadata.ResolveTag(ctx, repository, tag)
	if err != nil {
		return nil, err
	}
	return r.getManifest(ctx, repository, desc.Digest)
}

func (r *Store) resolveBlob(ctx context.Context, repository string, digest oci.Digest) (oci.Descriptor, error) {
	desc, err := r.metadata.Blob(ctx, repository, digest)
	if err != nil {
		return oci.Descriptor{}, err
	}
	return desc, nil
}

func (r *Store) resolveManifest(ctx context.Context, repository string, digest oci.Digest) (oci.Descriptor, error) {
	record, err := r.metadata.Manifest(ctx, repository, digest)
	if err != nil {
		return oci.Descriptor{}, err
	}
	return record.Descriptor, nil
}

func (r *Store) resolveTag(ctx context.Context, repository, tag string) (oci.Descriptor, error) {
	return r.metadata.ResolveTag(ctx, repository, tag)
}

func (r *Store) pushBlob(ctx context.Context, repository string, desc oci.Descriptor, content io.Reader) (oci.Descriptor, error) {
	if err := validateDescriptor(desc); err != nil {
		return oci.Descriptor{}, err
	}
	if desc.MediaType == "" {
		desc.MediaType = "application/octet-stream"
	}
	if err := r.metadata.EnsureRepository(ctx, repository); err != nil {
		return oci.Descriptor{}, err
	}
	reservation, err := r.metadata.ReserveContent(ctx, repository, desc, time.Now())
	if err != nil {
		return oci.Descriptor{}, err
	}
	if err := r.content.PutBlob(ctx, desc.Digest, desc.Size, content); err != nil {
		return oci.Descriptor{}, errors.Join(err, r.metadata.ReleaseReservation(context.WithoutCancel(ctx), reservation))
	}
	if err := r.metadata.RecordBlob(ctx, reservation); err != nil {
		return oci.Descriptor{}, errors.Join(err, r.metadata.ReleaseReservation(context.WithoutCancel(ctx), reservation))
	}
	return desc, nil
}

func (r *Store) pushBlobChunked(ctx context.Context, repository string, chunkSize int) (oci.BlobWriter, error) {
	if err := r.metadata.EnsureRepository(ctx, repository); err != nil {
		return nil, err
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	uploadID := backend.UploadID(id)
	if err := r.content.StartUpload(ctx, uploadID); err != nil {
		return nil, err
	}
	if err := r.metadata.CreateUpload(ctx, backend.UploadSession{
		ID:         uploadID,
		Repository: repository,
		StartedAt:  time.Now(),
	}); err != nil {
		return nil, errors.Join(err, r.content.CancelUpload(context.WithoutCancel(ctx), uploadID))
	}
	return &uploadWriter{registry: r, ctx: ctx, repository: repository, id: uploadID, offset: 0, chunkSize: chunkSize}, nil
}

func (r *Store) pushBlobChunkedResume(ctx context.Context, repository, id string, offset int64, chunkSize int) (oci.BlobWriter, error) {
	uploadID := backend.UploadID(id)
	if _, err := r.metadata.Upload(ctx, repository, uploadID); err != nil {
		if errors.Is(err, backend.ErrUploadUnknown) {
			return nil, oci.ErrBlobUploadUnknown
		}
		return nil, err
	}
	size, err := r.content.UploadSize(ctx, uploadID)
	if err != nil {
		return nil, mapUploadError(err)
	}
	if offset == -1 {
		offset = size
	}
	return &uploadWriter{
		registry:       r,
		ctx:            ctx,
		repository:     repository,
		id:             uploadID,
		offset:         size,
		expectedOffset: offset,
		checkOffset:    offset != -1,
		chunkSize:      chunkSize,
	}, nil
}

func (r *Store) mountBlob(ctx context.Context, fromRepository, toRepository string, digest oci.Digest) (oci.Descriptor, error) {
	desc, err := r.metadata.Blob(ctx, fromRepository, digest)
	if err != nil {
		return oci.Descriptor{}, err
	}
	if err := r.metadata.EnsureRepository(ctx, toRepository); err != nil {
		return oci.Descriptor{}, err
	}
	reservation, err := r.metadata.ReserveContent(ctx, toRepository, desc, time.Now())
	if err != nil {
		return oci.Descriptor{}, err
	}
	if err := r.metadata.RecordBlob(ctx, reservation); err != nil {
		_ = r.metadata.ReleaseReservation(context.WithoutCancel(ctx), reservation)
		return oci.Descriptor{}, err
	}
	return desc, nil
}

func (r *Store) pushManifest(ctx context.Context, repository string, content []byte, mediaType string, params *oci.PushManifestParameters) (oci.Descriptor, error) {
	if !ociref.IsValidRepository(repository) {
		return oci.Descriptor{}, oci.ErrNameInvalid
	}
	if mediaType == "" {
		return oci.Descriptor{}, fmt.Errorf("manifest media type is required")
	}
	if err := r.metadata.EnsureRepository(ctx, repository); err != nil {
		return oci.Descriptor{}, err
	}
	digest := ocidigest.FromBytes(content)
	if params != nil && params.Digest != "" {
		if err := params.Digest.Validate(); err != nil {
			return oci.Descriptor{}, fmt.Errorf("invalid manifest digest: %w: %v", oci.ErrDigestInvalid, err)
		}
		actual, err := params.Digest.Algorithm().FromReader(bytes.NewReader(content))
		if err != nil {
			return oci.Descriptor{}, fmt.Errorf("calculating manifest digest: %w", err)
		}
		if actual != params.Digest {
			return oci.Descriptor{}, oci.ErrDigestInvalid
		}
		digest = params.Digest
	}
	manifest, err := r.parseManifest(mediaType, content)
	if err != nil {
		return oci.Descriptor{}, err
	}
	if err := r.validateManifestDependencies(ctx, repository, manifest); err != nil {
		return oci.Descriptor{}, err
	}
	desc := oci.Descriptor{Digest: digest, MediaType: mediaType, Size: int64(len(content)), ArtifactType: manifest.ArtifactType}
	tags := []string(nil)
	if params != nil {
		tags = params.Tags
		for _, tag := range tags {
			if !ociref.IsValidTag(tag) {
				return oci.Descriptor{}, fmt.Errorf("invalid manifest tag %q: %w", tag, oci.ErrNameInvalid)
			}
		}
	}
	reservation, err := r.metadata.ReserveContent(ctx, repository, desc, time.Now())
	if err != nil {
		return oci.Descriptor{}, err
	}
	if err := r.content.PutBlob(ctx, digest, desc.Size, bytes.NewReader(content)); err != nil {
		return oci.Descriptor{}, errors.Join(err, r.metadata.ReleaseReservation(context.WithoutCancel(ctx), reservation))
	}
	dependencies := manifestDependencies(manifest)
	references := make([]oci.Digest, 0, len(dependencies)+len(manifest.Manifests))
	seenReferences := make(map[oci.Digest]bool)
	for _, dependency := range dependencies {
		if !seenReferences[dependency.Digest] {
			references = append(references, dependency.Digest)
			seenReferences[dependency.Digest] = true
		}
	}
	for _, child := range manifest.Manifests {
		if !seenReferences[child.Digest] {
			references = append(references, child.Digest)
			seenReferences[child.Digest] = true
		}
	}
	_, err = r.metadata.PutManifest(ctx, reservation, backend.ManifestRecord{
		Descriptor:   desc,
		References:   references,
		Subject:      manifest.Subject,
		ArtifactType: manifest.ArtifactType,
		Tags:         tags,
		Config:       manifest.Config,
		Layers:       manifest.Layers,
		Manifests:    manifest.Manifests,
		Annotations:  manifest.Annotations,
		Dependencies: dependencies,
		Details:      manifest.Details,
	})
	if err != nil {
		_ = r.metadata.ReleaseReservation(context.WithoutCancel(ctx), reservation)
		return oci.Descriptor{}, err
	}
	return desc, nil
}

func manifestDependencies(manifest ManifestMetadata) []oci.Descriptor {
	dependencies := make([]oci.Descriptor, 0, len(manifest.Dependencies)+len(manifest.Layers)+1)
	seen := make(map[oci.Digest]bool)
	add := func(descriptor oci.Descriptor) {
		if len(descriptor.URLs) == 0 && !seen[descriptor.Digest] {
			dependencies = append(dependencies, descriptor)
			seen[descriptor.Digest] = true
		}
	}
	for _, descriptor := range manifest.Dependencies {
		add(descriptor)
	}
	if manifest.Config != nil {
		add(*manifest.Config)
	}
	for _, descriptor := range manifest.Layers {
		add(descriptor)
	}
	return dependencies
}

func (r *Store) validateManifestDependencies(ctx context.Context, repository string, manifest ManifestMetadata) error {
	for _, descriptor := range manifest.Manifests {
		if err := validateDescriptor(descriptor); err != nil {
			return err
		}
		// Present child manifests must match their descriptor. Missing children
		// are accepted only when explicitly enabled for partial pulls.
		stored, err := r.metadata.Manifest(ctx, repository, descriptor.Digest)
		if err != nil {
			if errors.Is(err, oci.ErrNameUnknown) || errors.Is(err, oci.ErrManifestUnknown) {
				if r.allowMissingManifestChildren {
					continue
				}
				return fmt.Errorf("referenced manifest %s not found: %w", descriptor.Digest, oci.ErrManifestUnknown)
			}
			return err
		}
		if stored.Descriptor.Size != descriptor.Size {
			return fmt.Errorf("referenced manifest size mismatch: %w", oci.ErrSizeInvalid)
		}
	}
	for _, descriptor := range manifestDependencies(manifest) {
		if err := validateDescriptor(descriptor); err != nil {
			return err
		}
		if len(descriptor.URLs) > 0 {
			continue
		}
		stored, err := r.metadata.Blob(ctx, repository, descriptor.Digest)
		if err != nil {
			if errors.Is(err, oci.ErrNameUnknown) || errors.Is(err, oci.ErrBlobUnknown) {
				return fmt.Errorf("referenced blob %s not found: %w", descriptor.Digest, oci.ErrBlobUnknown)
			}
			return err
		}
		if stored.Size != descriptor.Size {
			return fmt.Errorf("referenced blob size mismatch: %w", oci.ErrSizeInvalid)
		}
	}
	if manifest.Subject != nil {
		if err := validateDescriptor(oci.Descriptor{Digest: *manifest.Subject}); err != nil {
			return fmt.Errorf("invalid subject descriptor: %w", err)
		}
	}
	return nil
}

func (r *Store) deleteBlob(ctx context.Context, repository string, digest oci.Digest) error {
	_, err := r.metadata.DeleteBlob(ctx, repository, digest)
	return err
}

func (r *Store) deleteManifest(ctx context.Context, repository string, digest oci.Digest) error {
	_, err := r.metadata.DeleteManifest(ctx, repository, digest)
	return err
}

func (r *Store) deleteTag(ctx context.Context, repository, tag string) error {
	_, err := r.metadata.DeleteTag(ctx, repository, tag)
	return err
}

func (r *Store) repositories(ctx context.Context, after string) iter.Seq2[string, error] {
	return pageSequence(ctx, func(last string) ([]string, error) { return r.metadata.Repositories(ctx, last, 256) }, after)
}

func (r *Store) tags(ctx context.Context, repository string, params *oci.TagsParameters) iter.Seq2[string, error] {
	var startAfter string
	limit := 0
	if params != nil {
		startAfter = params.StartAfter
		limit = params.Limit
	}
	return func(yield func(string, error) bool) {
		after := startAfter
		remaining := limit
		pageSize := 256
		if remaining > 0 && remaining < pageSize {
			pageSize = remaining
		}
		for {
			if err := ctx.Err(); err != nil {
				yield("", err)
				return
			}
			items, err := r.metadata.Tags(ctx, repository, after, pageSize)
			if err != nil {
				yield("", err)
				return
			}
			for _, item := range items {
				if !yield(item, nil) {
					return
				}
				after = item
				if remaining > 0 {
					remaining--
					if remaining == 0 {
						return
					}
				}
			}
			if len(items) < pageSize {
				return
			}
		}
	}
}

func (r *Store) referrers(ctx context.Context, repository string, digest oci.Digest, params *oci.ReferrersParameters) iter.Seq2[oci.Descriptor, error] {
	artifactType := ""
	if params != nil {
		artifactType = params.ArtifactType
	}
	return func(yield func(oci.Descriptor, error) bool) {
		var after string
		for {
			items, err := r.metadata.Referrers(ctx, repository, digest, artifactType, after, 256)
			if err != nil {
				yield(oci.Descriptor{}, err)
				return
			}
			for _, item := range items {
				if !yield(item, nil) {
					return
				}
				after = item.Digest.String()
			}
			if len(items) < 256 {
				return
			}
		}
	}
}

func pageSequence(ctx context.Context, page func(string) ([]string, error), after string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		last := after
		for {
			if err := ctx.Err(); err != nil {
				yield("", err)
				return
			}
			items, err := page(last)
			if err != nil {
				yield("", err)
				return
			}
			for _, item := range items {
				if !yield(item, nil) {
					return
				}
				last = item
			}
			if len(items) < 256 {
				return
			}
		}
	}
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

func mapContentError(err error, manifest bool) error {
	if errors.Is(err, blobstore.ErrObjectUnknown) {
		if manifest {
			return oci.ErrManifestUnknown
		}
		return oci.ErrBlobUnknown
	}
	return err
}

func mapUploadError(err error) error {
	if errors.Is(err, backend.ErrUploadUnknown) {
		return oci.ErrBlobUploadUnknown
	}
	if errors.Is(err, backend.ErrUploadOffset) {
		return oci.ErrRangeInvalid
	}
	return err
}

type blobReader struct {
	io.ReadCloser
	descriptor oci.Descriptor
}

func (r *blobReader) Descriptor() oci.Descriptor {
	return r.descriptor
}

type uploadWriter struct {
	registry       *Store
	ctx            context.Context
	repository     string
	id             backend.UploadID
	offset         int64
	expectedOffset int64
	checkOffset    bool
	chunkSize      int
}

func (w *uploadWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	expectedOffset := w.offset
	if w.checkOffset {
		expectedOffset = w.expectedOffset
		if expectedOffset != w.offset {
			return 0, oci.ErrRangeInvalid
		}
	}
	next, err := w.registry.content.AppendUpload(w.ctx, w.id, expectedOffset, bytes.NewReader(data))
	w.checkOffset = false
	w.offset = next
	if err != nil {
		return 0, mapUploadError(err)
	}
	return len(data), nil
}

func (w *uploadWriter) Close() error {
	return nil
}

func (w *uploadWriter) Size() int64 {
	return w.offset
}

func (w *uploadWriter) ChunkSize() int {
	if w.chunkSize > 0 {
		return w.chunkSize
	}
	return 4 * 1024 * 1024
}

func (w *uploadWriter) ID() string {
	return string(w.id)
}

func (w *uploadWriter) Commit(digest oci.Digest) (oci.Descriptor, error) {
	if err := digest.Validate(); err != nil {
		return oci.Descriptor{}, fmt.Errorf("invalid upload digest: %w: %v", oci.ErrDigestInvalid, err)
	}
	desc := oci.Descriptor{MediaType: "application/octet-stream", Digest: digest, Size: w.offset}
	reservation, err := w.registry.metadata.ReserveContent(w.ctx, w.repository, desc, time.Now())
	if err != nil {
		return oci.Descriptor{}, err
	}
	if err := w.registry.content.CommitUpload(w.ctx, w.id, digest, w.offset); err != nil {
		_ = w.registry.metadata.ReleaseReservation(context.WithoutCancel(w.ctx), reservation)
		return oci.Descriptor{}, err
	}
	if err := w.registry.metadata.RecordBlob(w.ctx, reservation); err != nil {
		return oci.Descriptor{}, errors.Join(err, w.registry.metadata.ReleaseReservation(context.WithoutCancel(w.ctx), reservation))
	}
	return desc, nil
}

func (w *uploadWriter) Cancel() error {
	contentErr := w.registry.content.CancelUpload(context.WithoutCancel(w.ctx), w.id)
	metadataErr := w.registry.metadata.DeleteUpload(context.WithoutCancel(w.ctx), w.repository, w.id)
	if errors.Is(metadataErr, backend.ErrUploadUnknown) {
		metadataErr = nil
	}
	return errors.Join(contentErr, metadataErr)
}

func randomID() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", fmt.Errorf("generating upload ID: %w", err)
	}
	return fmt.Sprintf("%x", data), nil
}
