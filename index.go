package ocistore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/docker/oci"
	"github.com/sysson/ocistore/backend"
)

const (
	maxClosure        = 10000
	closureDependents = 1000
	// MaxImageConfigSize bounds how much of an image config Index.ImageConfig reads.
	MaxImageConfigSize = 4 << 20
)

// Metadata is a metadata store together with the indexes it maintains.
type Metadata interface {
	backend.MetadataStore
	backend.MetadataQuerier
}

// Index is the typed, read-only view of a Store's metadata indexes. Results
// are snapshots: they never authorize deletion, which garbage collection
// re-checks itself.
type Index struct {
	metadata Metadata
	content  backend.ContentStore
}

// Index returns the typed metadata view of the store.
func (r *Store) Index() *Index {
	return &Index{metadata: r.metadata, content: r.content}
}

func (x *Index) Repositories(ctx context.Context, after string, limit int) ([]string, error) {
	return x.metadata.Repositories(ctx, after, limit)
}

func (x *Index) TagRecords(ctx context.Context, repository, after string, limit int) ([]backend.TagRecord, error) {
	return x.metadata.TagRecords(ctx, repository, after, limit)
}

func (x *Index) ResolveTag(ctx context.Context, repository, tag string) (oci.Descriptor, error) {
	return x.metadata.ResolveTag(ctx, repository, tag)
}

func (x *Index) Manifest(ctx context.Context, repository string, digest oci.Digest) (backend.ManifestRecord, error) {
	return x.metadata.Manifest(ctx, repository, digest)
}

func (x *Index) Manifests(ctx context.Context, repository string, after oci.Digest, limit int) ([]backend.ManifestRecord, error) {
	return x.metadata.Manifests(ctx, repository, after, limit)
}

func (x *Index) Blobs(ctx context.Context, repository string, after oci.Digest, limit int) ([]backend.BlobRecord, error) {
	return x.metadata.Blobs(ctx, repository, after, limit)
}

func (x *Index) Referrers(ctx context.Context, repository string, digest oci.Digest, artifactType, after string, limit int) ([]oci.Descriptor, error) {
	return x.metadata.Referrers(ctx, repository, digest, artifactType, after, limit)
}

func (x *Index) Dependents(ctx context.Context, digest oci.Digest, limit int) ([]backend.Dependent, error) {
	return x.metadata.Dependents(ctx, digest, limit)
}

func (x *Index) Locations(ctx context.Context, digest oci.Digest) ([]backend.Location, error) {
	return x.metadata.Locations(ctx, digest)
}

func (x *Index) Reserved(ctx context.Context, digest oci.Digest) (bool, error) {
	return x.metadata.Reserved(ctx, digest)
}

// ImageConfig returns the raw image config config describes, or nil when it
// is not an image config.
func (x *Index) ImageConfig(ctx context.Context, config oci.Descriptor) ([]byte, error) {
	if !IsImageConfig(config.MediaType) {
		return nil, nil
	}
	if config.Size > MaxImageConfigSize {
		return nil, fmt.Errorf("image config %s is larger than %d bytes", config.Digest, MaxImageConfigSize)
	}
	reader, err := x.content.Open(ctx, config.Digest)
	if err != nil {
		return nil, fmt.Errorf("reading image config %s: %w", config.Digest, mapContentError(err, false))
	}
	defer func() { _ = reader.Close() }()
	raw, err := io.ReadAll(io.LimitReader(reader, MaxImageConfigSize+1))
	if err != nil {
		return nil, fmt.Errorf("reading image config %s: %w", config.Digest, err)
	}
	if len(raw) > MaxImageConfigSize {
		return nil, fmt.Errorf("image config %s is larger than %d bytes", config.Digest, MaxImageConfigSize)
	}
	return raw, nil
}

func IsImageConfig(mediaType string) bool {
	return mediaType == "application/vnd.oci.image.config.v1+json" ||
		mediaType == "application/vnd.docker.container.image.v1+json"
}

// ClosureRole is how content takes part in a manifest closure.
type ClosureRole string

const (
	RoleManifest ClosureRole = "MANIFEST"
	RoleConfig   ClosureRole = "CONFIG"
	RoleLayer    ClosureRole = "LAYER"
)

// ClosureEntry is a digest in a manifest closure, classified against
// everything outside the closure.
type ClosureEntry struct {
	Descriptor oci.Descriptor
	Role       ClosureRole
	// RetainedInRepository is true if removing the closure from its
	// repository must keep the digest there: a manifest in the repository
	// outside the closure references it, or it is a tagged non-root manifest.
	RetainedInRepository bool
	// Shared is true if the content outlives removal of the closure from its
	// repository.
	Shared bool
	// SharedWith are the manifests outside the closure referencing the digest.
	SharedWith []backend.Dependent
	// Locations are every repository holding the digest.
	Locations []backend.Location
}

type closureMember struct {
	descriptor oci.Descriptor
	role       ClosureRole
	root       bool
	tagged     bool
}

// Closure walks the manifest digest names and everything it transitively
// references in repository, root first. Child manifests absent from the
// repository are skipped.
func (x *Index) Closure(ctx context.Context, repository string, digest oci.Digest) ([]ClosureEntry, error) {
	root, err := x.metadata.Manifest(ctx, repository, digest)
	if err != nil {
		return nil, err
	}
	members, err := x.walkClosure(ctx, repository, root)
	if err != nil {
		return nil, err
	}
	manifests := make(map[oci.Digest]bool, len(members))
	for _, member := range members {
		if member.role == RoleManifest {
			manifests[member.descriptor.Digest] = true
		}
	}
	result := make([]ClosureEntry, 0, len(members))
	for _, member := range members {
		entry, err := x.classify(ctx, repository, member, manifests)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, nil
}

func (x *Index) walkClosure(ctx context.Context, repository string, root backend.ManifestRecord) ([]closureMember, error) {
	var members []closureMember
	seen := make(map[oci.Digest]bool)
	add := func(member closureMember) error {
		if seen[member.descriptor.Digest] {
			return nil
		}
		if len(members) >= maxClosure {
			return fmt.Errorf("manifest closure exceeds %d entries", maxClosure)
		}
		seen[member.descriptor.Digest] = true
		members = append(members, member)
		return nil
	}
	queue := []backend.ManifestRecord{root}
	if err := add(closureMember{descriptor: root.Descriptor, role: RoleManifest, root: true}); err != nil {
		return nil, err
	}
	for len(queue) > 0 {
		record := queue[0]
		queue = queue[1:]
		for _, child := range record.Manifests {
			if seen[child.Digest] {
				continue
			}
			childRecord, err := x.metadata.Manifest(ctx, repository, child.Digest)
			if err != nil {
				if errors.Is(err, oci.ErrNameUnknown) || errors.Is(err, oci.ErrManifestUnknown) {
					continue
				}
				return nil, err
			}
			if err := add(closureMember{descriptor: childRecord.Descriptor, role: RoleManifest, tagged: len(childRecord.Tags) > 0}); err != nil {
				return nil, err
			}
			queue = append(queue, childRecord)
		}
		if record.Config != nil {
			if err := add(closureMember{descriptor: *record.Config, role: RoleConfig}); err != nil {
				return nil, err
			}
		}
		for _, layer := range record.Layers {
			if len(layer.URLs) > 0 {
				continue
			}
			if err := add(closureMember{descriptor: layer, role: RoleLayer}); err != nil {
				return nil, err
			}
		}
	}
	return members, nil
}

func (x *Index) classify(ctx context.Context, repository string, member closureMember, manifests map[oci.Digest]bool) (ClosureEntry, error) {
	digest := member.descriptor.Digest
	dependents, err := x.metadata.Dependents(ctx, digest, closureDependents)
	if err != nil {
		return ClosureEntry{}, err
	}
	outside := make([]backend.Dependent, 0, len(dependents))
	retained := member.tagged && !member.root
	shared := false
	for _, dependent := range dependents {
		if dependent.Repository == repository && manifests[dependent.Manifest] {
			continue
		}
		outside = append(outside, dependent)
		if dependent.Repository == repository {
			retained = true
		} else {
			shared = true
		}
	}
	// A full page may hide outside dependents; stay conservative.
	if len(dependents) == closureDependents {
		retained = true
	}
	locations, err := x.metadata.Locations(ctx, digest)
	if err != nil {
		return ClosureEntry{}, err
	}
	for _, location := range locations {
		if location.Repository != repository {
			shared = true
		}
	}
	return ClosureEntry{
		Descriptor:           member.descriptor,
		Role:                 member.role,
		RetainedInRepository: retained,
		Shared:               shared || retained,
		SharedWith:           outside,
		Locations:            locations,
	}, nil
}
