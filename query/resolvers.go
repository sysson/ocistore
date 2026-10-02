package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/docker/oci"
	"github.com/sysson/ocistore"
	"github.com/sysson/ocistore/backend"
)

// resolver is the gqlgen resolver root and the Query type resolver.
type resolver struct {
	index *ocistore.Index
}

func (r *resolver) Query() QueryResolver { return r }

func (r *resolver) Repositories(ctx context.Context, first *int32, after *string) ([]*Repository, error) {
	n, err := limit(first)
	if err != nil {
		return nil, err
	}
	names, err := r.index.Repositories(ctx, deref(after), n)
	if err != nil {
		return nil, err
	}
	result := make([]*Repository, len(names))
	for i, name := range names {
		result[i] = &Repository{root: r, name: name}
	}
	return result, nil
}

func (r *resolver) Repository(ctx context.Context, name string) (*Repository, error) {
	if _, err := r.index.TagRecords(ctx, name, "", 1); err != nil {
		if isNotFound(err) || errors.Is(err, oci.ErrNameInvalid) {
			return nil, nil
		}
		return nil, err
	}
	return &Repository{root: r, name: name}, nil
}

func (r *resolver) Image(ctx context.Context, repository, reference string) (*Manifest, error) {
	return r.manifestReference(ctx, repository, reference)
}

func (r *resolver) Manifest(ctx context.Context, repository string, digest, reference *string) (*Manifest, error) {
	if (digest == nil) == (reference == nil) {
		return nil, errors.New("supply exactly one of digest or reference")
	}
	if reference != nil {
		return r.manifestReference(ctx, repository, *reference)
	}
	parsed, err := parseDigest(*digest)
	if err != nil {
		return nil, err
	}
	return r.manifest(ctx, repository, parsed)
}

func (r *resolver) manifestReference(ctx context.Context, repository, reference string) (*Manifest, error) {
	if strings.Contains(reference, ":") {
		parsed, err := parseDigest(reference)
		if err != nil {
			return nil, err
		}
		return r.manifest(ctx, repository, parsed)
	}
	descriptor, err := r.index.ResolveTag(ctx, repository, reference)
	if err != nil {
		if isNotFound(err) || errors.Is(err, oci.ErrNameInvalid) {
			return nil, nil
		}
		return nil, err
	}
	return r.manifest(ctx, repository, descriptor.Digest)
}

func (r *resolver) Content(_ context.Context, digest string) (*Content, error) {
	parsed, err := parseDigest(digest)
	if err != nil {
		return nil, err
	}
	return &Content{root: r, digest: parsed}, nil
}

func (r *resolver) manifest(ctx context.Context, repository string, digest oci.Digest) (*Manifest, error) {
	record, err := r.index.Manifest(ctx, repository, digest)
	if err != nil {
		if isNotFound(err) || errors.Is(err, oci.ErrNameInvalid) {
			return nil, nil
		}
		return nil, err
	}
	return &Manifest{root: r, repository: repository, record: record}, nil
}

func (r *resolver) dependents(ctx context.Context, digest oci.Digest, first *int32) ([]*Dependent, error) {
	n, err := limit(first)
	if err != nil {
		return nil, err
	}
	dependents, err := r.index.Dependents(ctx, digest, n)
	if err != nil {
		return nil, err
	}
	return r.dependentModels(dependents), nil
}

func (r *resolver) dependentModels(dependents []backend.Dependent) []*Dependent {
	result := make([]*Dependent, len(dependents))
	for i, dependent := range dependents {
		result[i] = &Dependent{root: r, dependent: dependent}
	}
	return result
}

func locationModels(locations []backend.Location) []*Location {
	result := make([]*Location, len(locations))
	for i, location := range locations {
		result[i] = &Location{location: location}
	}
	return result
}

func parseDigest(value string) (oci.Digest, error) {
	digest := oci.Digest(value)
	if err := digest.Validate(); err != nil {
		return "", fmt.Errorf("invalid digest %q: %w", value, err)
	}
	return digest, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, oci.ErrNameUnknown) || errors.Is(err, oci.ErrManifestUnknown) ||
		errors.Is(err, oci.ErrBlobUnknown) || errors.Is(err, oci.ErrManifestBlobUnknown)
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// Repository is the GraphQL Repository type.
type Repository struct {
	root *resolver
	name string
}

func (r *Repository) Name() string { return r.name }

func (r *Repository) Tags(ctx context.Context, first *int32, after *string) ([]*Tag, error) {
	n, err := limit(first)
	if err != nil {
		return nil, err
	}
	records, err := r.root.index.TagRecords(ctx, r.name, deref(after), n)
	if err != nil {
		return nil, err
	}
	result := make([]*Tag, len(records))
	for i, record := range records {
		result[i] = &Tag{root: r.root, record: record}
	}
	return result, nil
}

func (r *Repository) Manifests(ctx context.Context, first *int32, after *string) ([]*Manifest, error) {
	n, err := limit(first)
	if err != nil {
		return nil, err
	}
	records, err := r.root.index.Manifests(ctx, r.name, oci.Digest(deref(after)), n)
	if err != nil {
		return nil, err
	}
	result := make([]*Manifest, len(records))
	for i, record := range records {
		result[i] = &Manifest{root: r.root, repository: r.name, record: record}
	}
	return result, nil
}

func (r *Repository) Blobs(ctx context.Context, first *int32, after *string) ([]*Blob, error) {
	n, err := limit(first)
	if err != nil {
		return nil, err
	}
	records, err := r.root.index.Blobs(ctx, r.name, oci.Digest(deref(after)), n)
	if err != nil {
		return nil, err
	}
	result := make([]*Blob, len(records))
	for i, record := range records {
		result[i] = &Blob{root: r.root, record: record}
	}
	return result, nil
}

// Tag is the GraphQL Tag type.
type Tag struct {
	root   *resolver
	record backend.TagRecord
}

func (t *Tag) Repository() string   { return t.record.Repository }
func (t *Tag) Name() string         { return t.record.Tag }
func (t *Tag) Digest() string       { return string(t.record.Digest) }
func (t *Tag) UpdatedAt() time.Time { return t.record.UpdatedAt }
func (t *Tag) Manifest(ctx context.Context) (*Manifest, error) {
	return t.root.manifest(ctx, t.record.Repository, t.record.Digest)
}

// Manifest is the GraphQL Manifest type.
type Manifest struct {
	root       *resolver
	repository string
	record     backend.ManifestRecord
}

func (m *Manifest) Repository() string         { return m.repository }
func (m *Manifest) Digest() string             { return string(m.record.Descriptor.Digest) }
func (m *Manifest) MediaType() string          { return m.record.Descriptor.MediaType }
func (m *Manifest) Size() int64                { return m.record.Descriptor.Size }
func (m *Manifest) ArtifactType() *string      { return optional(m.record.ArtifactType) }
func (m *Manifest) Annotations() []*Annotation { return annotations(m.record.Annotations) }
func (m *Manifest) PushedAt() *time.Time       { return optionalTime(m.record.PushedAt) }
func (m *Manifest) Tags() []string             { return nonNil(m.record.Tags) }
func (m *Manifest) Layers() []*Descriptor      { return m.descriptors(m.record.Layers) }
func (m *Manifest) Manifests() []*Descriptor   { return m.descriptors(m.record.Manifests) }

func (m *Manifest) References() []*Descriptor {
	references := make([]oci.Descriptor, 0, len(m.record.Dependencies)+len(m.record.Layers)+len(m.record.Manifests)+1)
	seen := make(map[oci.Digest]bool)
	add := func(descriptor oci.Descriptor) {
		if len(descriptor.URLs) == 0 && !seen[descriptor.Digest] {
			references = append(references, descriptor)
			seen[descriptor.Digest] = true
		}
	}
	for _, dependency := range m.record.Dependencies {
		add(dependency)
	}
	if m.record.Config != nil {
		add(*m.record.Config)
	}
	for _, layer := range m.record.Layers {
		add(layer)
	}
	for _, child := range m.record.Manifests {
		add(child)
	}
	return m.descriptors(references)
}

func (m *Manifest) Details() (JSON, error) {
	if len(m.record.Details) == 0 {
		return nil, nil
	}
	var details any
	if err := json.Unmarshal(m.record.Details, &details); err != nil {
		return nil, fmt.Errorf("decoding manifest details %s: %w", m.record.Descriptor.Digest, err)
	}
	return JSON(details), nil
}

func (m *Manifest) Config() *Descriptor {
	if m.record.Config == nil {
		return nil
	}
	return m.descriptor(*m.record.Config)
}

func (m *Manifest) Subject(ctx context.Context) (*Descriptor, error) {
	if m.record.Subject == nil {
		return nil, nil
	}
	descriptor := oci.Descriptor{Digest: *m.record.Subject}
	if record, err := m.root.index.Manifest(ctx, m.repository, *m.record.Subject); err == nil {
		descriptor = record.Descriptor
	} else if !isNotFound(err) {
		return nil, err
	}
	return m.descriptor(descriptor), nil
}

func (m *Manifest) Referrers(ctx context.Context, artifactType *string, first *int32, after *string) ([]*Descriptor, error) {
	n, err := limit(first)
	if err != nil {
		return nil, err
	}
	referrers, err := m.root.index.Referrers(ctx, m.repository, m.record.Descriptor.Digest, deref(artifactType), deref(after), n)
	if err != nil {
		return nil, err
	}
	return m.descriptors(referrers), nil
}

func (m *Manifest) Dependents(ctx context.Context, first *int32) ([]*Dependent, error) {
	return m.root.dependents(ctx, m.record.Descriptor.Digest, first)
}

func (m *Manifest) ImageConfig(ctx context.Context) (*ImageConfig, error) {
	if m.record.Config == nil {
		return nil, nil
	}
	raw, err := m.root.index.ImageConfig(ctx, *m.record.Config)
	if err != nil || raw == nil {
		return nil, err
	}
	var config imageConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return nil, fmt.Errorf("decoding image config %s: %w", m.record.Config.Digest, err)
	}
	return &ImageConfig{digest: m.record.Config.Digest, config: config, raw: raw}, nil
}

func (m *Manifest) descriptor(descriptor oci.Descriptor) *Descriptor {
	return &Descriptor{root: m.root, repository: m.repository, descriptor: descriptor}
}

func (m *Manifest) descriptors(descriptors []oci.Descriptor) []*Descriptor {
	result := make([]*Descriptor, len(descriptors))
	for i, descriptor := range descriptors {
		result[i] = m.descriptor(descriptor)
	}
	return result
}

// Descriptor is the GraphQL Descriptor type.
type Descriptor struct {
	root       *resolver
	repository string
	descriptor oci.Descriptor
}

func (d *Descriptor) Digest() string             { return string(d.descriptor.Digest) }
func (d *Descriptor) MediaType() string          { return d.descriptor.MediaType }
func (d *Descriptor) Size() int64                { return d.descriptor.Size }
func (d *Descriptor) ArtifactType() *string      { return optional(d.descriptor.ArtifactType) }
func (d *Descriptor) Annotations() []*Annotation { return annotations(d.descriptor.Annotations) }
func (d *Descriptor) Urls() []string             { return nonNil(d.descriptor.URLs) }

func (d *Descriptor) Platform() *Platform {
	if d.descriptor.Platform == nil {
		return nil
	}
	return &Platform{platform: *d.descriptor.Platform}
}

func (d *Descriptor) Manifest(ctx context.Context) (*Manifest, error) {
	return d.root.manifest(ctx, d.repository, d.descriptor.Digest)
}

// Platform is the GraphQL Platform type.
type Platform struct{ platform oci.Platform }

func (p *Platform) Os() string           { return p.platform.OS }
func (p *Platform) Architecture() string { return p.platform.Architecture }
func (p *Platform) Variant() *string     { return optional(p.platform.Variant) }
func (p *Platform) OsVersion() *string   { return optional(p.platform.OSVersion) }
func (p *Platform) OsFeatures() []string { return nonNil(p.platform.OSFeatures) }

// Annotation is the GraphQL Annotation type.
type Annotation struct {
	Key   string
	Value string
}

func annotations(values map[string]string) []*Annotation {
	result := make([]*Annotation, 0, len(values))
	for key, value := range values {
		result = append(result, &Annotation{Key: key, Value: value})
	}
	slices.SortFunc(result, func(a, b *Annotation) int { return strings.Compare(a.Key, b.Key) })
	return result
}

// Blob is the GraphQL Blob type.
type Blob struct {
	root   *resolver
	record backend.BlobRecord
}

func (b *Blob) Repository() string   { return b.record.Repository }
func (b *Blob) Digest() string       { return string(b.record.Descriptor.Digest) }
func (b *Blob) MediaType() *string   { return optional(b.record.Descriptor.MediaType) }
func (b *Blob) Size() int64          { return b.record.Descriptor.Size }
func (b *Blob) PushedAt() *time.Time { return optionalTime(b.record.PushedAt) }
func (b *Blob) Dependents(ctx context.Context, first *int32) ([]*Dependent, error) {
	return b.root.dependents(ctx, b.record.Descriptor.Digest, first)
}

// Dependent is the GraphQL Dependent type.
type Dependent struct {
	root      *resolver
	dependent backend.Dependent
}

func (d *Dependent) Repository() string { return d.dependent.Repository }
func (d *Dependent) Digest() string     { return string(d.dependent.Manifest) }
func (d *Dependent) Manifest(ctx context.Context) (*Manifest, error) {
	return d.root.manifest(ctx, d.dependent.Repository, d.dependent.Manifest)
}

// Location is the GraphQL Location type.
type Location struct{ location backend.Location }

func (l *Location) Repository() string { return l.location.Repository }
func (l *Location) Kind() string       { return strings.ToUpper(string(l.location.Kind)) }

// Content is the GraphQL Content type.
type Content struct {
	root   *resolver
	digest oci.Digest
}

func (c *Content) Digest() string { return string(c.digest) }

func (c *Content) Reserved(ctx context.Context) (bool, error) {
	return c.root.index.Reserved(ctx, c.digest)
}

func (c *Content) Locations(ctx context.Context) ([]*Location, error) {
	locations, err := c.root.index.Locations(ctx, c.digest)
	if err != nil {
		return nil, err
	}
	return locationModels(locations), nil
}

func (c *Content) Dependents(ctx context.Context, first *int32) ([]*Dependent, error) {
	return c.root.dependents(ctx, c.digest, first)
}

func (c *Content) Referenced(ctx context.Context) (bool, error) {
	locations, err := c.root.index.Locations(ctx, c.digest)
	if err != nil || len(locations) > 0 {
		return len(locations) > 0, err
	}
	dependents, err := c.root.index.Dependents(ctx, c.digest, 1)
	if err != nil || len(dependents) > 0 {
		return len(dependents) > 0, err
	}
	return c.root.index.Reserved(ctx, c.digest)
}
