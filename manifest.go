package ocistore

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/docker/oci"
)

// ManifestParser handles a manifest format identified by its media type and,
// when present, artifact type.
type ManifestParser interface {
	Supports(mediaType, artifactType string) bool
	ParseManifest(mediaType string, content []byte) (ManifestMetadata, error)
}

// ManifestMetadata is the normalized metadata a parser extracts from a
// manifest. Dependencies are blob descriptors; Manifests are child manifests.
// Config and Layers retain the image-specific projections used by existing
// registry and query APIs.
type ManifestMetadata struct {
	ArtifactType string
	Subject      *oci.Digest
	Annotations  map[string]string
	Config       *oci.Descriptor
	Layers       []oci.Descriptor
	Manifests    []oci.Descriptor
	Dependencies []oci.Descriptor
	Details      json.RawMessage
}

type storeOptions struct {
	manifestParsers              []ManifestParser
	allowMissingManifestChildren bool
}

// Option configures a Store.
type Option func(*storeOptions) error

// WithManifestParser registers a parser for additional manifest formats.
// When multiple registered parsers support the same manifest, the push fails
// rather than selecting one ambiguously.
func WithManifestParser(parser ManifestParser) Option {
	return func(options *storeOptions) error {
		if parser == nil {
			return errors.New("manifest parser is required")
		}
		options.manifestParsers = append(options.manifestParsers, parser)
		return nil
	}
}

// WithAllowMissingManifestChildren allows a manifest to reference child
// manifests that are not present in the same repository. Present children
// must still match their descriptor. This is useful for partial index pulls.
func WithAllowMissingManifestChildren() Option {
	return func(options *storeOptions) error {
		options.allowMissingManifestChildren = true
		return nil
	}
}

func (r *Store) parseManifest(mediaType string, content []byte) (ManifestMetadata, error) {
	artifactType := manifestArtifactType(content)
	var selected ManifestParser
	for _, parser := range r.manifestParsers {
		if !parser.Supports(mediaType, artifactType) {
			continue
		}
		if selected != nil {
			return ManifestMetadata{}, fmt.Errorf("multiple parsers support manifest media type %q and artifact type %q", mediaType, artifactType)
		}
		selected = parser
	}
	if selected != nil {
		metadata, err := selected.ParseManifest(mediaType, content)
		if err != nil {
			return ManifestMetadata{}, fmt.Errorf("parsing manifest %q: %w", mediaType, err)
		}
		if len(metadata.Details) > 0 && !json.Valid(metadata.Details) {
			return ManifestMetadata{}, errors.New("manifest parser returned invalid JSON details")
		}
		return metadata, nil
	}
	return parseDefaultManifest(mediaType, content)
}

func manifestArtifactType(content []byte) string {
	var header struct {
		ArtifactType string `json:"artifactType"`
	}
	_ = json.Unmarshal(content, &header)
	return header.ArtifactType
}

func parseDefaultManifest(mediaType string, content []byte) (ManifestMetadata, error) {
	var document struct {
		MediaType    string            `json:"mediaType"`
		ArtifactType string            `json:"artifactType"`
		Subject      *oci.Descriptor   `json:"subject"`
		Annotations  map[string]string `json:"annotations"`
		Config       *oci.Descriptor   `json:"config"`
		Layers       []oci.Descriptor  `json:"layers"`
		Blobs        []oci.Descriptor  `json:"blobs"`
		Manifests    []oci.Descriptor  `json:"manifests"`
	}
	if err := json.Unmarshal(content, &document); err != nil {
		return ManifestMetadata{}, fmt.Errorf("decoding manifest: %w", err)
	}
	if document.MediaType != "" && document.MediaType != mediaType {
		return ManifestMetadata{}, fmt.Errorf("manifest media type %q does not match Content-Type %q", document.MediaType, mediaType)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(content, &fields); err != nil {
		return ManifestMetadata{}, fmt.Errorf("decoding manifest fields: %w", err)
	}
	_, hasArtifactBlobs := fields["blobs"]

	metadata := ManifestMetadata{
		ArtifactType: document.ArtifactType,
		Annotations:  document.Annotations,
		Config:       document.Config,
		Layers:       document.Layers,
		Manifests:    document.Manifests,
	}
	if document.Subject != nil {
		digest := document.Subject.Digest
		metadata.Subject = &digest
	}
	metadata.Dependencies = append(metadata.Dependencies, document.Blobs...)
	if document.Config != nil {
		metadata.Dependencies = append(metadata.Dependencies, *document.Config)
		if metadata.ArtifactType == "" {
			metadata.ArtifactType = document.Config.MediaType
		}
	}
	metadata.Dependencies = append(metadata.Dependencies, document.Layers...)

	imageManifest := false
	switch mediaType {
	case oci.MediaTypeImageManifest, oci.MediaTypeDockerManifest,
		oci.MediaTypeImageIndex, oci.MediaTypeDockerManifestList:
		if hasArtifactBlobs && document.Config == nil {
			break
		}
		imageManifest = true
		var manifest oci.IndexOrManifest
		if err := json.Unmarshal(content, &manifest); err != nil {
			return ManifestMetadata{}, fmt.Errorf("decoding image manifest: %w", err)
		}
		if manifest.MediaType == "" {
			manifest.MediaType = mediaType
		}
		if manifest.MediaType != mediaType {
			return ManifestMetadata{}, fmt.Errorf("manifest media type %q does not match Content-Type %q", manifest.MediaType, mediaType)
		}
		if err := manifest.Validate(); err != nil {
			return ManifestMetadata{}, fmt.Errorf("invalid manifest: %w", err)
		}
		metadata.Config = manifest.Config
		metadata.Layers = manifest.Layers
		metadata.Manifests = manifest.Manifests
		metadata.Annotations = manifest.Annotations
		metadata.ArtifactType = manifest.ArtifactType
		if metadata.ArtifactType == "" && manifest.Config != nil {
			metadata.ArtifactType = manifest.Config.MediaType
		}
		metadata.Dependencies = nil
		if manifest.Config != nil {
			metadata.Dependencies = append(metadata.Dependencies, *manifest.Config)
		}
		for _, layer := range manifest.Layers {
			if len(layer.URLs) == 0 {
				metadata.Dependencies = append(metadata.Dependencies, layer)
			}
		}
		if manifest.Subject != nil {
			digest := manifest.Subject.Digest
			metadata.Subject = &digest
		}
	}

	if !imageManifest {
		metadata.Details = append(json.RawMessage(nil), content...)
	}
	return metadata, nil
}
