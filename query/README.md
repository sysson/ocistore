# query

The `query` package provides a read-only GraphQL API over an `ocistore.Store` index. It can run in-process with `Exec` or as an HTTP handler with `ServeHTTP`. It does not start a server, add authentication, or perform registry mutations for you.

## Create a service

Create the registry store as usual, then pass its index to `query.New`:

```go
service, err := query.New(store.Index())
if err != nil {
	return err
}
```

`query.Schema()` returns the GraphQL schema as a string, for example for GraphQL tooling or schema inspection.

## Run a query in-process

`Exec` takes a context, GraphQL document, operation name, and JSON-encodable variables. Check `Response.Errors` before decoding the response data:

```go
import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sysson/ocistore"
	"github.com/sysson/ocistore/query"
)

func lookupManifest(ctx context.Context, store *ocistore.Store, repository, reference string) error {
	service, err := query.New(store.Index())
	if err != nil {
		return err
	}

	response := service.Exec(ctx, `
		query ManifestByReference($repository: String!, $reference: String!) {
			manifest(repository: $repository, reference: $reference) {
				digest
				mediaType
				artifactType
				tags
				references { digest mediaType size }
				details
			}
		}`, "ManifestByReference", map[string]any{
		"repository": repository,
		"reference":  reference,
	})
	if len(response.Errors) != 0 {
		return fmt.Errorf("GraphQL query failed: %v", response.Errors)
	}

	var result struct {
		Manifest *struct {
			Digest      string          `json:"digest"`
			MediaType   string          `json:"mediaType"`
			ArtifactType *string         `json:"artifactType"`
			Tags        []string        `json:"tags"`
			References  []struct {
				Digest    string `json:"digest"`
				MediaType string `json:"mediaType"`
				Size      int64  `json:"size"`
			} `json:"references"`
			Details json.RawMessage `json:"details"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(response.Data, &result); err != nil {
		return err
	}
	if result.Manifest == nil {
		fmt.Println("manifest not found")
		return nil
	}
	fmt.Printf("%s (%s)\n", result.Manifest.Digest, result.Manifest.MediaType)
	return nil
}
```

The `manifest` field resolves either a tag or a digest. Supply exactly one of `reference` or `digest`. For example, query by digest with `manifest(repository: $repository, digest: $digest)`. The older `image(repository:, reference:)` field remains available as an alias.

A manifest lookup returns `null` when the repository or reference does not exist. Invalid arguments and execution failures are returned in `Response.Errors`; execution may also return partial `Response.Data` alongside errors.

## Query artifact metadata

The `Manifest` type contains fields common to OCI content: `mediaType`, `artifactType`, `annotations`, `subject`, `referrers`, `dependents`, `references`, and `closure`. `references` lists the direct blob and child-manifest descriptors indexed for the manifest. `details` is a JSON scalar returned by the manifest parser; select it as a scalar, without nested fields:

```graphql
query Artifact($repository: String!, $tag: String!) {
  manifest(repository: $repository, reference: $tag) {
    digest
    artifactType
    details
    references { digest mediaType size }
    subject { digest mediaType }
    referrers(first: 20) { digest artifactType }
  }
}
```

Image-specific data remains optional. `config`, `layers`, and `imageConfig` are useful for image manifests; `imageConfig` is `null` for non-image content. For multi-platform indexes, use `manifests` to inspect child descriptors. Custom parser details are available through `details`, while the common metadata and dependency fields work across formats.

## Register a custom artifact parser

This example stores a made-up widget artifact. Its manifest uses a custom media type and artifact type, points to a separately uploaded data blob, and includes JSON metadata. The parser identifies that format, declares the blob dependency so the store can validate and retain it, and exposes the format-specific JSON through `Manifest.details`.

```go
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/docker/oci"
	"github.com/docker/oci/ocidigest"
	"github.com/sysson/ocistore"
	"github.com/sysson/ocistore/backend"
	"github.com/sysson/ocistore/query"
)

const (
	widgetManifestMediaType = "application/vnd.example.widget.manifest.v1+json"
	widgetArtifactType      = "application/vnd.example.widget.v1"
	widgetDataMediaType     = "application/vnd.example.widget.data.v1"
)

type widgetManifest struct {
	MediaType    string          `json:"mediaType"`
	ArtifactType string          `json:"artifactType"`
	Data         oci.Descriptor  `json:"data"`
	Details      json.RawMessage `json:"details"`
}

type widgetParser struct{}

func (widgetParser) Supports(mediaType, artifactType string) bool {
	return mediaType == widgetManifestMediaType && artifactType == widgetArtifactType
}

func (widgetParser) ParseManifest(mediaType string, content []byte) (ocistore.ManifestMetadata, error) {
	var manifest widgetManifest
	if err := json.Unmarshal(content, &manifest); err != nil {
		return ocistore.ManifestMetadata{}, err
	}
	if manifest.MediaType != mediaType || manifest.ArtifactType != widgetArtifactType {
		return ocistore.ManifestMetadata{}, fmt.Errorf("invalid widget manifest type")
	}
	if !json.Valid(manifest.Details) {
		return ocistore.ManifestMetadata{}, fmt.Errorf("widget details must be valid JSON")
	}
	return ocistore.ManifestMetadata{
		ArtifactType: manifest.ArtifactType,
		Dependencies: []oci.Descriptor{manifest.Data},
		Details:      manifest.Details,
	}, nil
}

func publishAndQueryWidget(ctx context.Context, content backend.ContentStore, metadata ocistore.Metadata) error {
	store, err := ocistore.New(content, metadata, ocistore.WithManifestParser(widgetParser{}))
	if err != nil {
		return err
	}

	repository := "widgets/demo"
	payload := []byte("widget payload")
	data := oci.Descriptor{
		MediaType: widgetDataMediaType,
		Digest:    ocidigest.FromBytes(payload),
		Size:      int64(len(payload)),
	}
	if _, err := store.PushBlob(ctx, repository, data, bytes.NewReader(payload)); err != nil {
		return err
	}

	manifest, err := json.Marshal(widgetManifest{
		MediaType:    widgetManifestMediaType,
		ArtifactType: widgetArtifactType,
		Data:         data,
		Details:      json.RawMessage(`{"name":"demo","version":3}`),
	})
	if err != nil {
		return err
	}
	if _, err := store.PushManifest(ctx, repository, manifest, widgetManifestMediaType,
		&oci.PushManifestParameters{Tags: []string{"latest"}}); err != nil {
		return err
	}

	service, err := query.New(store.Index())
	if err != nil {
		return err
	}
	response := service.Exec(ctx, `query($repository: String!, $reference: String!) {
		manifest(repository: $repository, reference: $reference) {
			digest artifactType details references { digest mediaType size }
		}
	}`, "", map[string]any{"repository": repository, "reference": "latest"})
	if len(response.Errors) != 0 {
		return fmt.Errorf("GraphQL query failed: %v", response.Errors)
	}
	var result struct {
		Manifest *struct {
			Digest       string `json:"digest"`
			ArtifactType string `json:"artifactType"`
			Details      struct {
				Name    string `json:"name"`
				Version int    `json:"version"`
			} `json:"details"`
			References []struct {
				Digest string `json:"digest"`
			} `json:"references"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(response.Data, &result); err != nil {
		return err
	}
	if result.Manifest == nil {
		return fmt.Errorf("widget manifest not found")
	}
	fmt.Printf("%s: %s v%d (%s)\n", result.Manifest.ArtifactType,
		result.Manifest.Details.Name, result.Manifest.Details.Version, result.Manifest.Digest)
	return nil
}
```

Upload dependency blobs before pushing the manifest: the store validates each declared blob dependency in the same repository. `Manifest.details` is a GraphQL JSON scalar, so request it without subfields and decode it into the format-specific Go type you expect. The generic `references` and `closure` fields still work for this custom format, and the declared dependency participates in garbage-collection reachability.

Child manifest descriptors are strict by default: each child declared in `ManifestMetadata.Manifests` must already exist in the same repository and match its descriptor. This normally applies to OCI indexes. For partial pulls where some index children are intentionally omitted, create the store with `ocistore.WithAllowMissingManifestChildren()`. Present children are still checked for size mismatches, and blob dependencies remain required.

## List repositories and content

List fields are paginated. `first` defaults to 100 and must be between 1 and 1000. `after` is an exclusive cursor: repository names and tags use their names; repository manifests and blobs use their digests.

```graphql
query RepositoryPage($first: Int!, $after: String) {
  repositories(first: $first, after: $after) {
    name
    tags(first: 20) { name digest }
    manifests(first: 20) { digest mediaType artifactType }
    blobs(first: 20) { digest mediaType size }
  }
}
```

Pass the last returned repository name as `after` to fetch the next repository page. For tags, manifests, and blobs, pass the last returned name or digest to the corresponding field's `after` argument.

The registry-wide `content(digest:)` query reports locations, dependents, reservations, and whether the digest is currently referenced. These are read-only snapshots; garbage collection rechecks reachability before deleting content. The query service has no mutation fields.

## Serve over HTTP

`Service` implements `http.Handler`. Mount it on an application server and add your own authentication, TLS, and request policies:

```go
import (
	"log"
	"net/http"
)

// service is the query.Service created above.
mux := http.NewServeMux()
mux.Handle("/graphql", service)
log.Fatal(http.ListenAndServe(":8080", mux))
```

The handler accepts GraphQL requests with `POST` JSON bodies or `GET` query parameters. A basic POST request looks like:

```sh
curl http://localhost:8080/graphql \
  -H 'Content-Type: application/json' \
  --data-binary '{"query":"{ repositories { name } }"}'
```

The handler limits request bodies to 1 MiB, query documents to 64 KiB, and selection depth to 16. Pagination is independently capped at 1000 items per field. These limits are fixed by the package.
