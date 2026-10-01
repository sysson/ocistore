# ocistore

`github.com/sysson/ocistore` is a Go library for storing OCI registry content and metadata. `ocistore.Store` implements `github.com/docker/oci.Interface`, with pluggable blob and metadata storage. It is a library, not an HTTP registry server.

## Getting started

Requires Go 1.27.1 or newer. Add the module with `go get github.com/sysson/ocistore`.

This example uses a local directory for blobs and a bbolt database for metadata:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/sysson/ocistore"
	"github.com/sysson/ocistore/blobstore/fileblob"
	"github.com/sysson/ocistore/kv/boltkv"
	"github.com/sysson/ocistore/kvmeta"
)

func main() {
	ctx := context.Background()
	content, err := fileblob.Open(ctx, "/var/lib/ocistore/blobs")
	if err != nil {
		log.Fatal(err)
	}
	defer content.Close()

	metadata, err := kvmeta.Open(ctx, boltkv.Config{Path: "/var/lib/ocistore/metadata.db"})
	if err != nil {
		log.Fatal(err)
	}
	defer metadata.Close()

	store, err := ocistore.New(content, metadata)
	if err != nil {
		log.Fatal(err)
	}
	repositories, err := store.Index().Repositories(ctx, "", 100)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(repositories)
}
```

Use absolute paths for the local drivers. bbolt holds an exclusive file lock, so use a remote KV driver when multiple processes need to share metadata.

## Serve a registry

The store implements `oci.Interface`, so `github.com/docker/oci/ociserver` can expose it as an OCI distribution registry. After creating `store` in the example above, replace the repository listing with:

```go
registry, err := ociserver.New(store, nil)
if err != nil {
	log.Fatal(err)
}
log.Fatal(http.ListenAndServe(":5000", registry))
```

Add `"net/http"` and `"github.com/docker/oci/ociserver"` to the imports. The server handles OCI registry requests under `/v2/`. `ListenAndServe` uses plain HTTP; configure TLS and authentication (for example, via `ociserver.ServerConfig.AuthMiddleware`) before exposing the registry beyond a trusted local environment.

## Packages

- `ocistore`: OCI store, read-only metadata `Index`, and cleanup/garbage-collection methods.
- `backend`: content and metadata store interfaces.
- `blobstore`: blob storage via Go CDK, with `fileblob`, `memblob`, `s3blob`, `gcsblob`, and `azureblob` drivers.
- `kv`: transactional key/value storage, with `boltkv`, `memkv`, `etcdkv`, and `natskv` drivers; `remotekv` provides remote KV support.
- `kvmeta`: indexed metadata storage over a `kv.Store`.
- `query`: read-only GraphQL queries over `store.Index()`; `query.New(index)` returns a service with `Exec` and `ServeHTTP` methods.

Call `store.CollectGarbage(ctx)` to remove unreferenced content after deletions. With the bundled Go CDK blobstore, it also scans stored blobs for objects left behind by interrupted writes; custom content backends can support this by implementing `backend.ContentLister`. Scanning the full bucket can be expensive on large stores.

`store.CleanupExpiredUploads(ctx, cutoff)` and `store.CleanupExpiredReservations(ctx, cutoff)` clear abandoned metadata and uploads. For a Go CDK blobstore, `content.CleanupStaged(ctx, cutoff)` removes `pending/` objects from interrupted writes. Choose cutoffs older than any operation that may still be active, and invoke these methods as appropriate for your deployment; no background cleanup is started automatically.