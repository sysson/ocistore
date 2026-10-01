package query_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/docker/oci"
	"github.com/docker/oci/ocidigest"
	"github.com/sysson/ocistore"
	"github.com/sysson/ocistore/blobstore/memblob"
	"github.com/sysson/ocistore/kv/memkv"
	"github.com/sysson/ocistore/kvmeta"
	"github.com/sysson/ocistore/query"
)

const (
	imageManifestType = "application/vnd.oci.image.manifest.v1+json"
	indexType         = "application/vnd.oci.image.index.v1+json"
	configType        = "application/vnd.oci.image.config.v1+json"
	layerType         = "application/vnd.oci.image.layer.v1.tar+gzip"
)

type fixture struct {
	t        *testing.T
	ctx      context.Context
	registry *ocistore.Store
	service  *query.Service
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	content, err := (memblob.Config{}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = content.Close() })
	metadata, err := kvmeta.New(ctx, memkv.New())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = metadata.Close() })
	registry, err := ocistore.New(content, metadata)
	if err != nil {
		t.Fatal(err)
	}
	service, err := query.New(registry.Index())
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, ctx: ctx, registry: registry, service: service}
}

func (f *fixture) blob(repo, mediaType string, data []byte) oci.Descriptor {
	f.t.Helper()
	desc := oci.Descriptor{MediaType: mediaType, Digest: ocidigest.FromBytes(data), Size: int64(len(data))}
	if _, err := f.registry.PushBlob(f.ctx, repo, desc, bytes.NewReader(data)); err != nil {
		f.t.Fatal(err)
	}
	return desc
}

func (f *fixture) manifest(repo, mediaType string, value any, tags ...string) oci.Descriptor {
	f.t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	desc, err := f.registry.PushManifest(f.ctx, repo, data, mediaType, &oci.PushManifestParameters{Tags: tags})
	if err != nil {
		f.t.Fatal(err)
	}
	return desc
}

func (f *fixture) image(repo string, config []byte, layers []oci.Descriptor, subject *oci.Descriptor, tags ...string) oci.Descriptor {
	f.t.Helper()
	configDesc := f.blob(repo, configType, config)
	value := map[string]any{
		"schemaVersion": 2,
		"mediaType":     imageManifestType,
		"config":        configDesc,
		"layers":        layers,
		"annotations":   map[string]string{"org.opencontainers.image.title": "test"},
	}
	if subject != nil {
		value["subject"] = subject
		value["artifactType"] = "application/vnd.example.sbom"
	}
	return f.manifest(repo, imageManifestType, value, tags...)
}

func (f *fixture) query(document string, variables map[string]any) map[string]any {
	f.t.Helper()
	response := f.service.Exec(f.ctx, document, "", variables)
	if len(response.Errors) > 0 {
		f.t.Fatalf("query errors: %v", response.Errors)
	}
	var data map[string]any
	if err := json.Unmarshal(response.Data, &data); err != nil {
		f.t.Fatal(err)
	}
	return data
}

func path(t *testing.T, value any, keys ...any) any {
	t.Helper()
	for _, key := range keys {
		switch k := key.(type) {
		case string:
			m, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("at %q: %T is not an object", k, value)
			}
			value = m[k]
		case int:
			list, ok := value.([]any)
			if !ok || k >= len(list) {
				t.Fatalf("at [%d]: %v is not a long enough list", k, value)
			}
			value = list[k]
		}
	}
	return value
}

const config1 = `{"architecture":"amd64","os":"linux","created":"2024-01-02T03:04:05Z",` +
	`"config":{"Env":["PATH=/bin"],"Cmd":["sh"],"Labels":{"b":"2","a":"1"},"ExposedPorts":{"80/tcp":{}}},` +
	`"rootfs":{"type":"layers","diff_ids":["sha256:aa"]},` +
	`"history":[{"created":"2024-01-02T03:04:05Z","created_by":"ADD rootfs"},{"created_by":"CMD sh","empty_layer":true}]}`

func TestImageQueries(t *testing.T) {
	f := newFixture(t)
	shared := f.blob("app", layerType, []byte("shared layer"))
	own := f.blob("app", layerType, []byte("app layer"))
	other := f.blob("app", layerType, []byte("other layer"))
	app := f.image("app", []byte(config1), []oci.Descriptor{shared, own}, nil, "v1", "latest")
	f.image("app", []byte(`{"architecture":"arm64","os":"linux"}`), []oci.Descriptor{shared, other}, nil, "v2")
	f.image("app", []byte(`{"sbom":true}`), []oci.Descriptor{f.blob("app", layerType, []byte("sbom"))}, &app)

	// The same shared layer also lives in another repository.
	f.blob("mirror", layerType, []byte("shared layer"))

	data := f.query(`query($repo: String!, $ref: String!) {
		repositories { name }
		image(repository: $repo, reference: $ref) {
			digest mediaType size tags artifactType
			annotations { key value }
			config { digest mediaType }
			layers { digest size }
			referrers { artifactType manifest { digest } }
			imageConfig {
				architecture os created env cmd exposedPorts diffIds
				labels { key value }
				history { createdBy emptyLayer }
			}
			closure { digest role shared retainedInRepository sharedWith { digest } locations { repository kind } }
		}
		repository(name: $repo) { tags { name digest } }
		missing: image(repository: $repo, reference: "nope") { digest }
	}`, map[string]any{"repo": "app", "ref": "v1"})

	if got := path(t, data, "repositories"); len(got.([]any)) != 2 {
		t.Fatalf("repositories = %v", got)
	}
	image := path(t, data, "image")
	if got := path(t, image, "digest"); got != string(app.Digest) {
		t.Fatalf("digest = %v, want %s", got, app.Digest)
	}
	if got := path(t, image, "size"); got != float64(app.Size) {
		t.Fatalf("size = %v, want %d", got, app.Size)
	}
	if got := path(t, image, "tags"); len(got.([]any)) != 2 {
		t.Fatalf("tags = %v", got)
	}
	if got := path(t, image, "artifactType"); got != configType {
		t.Fatalf("artifactType = %v", got)
	}
	if got := path(t, image, "annotations", 0, "value"); got != "test" {
		t.Fatalf("annotation = %v", got)
	}
	if got := path(t, image, "layers"); len(got.([]any)) != 2 {
		t.Fatalf("layers = %v", got)
	}
	if got := path(t, image, "referrers", 0, "artifactType"); got != "application/vnd.example.sbom" {
		t.Fatalf("referrer artifactType = %v", got)
	}
	if got := path(t, image, "referrers", 0, "manifest", "digest"); got == nil {
		t.Fatal("referrer manifest not resolved")
	}
	cfg := path(t, image, "imageConfig")
	if path(t, cfg, "architecture") != "amd64" || path(t, cfg, "created") != "2024-01-02T03:04:05Z" {
		t.Fatalf("imageConfig = %v", cfg)
	}
	if path(t, cfg, "labels", 0, "key") != "a" || path(t, cfg, "exposedPorts", 0) != "80/tcp" {
		t.Fatalf("labels/ports = %v", cfg)
	}
	if path(t, cfg, "history", 1, "emptyLayer") != true || path(t, cfg, "history", 0, "createdBy") != "ADD rootfs" {
		t.Fatalf("history = %v", path(t, cfg, "history"))
	}
	if path(t, data, "missing") != nil {
		t.Fatal("missing tag resolved")
	}
	if got := path(t, data, "repository", "tags"); len(got.([]any)) != 3 {
		t.Fatalf("repository tags = %v", got)
	}

	closure := map[string]map[string]any{}
	for _, entry := range path(t, image, "closure").([]any) {
		entry := entry.(map[string]any)
		closure[entry["digest"].(string)] = entry
	}
	if len(closure) != 4 {
		t.Fatalf("closure = %v", closure)
	}
	check := func(name string, digest oci.Digest, role string, retained, isShared bool) {
		t.Helper()
		entry := closure[string(digest)]
		if entry == nil {
			t.Fatalf("%s missing from closure", name)
		}
		if entry["role"] != role || entry["retainedInRepository"] != retained || entry["shared"] != isShared {
			t.Fatalf("%s = %v, want role %s retained %v shared %v", name, entry, role, retained, isShared)
		}
	}
	check("root", app.Digest, "MANIFEST", false, false)
	check("own layer", own.Digest, "LAYER", false, false)
	check("shared layer", shared.Digest, "LAYER", true, true)
	if got := closure[string(shared.Digest)]["sharedWith"]; len(got.([]any)) != 1 {
		t.Fatalf("shared layer sharedWith = %v", got)
	}
	if got := closure[string(shared.Digest)]["locations"]; len(got.([]any)) != 2 {
		t.Fatalf("shared layer locations = %v", got)
	}
}

func TestIndexClosureAndContent(t *testing.T) {
	f := newFixture(t)
	layer := f.blob("multi", layerType, []byte("layer"))
	amd := f.image("multi", []byte(`{"architecture":"amd64","os":"linux"}`), []oci.Descriptor{layer}, nil)
	arm := f.image("multi", []byte(`{"architecture":"arm64","os":"linux"}`), []oci.Descriptor{layer}, nil, "arm")
	amd.Platform = &oci.Platform{OS: "linux", Architecture: "amd64"}
	arm.Platform = &oci.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
	index := f.manifest("multi", indexType, map[string]any{
		"schemaVersion": 2,
		"mediaType":     indexType,
		"manifests":     []oci.Descriptor{amd, arm},
	}, "latest")

	data := f.query(`query($repo: String!, $digest: String!, $layer: String!) {
		manifest(repository: $repo, digest: $digest) {
			manifests { platform { os architecture variant } manifest { digest config { digest } } }
			closure { digest role retainedInRepository shared }
		}
		child: manifest(repository: $repo, digest: $layer) { digest }
		content(digest: $layer) {
			referenced reserved
			locations { repository kind }
			dependents { repository digest }
		}
		unknown: content(digest: "sha256:0000000000000000000000000000000000000000000000000000000000000000") { referenced locations { repository } }
	}`, map[string]any{"repo": "multi", "digest": string(index.Digest), "layer": string(layer.Digest)})

	manifest := path(t, data, "manifest")
	if path(t, manifest, "manifests", 1, "platform", "variant") != "v8" {
		t.Fatalf("platform = %v", path(t, manifest, "manifests"))
	}
	if path(t, manifest, "manifests", 0, "manifest", "config", "digest") == nil {
		t.Fatal("child manifest not resolved")
	}
	roles := map[string]int{}
	for _, entry := range path(t, manifest, "closure").([]any) {
		entry := entry.(map[string]any)
		roles[entry["role"].(string)]++
		switch entry["digest"] {
		case string(arm.Digest):
			if entry["retainedInRepository"] != true {
				t.Fatalf("tagged child not retained: %v", entry)
			}
		case string(layer.Digest), string(amd.Digest), string(index.Digest):
			if entry["retainedInRepository"] != false || entry["shared"] != false {
				t.Fatalf("exclusive entry flagged shared: %v", entry)
			}
		}
	}
	if roles["MANIFEST"] != 3 || roles["CONFIG"] != 2 || roles["LAYER"] != 1 {
		t.Fatalf("closure roles = %v", roles)
	}
	if path(t, data, "child") != nil {
		t.Fatal("blob digest resolved as manifest")
	}
	content := path(t, data, "content")
	if path(t, content, "referenced") != true || path(t, content, "reserved") != false {
		t.Fatalf("content = %v", content)
	}
	if got := path(t, content, "dependents"); len(got.([]any)) != 2 {
		t.Fatalf("dependents = %v", got)
	}
	if path(t, content, "locations", 0, "kind") != "BLOB" {
		t.Fatalf("locations = %v", path(t, content, "locations"))
	}
	if path(t, data, "unknown", "referenced") != false {
		t.Fatal("unknown content referenced")
	}
}

func TestQueryValidation(t *testing.T) {
	f := newFixture(t)
	for _, document := range []string{
		`{ repositories(first: 0) { name } }`,
		`{ repositories(first: 5000) { name } }`,
		`{ content(digest: "nope") { referenced } }`,
		`{ nope }`,
		"{ repositories { tags { manifest { " + strings.Repeat("manifests { manifest { ", 8) + "digest" + strings.Repeat(" } }", 8) + " } } } }",
		"{ repositories { name } }" + strings.Repeat(" ", 64<<10),
	} {
		if response := f.service.Exec(f.ctx, document, "", nil); len(response.Errors) == 0 {
			t.Errorf("%s succeeded, want error", document)
		}
	}
}

func TestServeHTTP(t *testing.T) {
	f := newFixture(t)
	f.blob("app", layerType, []byte("x"))
	server := httptest.NewServer(f.service)
	defer server.Close()

	response, err := http.Post(server.URL, "application/json", strings.NewReader(`{"query":"{ repositories { name } }"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	var body struct {
		Data struct {
			Repositories []struct{ Name string }
		}
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data.Repositories) != 1 || body.Data.Repositories[0].Name != "app" {
		t.Fatalf("body = %+v", body)
	}

	get, err := http.Get(server.URL + "?query=" + "%7B%20repositories%20%7B%20name%20%7D%20%7D")
	if err != nil {
		t.Fatal(err)
	}
	_ = get.Body.Close()
	if get.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d", get.StatusCode)
	}
	put, err := http.NewRequest(http.MethodPut, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	putResponse, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	_ = putResponse.Body.Close()
	if putResponse.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT status = %d", putResponse.StatusCode)
	}
}

func TestQueryDefaultsVariablesAndIntrospection(t *testing.T) {
	f := newFixture(t)
	for _, repo := range []string{"a", "b", "c"} {
		f.blob(repo, layerType, []byte(repo))
	}
	count := func(data map[string]any) int { return len(data["repositories"].([]any)) }
	if got := count(f.query(`{ repositories { name } }`, nil)); got != 3 {
		t.Fatalf("default page = %d repositories, want 3", got)
	}
	if got := count(f.query(`query($first: Int) { repositories(first: $first) { name } }`, map[string]any{"first": nil})); got != 3 {
		t.Fatalf("explicit null first = %d repositories, want 3", got)
	}
	// In-process callers pass Go-typed variables, including pointers.
	after := "a"
	data := f.query(`query($first: Int, $after: String) { repositories(first: $first, after: $after) { name } }`, map[string]any{"first": int64(1), "after": &after})
	if got := path(t, data, "repositories", 0, "name"); count(data) != 1 || got != "b" {
		t.Fatalf("paged repositories = %v, want [b]", data)
	}

	schema := f.query(`{ __type(name: "Manifest") { fields { name } } }`, nil)
	var fields []string
	for _, field := range path(t, schema, "__type", "fields").([]any) {
		fields = append(fields, field.(map[string]any)["name"].(string))
	}
	for _, want := range []string{"closure", "imageConfig", "referrers"} {
		if !slices.Contains(fields, want) {
			t.Fatalf("introspected Manifest fields = %v, want %s", fields, want)
		}
	}
}

func TestServeHTTPIntVariables(t *testing.T) {
	f := newFixture(t)
	f.blob("a", layerType, []byte("a"))
	f.blob("b", layerType, []byte("b"))
	server := httptest.NewServer(f.service)
	defer server.Close()
	response, err := http.Post(server.URL, "application/json", strings.NewReader(`{"query":"query($n: Int) { repositories(first: $n) { name } }","variables":{"n":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	var body struct {
		Data struct {
			Repositories []struct{ Name string }
		}
		Errors []any
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Errors) != 0 || len(body.Data.Repositories) != 1 {
		t.Fatalf("body = %+v, want one repository", body)
	}
}
