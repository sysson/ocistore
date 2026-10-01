package natskv_test

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/sysson/ocistore/kv"
	"github.com/sysson/ocistore/kv/kvtest"
	"github.com/sysson/ocistore/kv/natskv"
)

func TestConformance(t *testing.T) {
	nc, err := nats.Connect(kvtest.StartNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	store, err := natskv.New(context.Background(), nc, natskv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	kvtest.Run(t, store)
}

func TestConfigOpen(t *testing.T) {
	store, err := natskv.Config{
		Servers: []string{kvtest.StartNATS(t)},
		Stream:  "META_TEST", Subject: "test.meta",
	}.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	key := kv.Key("repo", "library/alpine", "tag.with.dots and spaces%")
	if err := store.Update(ctx, func(tx kv.Txn) error { return tx.Put(key, []byte("v")) }); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := store.View(ctx, func(r kv.Reader) error {
		got = nil
		return r.Scan(ctx, kv.Prefix("repo"), "", func(k string, _ []byte) (bool, error) {
			got = append(got, k)
			return true, nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != key {
		t.Fatalf("scan = %q, want %q", got, key)
	}
}

func TestConfigValidate(t *testing.T) {
	valid := map[string]natskv.Config{
		"bare":   {Servers: []string{"a:4222", "b:4222"}},
		"urls":   {Servers: []string{"nats://u:p@a:4222", "tls://b:4222"}, Options: natskv.Options{Stream: "META", Subject: "dinki.meta", Replicas: 3}},
		"secure": {Servers: []string{"a:4222"}, CredentialsFile: "/c", TLS: &kv.TLSConfig{CAFile: "/ca"}},
	}
	for name, cfg := range valid {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: Validate() = %v", name, err)
		}
	}
	invalid := map[string]natskv.Config{
		"no servers":       {},
		"bad scheme":       {Servers: []string{"http://a:4222"}},
		"path":             {Servers: []string{"nats://a:4222/STREAM"}},
		"dotted stream":    {Servers: []string{"a:4222"}, Options: natskv.Options{Stream: "a.b"}},
		"wildcard subject": {Servers: []string{"a:4222"}, Options: natskv.Options{Subject: "a.*"}},
		"empty token":      {Servers: []string{"a:4222"}, Options: natskv.Options{Subject: "a..b"}},
		"replicas":         {Servers: []string{"a:4222"}, Options: natskv.Options{Replicas: 6}},
		"cert without key": {Servers: []string{"a:4222"}, TLS: &kv.TLSConfig{CertFile: "/c"}},
	}
	for name, cfg := range invalid {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}
