package etcdkv_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sysson/ocistore/kv"
	"github.com/sysson/ocistore/kv/etcdkv"
	"github.com/sysson/ocistore/kv/kvtest"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestConformance(t *testing.T) {
	addr := kvtest.StartEtcd(t)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://" + addr}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	kvtest.Run(t, etcdkv.New(client, "/test", false))
}

func TestLargeCommit(t *testing.T) {
	addr := kvtest.StartEtcd(t)
	store, err := etcdkv.Config{Endpoints: []string{addr}, Prefix: "/large"}.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// More writes than etcd's default per-transaction operation limit.
	err = store.Update(t.Context(), func(tx kv.Txn) error {
		for i := range 500 {
			if err := tx.Put(kv.Key("bulk", fmt.Sprintf("%04d", i)), []byte("x")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := store.View(t.Context(), func(r kv.Reader) error {
		count = 0
		return r.Scan(t.Context(), kv.Prefix("bulk"), "", func(string, []byte) (bool, error) {
			count++
			return true, nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	if count != 500 {
		t.Fatalf("scanned %d keys, want 500", count)
	}
}

func TestConfigValidate(t *testing.T) {
	valid := map[string]etcdkv.Config{
		"bare":      {Endpoints: []string{"a:2379", "b:2379"}},
		"https":     {Endpoints: []string{"https://a:2379"}, TLS: &kv.TLSConfig{CAFile: "/ca"}},
		"auth":      {Endpoints: []string{"a:2379"}, Username: "u", PasswordFile: "/p", Prefix: "/x"},
		"mutualTLS": {Endpoints: []string{"a:2379"}, TLS: &kv.TLSConfig{CertFile: "/c", KeyFile: "/k"}},
	}
	for name, cfg := range valid {
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: Validate() = %v", name, err)
		}
	}
	invalid := map[string]etcdkv.Config{
		"no endpoints":     {},
		"empty endpoint":   {Endpoints: []string{""}},
		"bad scheme":       {Endpoints: []string{"tcp://a:2379"}},
		"path":             {Endpoints: []string{"http://a:2379/x"}},
		"http with tls":    {Endpoints: []string{"http://a:2379"}, TLS: &kv.TLSConfig{}},
		"relative prefix":  {Endpoints: []string{"a:2379"}, Prefix: "x"},
		"trailing slash":   {Endpoints: []string{"a:2379"}, Prefix: "/x/"},
		"user no password": {Endpoints: []string{"a:2379"}, Username: "u"},
		"cert without key": {Endpoints: []string{"a:2379"}, TLS: &kv.TLSConfig{CertFile: "/c"}},
	}
	for name, cfg := range invalid {
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want error", name)
		}
	}
}

func TestClientConfigNormalizesEndpoints(t *testing.T) {
	cfg, prefix, err := etcdkv.Config{Endpoints: []string{"a:2379", "https://b:2379/"}, TLS: &kv.TLSConfig{}}.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(cfg.Endpoints) != "[https://a:2379 https://b:2379]" || prefix != "/dinki" || cfg.TLS == nil {
		t.Fatalf("ClientConfig() = %v, %q, tls %v", cfg.Endpoints, prefix, cfg.TLS != nil)
	}
}
