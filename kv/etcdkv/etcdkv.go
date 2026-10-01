// Package etcdkv is a kv.Store backed by etcd.
//
// Each commit is one etcd transaction guarded by the modification revision
// of a head key. Writes are grouped into nested transactions so a commit can
// exceed etcd's per-transaction operation limit while staying atomic.
package etcdkv

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sysson/ocistore/kv"
	"github.com/sysson/ocistore/kv/remotekv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// opsPerTxn stays below etcd's default --max-txn-ops of 128.
const opsPerTxn = 100

const defaultPrefix = "/ocistore"

// Config selects the etcd driver.
type Config struct {
	// Endpoints are host:port or http(s)://host:port client addresses. Bare
	// addresses use https when TLS is set and http otherwise.
	Endpoints []string `json:"endpoints"`
	// Prefix is the key prefix the store keeps its data under (default
	// /ocistore).
	Prefix string `json:"prefix,omitempty"`
	// Username and PasswordFile, set together, enable etcd authentication.
	Username     string        `json:"username,omitempty"`
	PasswordFile string        `json:"passwordFile,omitempty"`
	TLS          *kv.TLSConfig `json:"tls,omitempty"`
}

var _ kv.Config = Config{}

func (c Config) Validate() error {
	var errs []error
	if len(c.Endpoints) == 0 {
		errs = append(errs, errors.New("etcd.endpoints must not be empty"))
	}
	for _, endpoint := range c.Endpoints {
		if _, err := c.endpoint(endpoint); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Prefix != "" && (!strings.HasPrefix(c.Prefix, "/") || strings.HasSuffix(c.Prefix, "/")) {
		errs = append(errs, fmt.Errorf("etcd.prefix %q must start with / and not end with /", c.Prefix))
	}
	if (c.Username == "") != (c.PasswordFile == "") {
		errs = append(errs, errors.New("etcd.username and etcd.passwordFile must be set together"))
	}
	if err := c.TLS.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("etcd.%w", err))
	}
	return errors.Join(errs...)
}

func (c Config) endpoint(endpoint string) (string, error) {
	if endpoint == "" {
		return "", errors.New("etcd.endpoints must not contain empty entries")
	}
	if !strings.Contains(endpoint, "://") {
		if c.TLS != nil {
			return "https://" + endpoint, nil
		}
		return "http://" + endpoint, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("etcd endpoint %q: %w", endpoint, err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return "", fmt.Errorf("etcd endpoint %q must use http or https", endpoint)
	case u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil:
		return "", fmt.Errorf("etcd endpoint %q must be scheme://host:port", endpoint)
	case u.Scheme == "http" && c.TLS != nil:
		return "", fmt.Errorf("etcd endpoint %q uses http but tls is set", endpoint)
	}
	return u.Scheme + "://" + u.Host, nil
}

// ClientConfig loads any referenced files and returns the etcd client
// configuration and key prefix.
func (c Config) ClientConfig() (clientv3.Config, string, error) {
	if err := c.Validate(); err != nil {
		return clientv3.Config{}, "", err
	}
	cfg := clientv3.Config{DialTimeout: 10 * time.Second}
	for _, endpoint := range c.Endpoints {
		normalized, _ := c.endpoint(endpoint)
		cfg.Endpoints = append(cfg.Endpoints, normalized)
	}
	if c.Username != "" {
		password, err := os.ReadFile(c.PasswordFile)
		if err != nil {
			return clientv3.Config{}, "", fmt.Errorf("reading etcd password: %w", err)
		}
		cfg.Username = c.Username
		cfg.Password = strings.TrimRight(string(password), "\r\n")
	}
	tlsConfig, err := c.TLS.ClientConfig()
	if err != nil {
		return clientv3.Config{}, "", fmt.Errorf("etcd tls: %w", err)
	}
	cfg.TLS = tlsConfig
	prefix := c.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	return cfg, prefix, nil
}

func (c Config) Open(context.Context) (kv.Store, error) {
	cfg, prefix, err := c.ClientConfig()
	if err != nil {
		return nil, err
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to etcd: %w", err)
	}
	return New(client, prefix, true), nil
}

// New returns a store keeping its data under prefix. When owned, Close also
// closes client.
func New(client *clientv3.Client, prefix string, owned bool) *remotekv.Store {
	return remotekv.New(&backend{
		client: client,
		owned:  owned,
		head:   prefix + "/head",
		data:   prefix + "/k/",
	})
}

type backend struct {
	client *clientv3.Client
	owned  bool
	head   string
	data   string
}

func (b *backend) Close() error {
	if b.owned {
		return b.client.Close()
	}
	return nil
}

func (b *backend) Revision(ctx context.Context) (uint64, error) {
	resp, err := b.client.Get(ctx, b.head)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	return uint64(resp.Kvs[0].ModRevision), nil
}

func (b *backend) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := b.client.Get(ctx, b.data+key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, kv.ErrNotFound
	}
	return resp.Kvs[0].Value, nil
}

func (b *backend) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	const page = 256
	start := b.data + prefix
	if after >= prefix && after != "" {
		start = b.data + after + "\x00"
	}
	end := clientv3.GetPrefixRangeEnd(b.data + prefix)
	for {
		resp, err := b.client.Get(ctx, start, clientv3.WithRange(end), clientv3.WithLimit(page),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return err
		}
		for _, item := range resp.Kvs {
			more, err := fn(strings.TrimPrefix(string(item.Key), b.data), item.Value)
			if err != nil || !more {
				return err
			}
		}
		if !resp.More || len(resp.Kvs) == 0 {
			return nil
		}
		start = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
}

func (b *backend) Commit(ctx context.Context, revision uint64, writes []kv.Write) error {
	ops := []clientv3.Op{clientv3.OpPut(b.head, strconv.FormatInt(time.Now().UnixNano(), 10))}
	for i := 0; i < len(writes); i += opsPerTxn {
		chunk := writes[i:min(i+opsPerTxn, len(writes))]
		nested := make([]clientv3.Op, 0, len(chunk))
		for _, write := range chunk {
			if write.Delete {
				nested = append(nested, clientv3.OpDelete(b.data+write.Key))
			} else {
				nested = append(nested, clientv3.OpPut(b.data+write.Key, string(write.Value)))
			}
		}
		ops = append(ops, clientv3.OpTxn(nil, nested, nil))
	}
	if len(ops) > opsPerTxn {
		return fmt.Errorf("etcd metadata commit of %d writes exceeds the supported transaction size", len(writes))
	}
	resp, err := b.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(b.head), "=", int64(revision))).
		Then(ops...).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return kv.ErrConflict
	}
	return nil
}
