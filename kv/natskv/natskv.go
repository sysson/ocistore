// Package natskv is a kv.Store backed by a NATS JetStream stream.
//
// The stream keeps the last message per subject, one subject per key. Each
// commit is an atomic batch publish whose first message advances a head
// subject with an expected-last-sequence check, so a commit is applied
// completely or not at all. It requires NATS server 2.12 or later.
package natskv

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/synadia-io/orbit.go/jetstreamext"
	"github.com/sysson/ocistore/kv"
	"github.com/sysson/ocistore/kv/remotekv"
)

const (
	opHeader = "Ocistore-Op"
	opDelete = "DEL"
	// tombstoneTTL bounds how long delete markers are retained. Once one
	// expires the key is simply absent, which means the same thing.
	tombstoneTTL = "10m"
	// maxBatch stays below the server's default atomic batch size limit.
	maxBatch = 1000
	getChunk = 128
)

// Options configure the stream a store keeps its data in. Zero values use
// the defaults: stream OCISTORE_METADATA, subject ocistore.metadata, one replica.
type Options struct {
	Stream   string `json:"stream,omitempty"`
	Subject  string `json:"subject,omitempty"`
	Replicas int    `json:"replicas,omitempty"`
}

// Config selects the NATS driver.
type Config struct {
	// Servers are nats://host:port (or tls://) URLs or bare host:port
	// addresses of the cluster.
	Servers []string `json:"servers"`
	Options
	// CredentialsFile is a NATS .creds file (JWT and NKey seed).
	CredentialsFile string        `json:"credentialsFile,omitempty"`
	TLS             *kv.TLSConfig `json:"tls,omitempty"`
}

var _ kv.Config = Config{}

func (c Config) Validate() error {
	var errs []error
	if len(c.Servers) == 0 {
		errs = append(errs, errors.New("nats.servers must not be empty"))
	}
	for _, server := range c.Servers {
		if _, err := serverURL(server); err != nil {
			errs = append(errs, err)
		}
	}
	if c.Stream != "" && strings.ContainsAny(c.Stream, " \t\r\n.*>/\\") {
		errs = append(errs, fmt.Errorf("nats.stream %q must not contain whitespace, '.', '*', '>', '/' or '\\'", c.Stream))
	}
	if c.Subject != "" {
		for token := range strings.SplitSeq(c.Subject, ".") {
			if token == "" || strings.ContainsAny(token, " \t\r\n*>") {
				errs = append(errs, fmt.Errorf("nats.subject %q must be dot-separated literal tokens", c.Subject))
				break
			}
		}
	}
	if c.Replicas < 0 || c.Replicas > 5 {
		errs = append(errs, fmt.Errorf("nats.replicas %d must be between 1 and 5", c.Replicas))
	}
	if err := c.TLS.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("nats.%w", err))
	}
	return errors.Join(errs...)
}

func serverURL(server string) (string, error) {
	if server == "" {
		return "", errors.New("nats.servers must not contain empty entries")
	}
	if !strings.Contains(server, "://") {
		server = "nats://" + server
	}
	u, err := url.Parse(server)
	if err != nil {
		return "", fmt.Errorf("nats server %q: %w", server, err)
	}
	if u.Scheme != "nats" && u.Scheme != "tls" {
		return "", fmt.Errorf("nats server %q must use nats or tls", server)
	}
	if u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return "", fmt.Errorf("nats server %q must be scheme://[user:password@]host:port", server)
	}
	u.Path = ""
	return u.String(), nil
}

func (c Config) Open(ctx context.Context) (kv.Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	servers := make([]string, 0, len(c.Servers))
	for _, server := range c.Servers {
		normalized, _ := serverURL(server)
		servers = append(servers, normalized)
	}
	natsOpts := []nats.Option{nats.Name("ocistore"), nats.MaxReconnects(-1)}
	if c.CredentialsFile != "" {
		natsOpts = append(natsOpts, nats.UserCredentials(c.CredentialsFile))
	}
	tlsConfig, err := c.TLS.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("nats tls: %w", err)
	}
	if tlsConfig != nil {
		natsOpts = append(natsOpts, nats.Secure(tlsConfig))
	}
	nc, err := nats.Connect(strings.Join(servers, ","), natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}
	store, err := New(ctx, nc, c.Options)
	if err != nil {
		nc.Close()
		return nil, err
	}
	store.OnClose(nc.Close)
	return store, nil
}

// New creates or updates the metadata stream and returns a store over it.
// The connection is not closed by the store.
func New(ctx context.Context, nc *nats.Conn, opts Options) (*remotekv.Store, error) {
	if opts.Stream == "" {
		opts.Stream = "OCISTORE_METADATA"
	}
	if opts.Subject == "" {
		opts.Subject = "ocistore.metadata"
	}
	if opts.Replicas == 0 {
		opts.Replicas = 1
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:               opts.Stream,
		Description:        "ocistore registry metadata",
		Subjects:           []string{opts.Subject + ".>"},
		MaxMsgsPerSubject:  1,
		Discard:            jetstream.DiscardOld,
		Storage:            jetstream.FileStorage,
		Replicas:           opts.Replicas,
		AllowDirect:        true,
		AllowRollup:        true,
		AllowMsgTTL:        true,
		AllowAtomicPublish: true,
	})
	if err != nil {
		return nil, fmt.Errorf("creating NATS metadata stream %s: %w", opts.Stream, err)
	}
	return remotekv.New(&backend{
		js:     js,
		stream: stream,
		name:   opts.Stream,
		head:   opts.Subject + ".head",
		data:   opts.Subject + ".k",
	}), nil
}

type backend struct {
	js     jetstream.JetStream
	stream jetstream.Stream
	name   string
	head   string
	data   string
}

func (b *backend) Close() error { return nil }

func (b *backend) Revision(ctx context.Context) (uint64, error) {
	msg, err := b.stream.GetLastMsgForSubject(ctx, b.head)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return msg.Sequence, nil
}

func (b *backend) Get(ctx context.Context, key string) ([]byte, error) {
	msg, err := b.stream.GetLastMsgForSubject(ctx, b.subject(key))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, kv.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if msg.Header.Get(opHeader) == opDelete {
		return nil, kv.ErrNotFound
	}
	return msg.Data, nil
}

func (b *backend) Scan(ctx context.Context, prefix, after string, fn func(string, []byte) (bool, error)) error {
	filter := b.data + ".>"
	if prefix != "" {
		filter = b.subject(strings.TrimSuffix(prefix, kv.Separator)) + ".>"
	}
	info, err := b.stream.Info(ctx, jetstream.WithSubjectFilter(filter))
	if err != nil {
		return err
	}
	type item struct {
		key     string
		subject string
	}
	items := make([]item, 0, len(info.State.Subjects))
	for subject := range info.State.Subjects {
		key, err := b.key(subject)
		if err != nil {
			return err
		}
		if strings.HasPrefix(key, prefix) && key > after {
			items = append(items, item{key, subject})
		}
	}
	slices.SortFunc(items, func(a, c item) int { return strings.Compare(a.key, c.key) })

	for i := 0; i < len(items); i += getChunk {
		chunk := items[i:min(i+getChunk, len(items))]
		subjects := make([]string, len(chunk))
		for j, it := range chunk {
			subjects[j] = it.subject
		}
		msgs, err := jetstreamext.GetLastMsgsFor(ctx, b.js, b.name, subjects)
		if err != nil {
			return err
		}
		values := make(map[string][]byte, len(chunk))
		for msg, err := range msgs {
			if err != nil {
				if errors.Is(err, jetstreamext.ErrNoMessages) {
					break
				}
				return err
			}
			if msg.Header.Get(opHeader) == opDelete {
				continue
			}
			values[msg.Subject] = msg.Data
		}
		for _, it := range chunk {
			value, ok := values[it.subject]
			if !ok {
				continue
			}
			more, err := fn(it.key, value)
			if err != nil || !more {
				return err
			}
		}
	}
	return nil
}

func (b *backend) Commit(ctx context.Context, revision uint64, writes []kv.Write) error {
	if len(writes)+1 > maxBatch {
		return fmt.Errorf("NATS metadata commit of %d writes exceeds the atomic batch limit", len(writes))
	}
	head := nats.NewMsg(b.head)
	head.Data = []byte(strconv.FormatInt(time.Now().UnixNano(), 10))
	head.Header.Set(jetstream.ExpectedLastSubjSeqHeader, strconv.FormatUint(revision, 10))
	msgs := []*nats.Msg{head}
	for _, write := range writes {
		msg := nats.NewMsg(b.subject(write.Key))
		if write.Delete {
			msg.Header.Set(opHeader, opDelete)
			msg.Header.Set(jetstream.MsgTTLHeader, tombstoneTTL)
		} else {
			msg.Data = write.Value
		}
		msgs = append(msgs, msg)
	}
	_, err := jetstreamext.PublishMsgBatch(ctx, b.js, msgs)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
			apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
			return kv.ErrConflict
		}
		return err
	}
	return nil
}

func (b *backend) subject(key string) string {
	parts := kv.Split(key)
	var s strings.Builder
	s.WriteString(b.data)
	for _, part := range parts {
		s.WriteByte('.')
		s.WriteString(encodeToken(part))
	}
	return s.String()
}

func (b *backend) key(subject string) (string, error) {
	rest, ok := strings.CutPrefix(subject, b.data+".")
	if !ok {
		return "", fmt.Errorf("unexpected metadata subject %q", subject)
	}
	tokens := strings.Split(rest, ".")
	parts := make([]string, len(tokens))
	for i, token := range tokens {
		part, err := decodeToken(token)
		if err != nil {
			return "", err
		}
		parts[i] = part
	}
	return kv.Key(parts...), nil
}

const hexDigits = "0123456789ABCDEF"

func safeByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '-' || c == '_' || c == ':' || c == '=' || c == '+' || c == '@' || c == ','
}

// encodeToken maps a key part to a subject token, escaping bytes that are
// not allowed or significant in subjects. The empty part becomes "%".
func encodeToken(part string) string {
	if part == "" {
		return "%"
	}
	var s strings.Builder
	for i := 0; i < len(part); i++ {
		c := part[i]
		if safeByte(c) {
			s.WriteByte(c)
			continue
		}
		s.WriteByte('%')
		s.WriteByte(hexDigits[c>>4])
		s.WriteByte(hexDigits[c&0xf])
	}
	return s.String()
}

func decodeToken(token string) (string, error) {
	if token == "%" {
		return "", nil
	}
	var s strings.Builder
	for i := 0; i < len(token); i++ {
		if token[i] != '%' {
			s.WriteByte(token[i])
			continue
		}
		if i+2 >= len(token) {
			return "", fmt.Errorf("invalid metadata subject token %q", token)
		}
		v, err := strconv.ParseUint(token[i+1:i+3], 16, 8)
		if err != nil {
			return "", fmt.Errorf("invalid metadata subject token %q", token)
		}
		s.WriteByte(byte(v))
		i += 2
	}
	return s.String(), nil
}
