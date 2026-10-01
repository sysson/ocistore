package gcsblob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/sysson/ocistore/blobstore"
	_ "gocloud.dev/blob/gcsblob"
)

// Config stores blobs in Google Cloud Storage.
type Config struct {
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	Anonymous bool   `json:"anonymous,omitempty"`
}

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)

func (c Config) Validate() error {
	var errs []error
	if !bucketPattern.MatchString(c.Bucket) {
		errs = append(errs, fmt.Errorf("gcs.bucket %q is not a valid bucket name", c.Bucket))
	}
	if strings.HasPrefix(c.Prefix, "/") || strings.Contains(c.Prefix, "//") {
		errs = append(errs, fmt.Errorf("gcs.prefix %q must be relative and must not contain empty segments", c.Prefix))
	}
	return errors.Join(errs...)
}

func (c Config) URL() string {
	query := url.Values{}
	if c.Prefix != "" {
		prefix := c.Prefix
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		query.Set("prefix", prefix)
	}
	if c.Anonymous {
		query.Set("anonymous", "true")
	}
	return (&url.URL{Scheme: "gs", Host: c.Bucket, RawQuery: query.Encode()}).String()
}

func (c Config) Open(ctx context.Context) (*blobstore.Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return blobstore.Open(ctx, c.URL())
}
