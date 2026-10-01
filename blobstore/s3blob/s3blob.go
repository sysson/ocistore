package s3blob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/sysson/ocistore/blobstore"
	_ "gocloud.dev/blob/s3blob"
)

// Config stores blobs in an S3 or S3-compatible bucket.
type Config struct {
	Bucket       string `json:"bucket"`
	Prefix       string `json:"prefix,omitempty"`
	Region       string `json:"region,omitempty"`
	Endpoint     string `json:"endpoint,omitempty"`
	UsePathStyle bool   `json:"usePathStyle,omitempty"`
	DisableHTTPS bool   `json:"disableHTTPS,omitempty"`
	Profile      string `json:"profile,omitempty"`
	Anonymous    bool   `json:"anonymous,omitempty"`
	SSEType      string `json:"sseType,omitempty"`
	KMSKeyID     string `json:"kmsKeyID,omitempty"`
}

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

func (c Config) Validate() error {
	var errs []error
	if !bucketPattern.MatchString(c.Bucket) || strings.Contains(c.Bucket, "..") {
		errs = append(errs, fmt.Errorf("s3.bucket %q is not a valid bucket name", c.Bucket))
	}
	if strings.HasPrefix(c.Prefix, "/") || strings.Contains(c.Prefix, "//") {
		errs = append(errs, fmt.Errorf("s3.prefix %q must be relative and must not contain empty segments", c.Prefix))
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("s3.endpoint %q must be an http or https URL", c.Endpoint))
		}
	}
	switch c.SSEType {
	case "", "AES256":
		if c.KMSKeyID != "" {
			errs = append(errs, errors.New("s3.kmsKeyID requires s3.sseType aws:kms or aws:kms:dsse"))
		}
	case "aws:kms", "aws:kms:dsse":
	default:
		errs = append(errs, fmt.Errorf("s3.sseType %q must be AES256, aws:kms, or aws:kms:dsse", c.SSEType))
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
	for key, value := range map[string]string{"region": c.Region, "endpoint": c.Endpoint, "profile": c.Profile, "ssetype": c.SSEType, "kmskeyid": c.KMSKeyID} {
		if value != "" {
			query.Set(key, value)
		}
	}
	for key, enabled := range map[string]bool{"use_path_style": c.UsePathStyle, "disable_https": c.DisableHTTPS, "anonymous": c.Anonymous} {
		if enabled {
			query.Set(key, "true")
		}
	}
	return (&url.URL{Scheme: "s3", Host: c.Bucket, RawQuery: query.Encode()}).String()
}

func (c Config) Open(ctx context.Context) (*blobstore.Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return blobstore.Open(ctx, c.URL())
}
