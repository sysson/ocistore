package azureblob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/sysson/ocistore/blobstore"
	_ "gocloud.dev/blob/azureblob"
)

// Config stores blobs in an Azure Blob Storage container.
type Config struct {
	Container     string `json:"container"`
	Prefix        string `json:"prefix,omitempty"`
	AccountName   string `json:"accountName,omitempty"`
	Domain        string `json:"domain,omitempty"`
	Protocol      string `json:"protocol,omitempty"`
	LocalEmulator bool   `json:"localEmulator,omitempty"`
}

var containerPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9]|-[a-z0-9]){2,62}$`)

func (c Config) Validate() error {
	var errs []error
	if !containerPattern.MatchString(c.Container) {
		errs = append(errs, fmt.Errorf("azure.container %q is not a valid container name", c.Container))
	}
	if strings.HasPrefix(c.Prefix, "/") || strings.Contains(c.Prefix, "//") {
		errs = append(errs, fmt.Errorf("azure.prefix %q must be relative and must not contain empty segments", c.Prefix))
	}
	if c.Protocol != "" && c.Protocol != "http" && c.Protocol != "https" {
		errs = append(errs, fmt.Errorf("azure.protocol %q must be http or https", c.Protocol))
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
	for key, value := range map[string]string{"storage_account": c.AccountName, "domain": c.Domain, "protocol": c.Protocol} {
		if value != "" {
			query.Set(key, value)
		}
	}
	if c.LocalEmulator {
		query.Set("localemu", "true")
	}
	return (&url.URL{Scheme: "azblob", Host: c.Container, RawQuery: query.Encode()}).String()
}

func (c Config) Open(ctx context.Context) (*blobstore.Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return blobstore.Open(ctx, c.URL())
}
