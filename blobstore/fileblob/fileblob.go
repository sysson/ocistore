package fileblob

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/sysson/ocistore/blobstore"
	_ "gocloud.dev/blob/fileblob"
)

// Config stores blobs in a local directory, creating it with mode 0750.
type Config struct {
	Path string `json:"path"`
}

func (c Config) Validate() error {
	if c.Path == "" {
		return errors.New("file.path must not be empty")
	}
	if !filepath.IsAbs(c.Path) {
		return fmt.Errorf("file.path %q must be absolute", c.Path)
	}
	return nil
}

func (c Config) URL() string {
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Clean(c.Path))}
	u.RawQuery = url.Values{
		"create_dir":    {"true"},
		"dir_file_mode": {"488"},
		"no_tmp_dir":    {"true"},
	}.Encode()
	return u.String()
}

func (c Config) Open(ctx context.Context) (*blobstore.Store, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return blobstore.Open(ctx, c.URL())
}

// Open opens a local blob store at path, creating the directory if necessary.
func Open(ctx context.Context, path string) (*blobstore.Store, error) {
	return (Config{Path: path}).Open(ctx)
}
