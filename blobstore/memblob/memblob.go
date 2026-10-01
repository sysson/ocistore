package memblob

import (
	"context"

	"github.com/sysson/ocistore/blobstore"
	_ "gocloud.dev/blob/memblob"
)

type Config struct{}

func (Config) Validate() error { return nil }
func (Config) URL() string     { return "mem://" }

func (c Config) Open(ctx context.Context) (*blobstore.Store, error) {
	return blobstore.Open(ctx, c.URL())
}
