package memkv_test

import (
	"testing"

	"github.com/sysson/ocistore/kv/kvtest"
	"github.com/sysson/ocistore/kv/memkv"
)

func TestConformance(t *testing.T) {
	kvtest.Run(t, memkv.New())
}
