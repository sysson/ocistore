package query

import (
	"context"

	"github.com/sysson/ocistore"
)

func (m *Manifest) Closure(ctx context.Context) ([]*ClosureEntry, error) {
	entries, err := m.root.index.Closure(ctx, m.repository, m.record.Descriptor.Digest)
	if err != nil {
		return nil, err
	}
	result := make([]*ClosureEntry, len(entries))
	for i, entry := range entries {
		result[i] = &ClosureEntry{root: m.root, entry: entry}
	}
	return result, nil
}

// ClosureEntry is the GraphQL ClosureEntry type.
type ClosureEntry struct {
	root  *resolver
	entry ocistore.ClosureEntry
}

func (c *ClosureEntry) Digest() string             { return string(c.entry.Descriptor.Digest) }
func (c *ClosureEntry) Role() string               { return string(c.entry.Role) }
func (c *ClosureEntry) MediaType() string          { return c.entry.Descriptor.MediaType }
func (c *ClosureEntry) Size() int64                { return c.entry.Descriptor.Size }
func (c *ClosureEntry) RetainedInRepository() bool { return c.entry.RetainedInRepository }
func (c *ClosureEntry) Shared() bool               { return c.entry.Shared }
func (c *ClosureEntry) SharedWith() []*Dependent   { return c.root.dependentModels(c.entry.SharedWith) }
func (c *ClosureEntry) Locations() []*Location     { return locationModels(c.entry.Locations) }
