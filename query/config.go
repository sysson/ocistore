package query

import (
	"slices"
	"time"

	"github.com/docker/oci"
)

// imageConfig is the subset of the OCI/Docker image config the schema exposes.
type imageConfig struct {
	Created      *time.Time `json:"created,omitempty"`
	Author       string     `json:"author,omitempty"`
	Architecture string     `json:"architecture,omitempty"`
	OS           string     `json:"os,omitempty"`
	Variant      string     `json:"variant,omitempty"`
	Config       struct {
		User         string              `json:"User,omitempty"`
		WorkingDir   string              `json:"WorkingDir,omitempty"`
		Env          []string            `json:"Env,omitempty"`
		Entrypoint   []string            `json:"Entrypoint,omitempty"`
		Cmd          []string            `json:"Cmd,omitempty"`
		Labels       map[string]string   `json:"Labels,omitempty"`
		ExposedPorts map[string]struct{} `json:"ExposedPorts,omitempty"`
		Volumes      map[string]struct{} `json:"Volumes,omitempty"`
		StopSignal   string              `json:"StopSignal,omitempty"`
	} `json:"config"`
	RootFS struct {
		DiffIDs []string `json:"diff_ids,omitempty"`
	} `json:"rootfs"`
	History []imageHistory `json:"history,omitempty"`
}

type imageHistory struct {
	Created    *time.Time `json:"created,omitempty"`
	CreatedBy  string     `json:"created_by,omitempty"`
	Author     string     `json:"author,omitempty"`
	Comment    string     `json:"comment,omitempty"`
	EmptyLayer bool       `json:"empty_layer,omitempty"`
}

// ImageConfig is the GraphQL ImageConfig type.
type ImageConfig struct {
	digest oci.Digest
	config imageConfig
	raw    []byte
}

func optionalTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	return optionalTime(*value)
}

func keys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

func (c *ImageConfig) Digest() string         { return string(c.digest) }
func (c *ImageConfig) Created() *time.Time    { return optionalTimePtr(c.config.Created) }
func (c *ImageConfig) Author() *string        { return optional(c.config.Author) }
func (c *ImageConfig) Architecture() *string  { return optional(c.config.Architecture) }
func (c *ImageConfig) Os() *string            { return optional(c.config.OS) }
func (c *ImageConfig) Variant() *string       { return optional(c.config.Variant) }
func (c *ImageConfig) User() *string          { return optional(c.config.Config.User) }
func (c *ImageConfig) WorkingDir() *string    { return optional(c.config.Config.WorkingDir) }
func (c *ImageConfig) Env() []string          { return nonNil(c.config.Config.Env) }
func (c *ImageConfig) Entrypoint() []string   { return nonNil(c.config.Config.Entrypoint) }
func (c *ImageConfig) Cmd() []string          { return nonNil(c.config.Config.Cmd) }
func (c *ImageConfig) ExposedPorts() []string { return keys(c.config.Config.ExposedPorts) }
func (c *ImageConfig) Volumes() []string      { return keys(c.config.Config.Volumes) }
func (c *ImageConfig) StopSignal() *string    { return optional(c.config.Config.StopSignal) }
func (c *ImageConfig) DiffIds() []string      { return nonNil(c.config.RootFS.DiffIDs) }
func (c *ImageConfig) Raw() string            { return string(c.raw) }
func (c *ImageConfig) Labels() []*Annotation {
	return annotations(c.config.Config.Labels)
}

func (c *ImageConfig) History() []*History {
	result := make([]*History, len(c.config.History))
	for i := range c.config.History {
		result[i] = &History{history: c.config.History[i]}
	}
	return result
}

// History is the GraphQL History type.
type History struct{ history imageHistory }

func (h *History) Created() *time.Time { return optionalTimePtr(h.history.Created) }
func (h *History) CreatedBy() *string  { return optional(h.history.CreatedBy) }
func (h *History) Author() *string     { return optional(h.history.Author) }
func (h *History) Comment() *string    { return optional(h.history.Comment) }
func (h *History) EmptyLayer() bool    { return h.history.EmptyLayer }
