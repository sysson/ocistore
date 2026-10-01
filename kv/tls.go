package kv

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// TLSConfig is client TLS for a networked driver. CAFile replaces the system
// roots; CertFile and KeyFile, set together, enable client authentication.
type TLSConfig struct {
	CAFile     string `json:"caFile,omitempty"`
	CertFile   string `json:"certFile,omitempty"`
	KeyFile    string `json:"keyFile,omitempty"`
	ServerName string `json:"serverName,omitempty"`
}

func (c *TLSConfig) Validate() error {
	if c == nil {
		return nil
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errors.New("tls.certFile and tls.keyFile must be set together")
	}
	return nil
}

// ClientConfig loads the files and returns the TLS client configuration, or
// nil when c is nil.
func (c *TLSConfig) ClientConfig() (*tls.Config, error) {
	if c == nil {
		return nil, nil
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", c.CAFile)
		}
		config.RootCAs = pool
	}
	if c.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}
