package kvtest

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"go.etcd.io/etcd/server/v3/embed"
)

// StartNATS runs an embedded JetStream server for the test and returns its
// client URL.
func StartNATS(t testing.TB) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server did not start")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// StartEtcd runs an embedded single-member etcd server for the test and
// returns its client address (host:port).
func StartEtcd(t testing.TB) string {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	// Shutdown always logs listener/mux "closed" errors; keep them out of test output.
	cfg.LogLevel = "fatal"
	client, _ := url.Parse("http://127.0.0.1:0")
	peer, _ := url.Parse("http://127.0.0.1:0")
	cfg.ListenClientUrls = []url.URL{*client}
	cfg.AdvertiseClientUrls = []url.URL{*client}
	cfg.ListenPeerUrls = []url.URL{*peer}
	cfg.AdvertisePeerUrls = []url.URL{*peer}
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, peer.String())
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(etcd.Close)
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		t.Fatal("etcd did not start")
	}
	return etcd.Clients[0].Addr().String()
}
