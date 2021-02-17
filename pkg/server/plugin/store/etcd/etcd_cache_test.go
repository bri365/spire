package etcd

import (
	"context"
	"fmt"
	"time"

	spi "github.com/spiffe/spire/proto/spire/common/plugin"
)

func (s *PluginSuite) TestZHeartbeat() {
	cfg := &spi.ConfigureRequest{
		Configuration: `
		endpoints = ["192.168.50.181:2379","192.168.50.182:2379","192.168.50.183:2379"]
		root_ca_path = "/Users/brian/Dev/scytale/performance-tests/etcd/tf-etcd-vsphere/certs/ca.pem"
		client_cert_path = "/Users/brian/Dev/scytale/performance-tests/etcd/tf-etcd-vsphere/certs/client.pem"
		client_key_path = "/Users/brian/Dev/scytale/performance-tests/etcd/tf-etcd-vsphere/certs/client-key.pem"
		heartbeat_interval = 3
		write_response_delay = 20
		reset_cache = true
		`,
	}

	// Enable heartbeats
	s.st.Configure(context.TODO(), cfg)

	// Let a few heartbeats happen
	fmt.Printf("Sleeping for a few heartbeats")
	time.Sleep(5 * time.Second)
	s.Require().NoError(nil)
}
