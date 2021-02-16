package etcd

import (
	"time"

	ss "github.com/spiffe/spire/pkg/server/store"
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
		`,
	}

	s.st = s.newPlugin(cfg)
	s.shim = ss.New(nil, s.st, s.shim.Log)
	time.Sleep(4 * time.Second)
	s.Require().NoError(nil)
}
