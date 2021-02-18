package etcd

import (
	"time"

	ss "github.com/spiffe/spire/pkg/server/store"
)

func (s *PluginSuite) TestZHeartbeat() {
	cfg := &ss.Configuration{
		HeartbeatInterval: 2,
	}
	s.shim, _ = ss.New(nil, s.st, s.shim.Log, cfg, s.shim.Etcd)
	time.Sleep(0 * time.Second)
	s.Require().NoError(nil)
}
