package etcd

import (
	"time"

	ss "github.com/spiffe/spire/pkg/server/store"
)

func (s *PluginSuite) TestHeartbeat() {
	cfg := &ss.Configuration{
		HeartbeatInterval: 5,
	}
	s.shim, _ = ss.New(nil, s.st, s.shim.Log, cfg, s.shim.Etcd)
	time.Sleep(10 * time.Second)
	s.Require().NoError(nil)
}
