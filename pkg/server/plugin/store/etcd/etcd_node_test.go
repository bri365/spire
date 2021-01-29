package etcd

import (
	"time"

	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
)

func (s *PluginSuite) TestCountAttestedNodes() {
	// Count empty attested nodes
	resp, err := s.shim.CountAttestedNodes(ctx, &datastore.CountAttestedNodesRequest{})
	s.Require().NoError(err)
	spiretest.RequireProtoEqual(s.T(), &datastore.CountAttestedNodesResponse{Nodes: 0}, resp)

	// Create attested nodes
	node := &common.AttestedNode{
		SpiffeId:            "spiffe://example.org/foo",
		AttestationDataType: "t1",
		CertSerialNumber:    "1234",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}
	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node})
	s.Require().NoError(err)

	node2 := &common.AttestedNode{
		SpiffeId:            "spiffe://example.org/bar",
		AttestationDataType: "t2",
		CertSerialNumber:    "5678",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}
	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node2})
	s.Require().NoError(err)

	// Count all
	resp, err = s.shim.CountAttestedNodes(ctx, &datastore.CountAttestedNodesRequest{})
	s.Require().NoError(err)
	spiretest.RequireProtoEqual(s.T(), &datastore.CountAttestedNodesResponse{Nodes: 2}, resp)
}
