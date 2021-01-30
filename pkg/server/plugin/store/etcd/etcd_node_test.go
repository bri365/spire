package etcd

import (
	"time"

	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

func (s *PluginSuite) TestCreateAttestedNode() {
	node := &common.AttestedNode{
		SpiffeId:            "foo",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	cresp, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node})
	s.Require().NoError(err)
	s.AssertProtoEqual(node, cresp.Node)

	fresp, err := s.shim.FetchAttestedNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: node.SpiffeId})
	s.Require().NoError(err)
	s.AssertProtoEqual(node, fresp.Node)

	expiration := time.Now().Unix()
	sresp, err := s.shim.ListAttestedNodes(ctx, &datastore.ListAttestedNodesRequest{
		ByExpiresBefore: &wrapperspb.Int64Value{
			Value: expiration,
		},
	})
	s.Require().NoError(err)
	s.Empty(sresp.Nodes)
}

func (s *PluginSuite) TestFetchAttestedNodeMissing() {
	fresp, err := s.shim.FetchAttestedNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: "missing"})
	s.Require().NoError(err)
	s.Require().Nil(fresp.Node)
}

func (s *PluginSuite) TestFetchStaleNodes() {
	efuture := &common.AttestedNode{
		SpiffeId:            "foo",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	epast := &common.AttestedNode{
		SpiffeId:            "bar",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "deadbeef",
		CertNotAfter:        time.Now().Add(-time.Hour).Unix(),
	}

	_, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: efuture})
	s.Require().NoError(err)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: epast})
	s.Require().NoError(err)

	expiration := time.Now().Unix()
	sresp, err := s.shim.ListAttestedNodes(ctx, &datastore.ListAttestedNodesRequest{
		ByExpiresBefore: &wrapperspb.Int64Value{
			Value: expiration,
		},
	})
	s.Require().NoError(err)
	s.T().Logf("expected: %v", epast)
	s.T().Logf("received: %v", sresp.Nodes)
	s.RequireProtoListEqual([]*common.AttestedNode{epast}, sresp.Nodes)
}
