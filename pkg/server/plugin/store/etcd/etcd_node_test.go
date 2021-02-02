package etcd

import (
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func cloneAttestedNode(aNode *common.AttestedNode) *common.AttestedNode {
	return proto.Clone(aNode).(*common.AttestedNode)
}

func (s *PluginSuite) getNodeSelectors(spiffeID string, tolerateStale bool) []*common.Selector {
	resp, err := s.shim.GetNodeSelectors(ctx, &datastore.GetNodeSelectorsRequest{
		SpiffeId:      spiffeID,
		TolerateStale: tolerateStale,
	})
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.Require().NotNil(resp.Selectors)
	s.Require().Equal(spiffeID, resp.Selectors.SpiffeId)
	return resp.Selectors.Selectors
}

func (s *PluginSuite) listNodeSelectors(req *datastore.ListNodeSelectorsRequest) *datastore.ListNodeSelectorsResponse {
	resp, err := s.shim.ListNodeSelectors(ctx, req)
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	return resp
}

func (s *PluginSuite) setNodeSelectors(spiffeID string, selectors []*common.Selector) {
	resp, err := s.shim.SetNodeSelectors(ctx, &datastore.SetNodeSelectorsRequest{
		Selectors: &datastore.NodeSelectors{
			SpiffeId:  spiffeID,
			Selectors: selectors,
		},
	})
	s.Require().NoError(err)
	s.RequireProtoEqual(&datastore.SetNodeSelectorsResponse{}, resp)
}

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
	s.RequireProtoListEqual([]*common.AttestedNode{epast}, sresp.Nodes)
}

func (s *PluginSuite) TestFetchAttestedNodesWithPagination() {
	// Create all necessary nodes
	aNode1 := &common.AttestedNode{
		SpiffeId:            "node1",
		AttestationDataType: "t1",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(-time.Hour).Unix(),
	}

	aNode2 := &common.AttestedNode{
		SpiffeId:            "node2",
		AttestationDataType: "t2",
		CertSerialNumber:    "deadbeef",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	aNode3 := &common.AttestedNode{
		SpiffeId:            "node3",
		AttestationDataType: "t3",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(-time.Hour).Unix(),
	}

	aNode4 := &common.AttestedNode{
		SpiffeId:            "node4",
		AttestationDataType: "t1",
		// Banned
		CertSerialNumber: "",
		CertNotAfter:     time.Now().Add(-time.Hour).Unix(),
	}
	aNode5 := &common.AttestedNode{
		SpiffeId:            "node5",
		AttestationDataType: "t4",
		// Banned
		CertSerialNumber: "",
		CertNotAfter:     time.Now().Add(-time.Hour).Unix(),
	}

	_, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: aNode1})
	s.Require().NoError(err)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: aNode2})
	s.Require().NoError(err)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: aNode3})
	s.Require().NoError(err)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: aNode4})
	s.Require().NoError(err)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: aNode5})
	s.Require().NoError(err)

	aNode1WithSelectors := cloneAttestedNode(aNode1)
	aNode1WithSelectors.Selectors = []*common.Selector{
		{Type: "a", Value: "1"},
		{Type: "b", Value: "2"},
	}
	s.setNodeSelectors("node1", aNode1WithSelectors.Selectors)

	aNode2WithSelectors := cloneAttestedNode(aNode2)
	aNode2WithSelectors.Selectors = []*common.Selector{
		{Type: "b", Value: "2"},
	}
	s.setNodeSelectors("node2", aNode2WithSelectors.Selectors)

	aNode3WithSelectors := cloneAttestedNode(aNode3)
	aNode3WithSelectors.Selectors = []*common.Selector{
		{Type: "a", Value: "1"},
		{Type: "c", Value: "3"},
	}
	s.setNodeSelectors("node3", aNode3WithSelectors.Selectors)

	aNode4WithSelectors := cloneAttestedNode(aNode4)
	aNode4WithSelectors.Selectors = []*common.Selector{
		{Type: "a", Value: "1"},
		{Type: "b", Value: "2"},
	}
	s.setNodeSelectors("node4", aNode4WithSelectors.Selectors)

	tests := []struct {
		name               string
		req                *datastore.ListAttestedNodesRequest
		expectedList       []*common.AttestedNode
		expectedPagination *datastore.Pagination
		expectedErr        string
	}{
		{
			name:         "fetch without pagination",
			req:          &datastore.ListAttestedNodesRequest{},
			expectedList: []*common.AttestedNode{aNode1, aNode2, aNode3, aNode4, aNode5},
		},
		{
			name: "pagination without token",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					PageSize: 2,
				},
			},
			expectedList: []*common.AttestedNode{aNode1, aNode2},
			expectedPagination: &datastore.Pagination{
				Token:    "2",
				PageSize: 2,
			},
		},
		{
			name: "pagination without token and fetch selectors",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					PageSize: 3,
				},
				FetchSelectors: true,
			},
			expectedList: []*common.AttestedNode{
				aNode1WithSelectors, aNode2WithSelectors, aNode3WithSelectors,
			},
			expectedPagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 3,
			},
		},
		{
			name: "list without pagination and fetch selectors",
			req: &datastore.ListAttestedNodesRequest{
				FetchSelectors: true,
			},
			expectedList: []*common.AttestedNode{
				aNode1WithSelectors, aNode2WithSelectors, aNode3WithSelectors,
				aNode4WithSelectors, aNode5,
			},
		},
		{
			name: "pagination not null but page size is zero",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 0,
				},
			},
			expectedErr: "rpc error: code = InvalidArgument desc = cannot paginate with pagesize = 0",
		},
		{
			name: "by selector match but empty selectors",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 2,
				},
				BySelectorMatch: &datastore.BySelectors{
					Selectors: []*common.Selector{},
				},
			},
			expectedErr: "rpc error: code = InvalidArgument desc = cannot list by empty selectors set",
		},
		{
			name: "get all nodes first page",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 3,
				},
			},
			expectedList: []*common.AttestedNode{aNode1, aNode2, aNode3},
			expectedPagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 3,
			},
		},
		{
			name: "get all nodes second page",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "3",
					PageSize: 3,
				},
			},
			expectedList: []*common.AttestedNode{aNode4, aNode5},
			expectedPagination: &datastore.Pagination{
				Token:    "5",
				PageSize: 3,
			},
		},
		{
			name:         "get all nodes third page no results",
			expectedList: []*common.AttestedNode{},
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "5",
					PageSize: 3,
				},
			},
			expectedPagination: &datastore.Pagination{
				PageSize: 3,
			},
		},
		{
			name: "get nodes by expire no pagination",
			req: &datastore.ListAttestedNodesRequest{
				ByExpiresBefore: &wrapperspb.Int64Value{
					Value: time.Now().Unix(),
				},
			},
			expectedList: []*common.AttestedNode{aNode1, aNode3, aNode4, aNode5},
		},
		{
			name: "get nodes by expire before get only page first page",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 2,
				},
				ByExpiresBefore: &wrapperspb.Int64Value{
					Value: time.Now().Unix(),
				},
			},
			expectedList: []*common.AttestedNode{aNode1, aNode3},
			expectedPagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 2,
			},
		},
		{
			name: "get nodes by expire before get only page second page",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "3",
					PageSize: 2,
				},
				ByExpiresBefore: &wrapperspb.Int64Value{
					Value: time.Now().Unix(),
				},
			},
			expectedList: []*common.AttestedNode{aNode4, aNode5},
			expectedPagination: &datastore.Pagination{
				Token:    "5",
				PageSize: 2,
			},
		},
		{
			name: "get nodes by expire before get only page third page no results",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "5",
					PageSize: 2,
				},
				ByExpiresBefore: &wrapperspb.Int64Value{
					Value: time.Now().Unix(),
				},
			},
			expectedList: []*common.AttestedNode{},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
			},
		},
		{
			name: "by attestation type",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 3,
				},
				ByAttestationType: "t1",
			},
			expectedList: []*common.AttestedNode{aNode1, aNode4},
			expectedPagination: &datastore.Pagination{
				PageSize: 3,
				Token:    "4",
			},
		},
		{
			name: "by attestation type no pagination",
			req: &datastore.ListAttestedNodesRequest{
				ByAttestationType: "t1",
			},
			expectedList: []*common.AttestedNode{aNode1, aNode4},
		},
		{
			name: "by attestation type no results",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 10,
				},
				ByAttestationType: "invalid type",
			},
			expectedList: []*common.AttestedNode{},
			expectedPagination: &datastore.Pagination{
				PageSize: 10,
			},
		},
		{
			name: "not banned",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 4,
				},
				ByBanned: &wrapperspb.BoolValue{Value: false},
			},
			expectedList: []*common.AttestedNode{aNode1, aNode2, aNode3},
			expectedPagination: &datastore.Pagination{
				PageSize: 4,
				Token:    "3",
			},
		},
		{
			name: "not banned no pagination",
			req: &datastore.ListAttestedNodesRequest{
				ByBanned: &wrapperspb.BoolValue{Value: false},
			},
			expectedList: []*common.AttestedNode{aNode1, aNode2, aNode3},
		},
		{
			name: "banned",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 2,
				},
				ByBanned: &wrapperspb.BoolValue{Value: true},
			},
			expectedList: []*common.AttestedNode{aNode4, aNode5},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
				Token:    "5",
			},
		},
		{
			name: "by selector match exact",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 2,
				},
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_EXACT,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{aNode1WithSelectors, aNode4WithSelectors},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
				Token:    "4",
			},
		},
		{
			name: "by selector match exact second page no results",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "4",
					PageSize: 2,
				},
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_EXACT,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
				Token:    "",
			},
		},
		{
			name: "by selector match exact no pagination",
			req: &datastore.ListAttestedNodesRequest{
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_EXACT,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{aNode1WithSelectors, aNode4WithSelectors},
		},
		{
			name: "by selector match subset",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 4,
				},
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_SUBSET,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{aNode1WithSelectors, aNode2WithSelectors, aNode4WithSelectors},
			expectedPagination: &datastore.Pagination{
				PageSize: 4,
				Token:    "4",
			},
		},
		{
			name: "by selector match subset no pagination",
			req: &datastore.ListAttestedNodesRequest{
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_SUBSET,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{aNode1WithSelectors, aNode2WithSelectors, aNode4WithSelectors},
		},
		{
			name: "multiple filters",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "",
					PageSize: 2,
				},
				ByAttestationType: "t1",
				ByBanned:          &wrapperspb.BoolValue{Value: false},
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_EXACT,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{aNode1WithSelectors},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
				Token:    "1",
			},
		},
		{
			name: "multiple filters no pagination",
			req: &datastore.ListAttestedNodesRequest{
				ByAttestationType: "t1",
				ByBanned:          &wrapperspb.BoolValue{Value: false},
				BySelectorMatch: &datastore.BySelectors{
					Match: datastore.BySelectors_MATCH_EXACT,
					Selectors: []*common.Selector{
						{Type: "a", Value: "1"},
						{Type: "b", Value: "2"},
					},
				},
			},
			expectedList: []*common.AttestedNode{aNode1WithSelectors},
		},
	}
	for _, test := range tests {
		test := test
		s.T().Run(test.name, func(t *testing.T) {
			resp, err := s.shim.ListAttestedNodes(ctx, test.req)
			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)

			expectedResponse := &datastore.ListAttestedNodesResponse{
				Nodes:      test.expectedList,
				Pagination: test.expectedPagination,
			}
			spiretest.RequireProtoEqual(t, expectedResponse, resp)
		})
	}

	resp, err := s.shim.ListAttestedNodes(ctx, &datastore.ListAttestedNodesRequest{
		Pagination: &datastore.Pagination{
			Token:    "invalid int",
			PageSize: 10,
		},
	})
	s.Require().Nil(resp)
	s.Require().Error(err, "could not parse token 'invalid int'")
}
