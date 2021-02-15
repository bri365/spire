package etcd

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/common/util"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			name: "fetch without pagination",
			req:  &datastore.ListAttestedNodesRequest{},
			// TODO expectedList: []*common.AttestedNode{aNode1, aNode2, aNode3, aNode4, aNode5},
			expectedList: []*common.AttestedNode{
				aNode1WithSelectors, aNode2WithSelectors, aNode3WithSelectors,
				aNode4WithSelectors, aNode5},
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
				Token:    "N|node2",
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
				Token:    "N|node3",
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
				Token:    "N|node3",
				PageSize: 3,
			},
		},
		{
			name: "get all nodes second page",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "N|node3",
					PageSize: 3,
				},
			},
			expectedList: []*common.AttestedNode{aNode4, aNode5},
			expectedPagination: &datastore.Pagination{
				Token:    "N|node5",
				PageSize: 3,
			},
		},
		{
			name:         "get all nodes third page no results",
			expectedList: []*common.AttestedNode{},
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "N|node5",
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
			name: "get nodes by expire before get only first page",
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
				Token:    "N|node3",
				PageSize: 2,
			},
		},
		{
			name: "get nodes by expire before get only second page",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "N|node3",
					PageSize: 2,
				},
				ByExpiresBefore: &wrapperspb.Int64Value{
					Value: time.Now().Unix(),
				},
			},
			expectedList: []*common.AttestedNode{aNode4, aNode5},
			expectedPagination: &datastore.Pagination{
				Token:    "N|node5",
				PageSize: 2,
			},
		},
		{
			name: "get nodes by expire before get only third page no results",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "N|node5",
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
				Token:    "N|node4",
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
				Token:    "N|node3",
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
				Token:    "N|node5",
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
				Token:    "N|node4",
			},
		},
		{
			name: "by selector match exact second page no results",
			req: &datastore.ListAttestedNodesRequest{
				Pagination: &datastore.Pagination{
					Token:    "N|node4",
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
				Token:    "N|node4",
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
				Token:    "N|node1",
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
			util.SortAttestedNodes(expectedResponse.Nodes)
			util.SortAttestedNodes(resp.Nodes)
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

func (s *PluginSuite) TestUpdateAttestedNode() {
	// Current nodes values
	nodeID := "node1"
	attestationType := "attestation-data-type"
	serial := "cert-serial-number-1"
	expires := int64(1)
	newSerial := "new-cert-serial-number"
	newExpires := int64(2)

	// Updated nodes values
	updatedSerial := "cert-serial-number-2"
	updatedExpires := int64(3)
	updatedNewSerial := ""
	updatedNewExpires := int64(0)

	tests := []struct {
		name           string
		updateReq      *datastore.UpdateAttestedNodeRequest
		expUpdatedNode *common.AttestedNode
		expCode        codes.Code
		expMsg         string
	}{
		{
			name: "update non-existing attested node",
			updateReq: &datastore.UpdateAttestedNodeRequest{
				SpiffeId:         "non-existent-node-id",
				CertSerialNumber: updatedSerial,
				CertNotAfter:     updatedExpires,
			},
			expCode: codes.NotFound,
			expMsg:  _notFoundErrMsg,
		},
		{
			name: "update attested node with all false mask",
			updateReq: &datastore.UpdateAttestedNodeRequest{
				SpiffeId:            nodeID,
				CertSerialNumber:    updatedSerial,
				CertNotAfter:        updatedExpires,
				NewCertNotAfter:     updatedNewExpires,
				NewCertSerialNumber: updatedNewSerial,
				InputMask:           &common.AttestedNodeMask{},
			},
			expUpdatedNode: &common.AttestedNode{
				SpiffeId:            nodeID,
				AttestationDataType: attestationType,
				CertSerialNumber:    serial,
				CertNotAfter:        expires,
				NewCertNotAfter:     newExpires,
				NewCertSerialNumber: newSerial,
			},
		},
		{
			name: "update attested node with mask set only some fields: 'CertSerialNumber', 'NewCertNotAfter'",
			updateReq: &datastore.UpdateAttestedNodeRequest{
				SpiffeId:            nodeID,
				CertSerialNumber:    updatedSerial,
				CertNotAfter:        updatedExpires,
				NewCertNotAfter:     updatedNewExpires,
				NewCertSerialNumber: updatedNewSerial,
				InputMask: &common.AttestedNodeMask{
					CertSerialNumber: true,
					NewCertNotAfter:  true,
				},
			},
			expUpdatedNode: &common.AttestedNode{
				SpiffeId:            nodeID,
				AttestationDataType: attestationType,
				CertSerialNumber:    updatedSerial,
				CertNotAfter:        expires,
				NewCertNotAfter:     updatedNewExpires,
				NewCertSerialNumber: newSerial,
			},
		},
		{
			name: "update attested node with nil mask",
			updateReq: &datastore.UpdateAttestedNodeRequest{
				SpiffeId:            nodeID,
				CertSerialNumber:    updatedSerial,
				CertNotAfter:        updatedExpires,
				NewCertNotAfter:     updatedNewExpires,
				NewCertSerialNumber: updatedNewSerial,
			},
			expUpdatedNode: &common.AttestedNode{
				SpiffeId:            nodeID,
				AttestationDataType: attestationType,
				CertSerialNumber:    updatedSerial,
				CertNotAfter:        updatedExpires,
				NewCertNotAfter:     updatedNewExpires,
				NewCertSerialNumber: updatedNewSerial,
			},
		},
	}
	for _, test := range tests {
		test := test
		s.T().Run(test.name, func(t *testing.T) {
			node := &common.AttestedNode{
				SpiffeId:            nodeID,
				AttestationDataType: attestationType,
				CertSerialNumber:    serial,
				CertNotAfter:        expires,
				NewCertNotAfter:     newExpires,
				NewCertSerialNumber: newSerial,
			}
			s.shim.DeleteAttestedNode(ctx, &datastore.DeleteAttestedNodeRequest{SpiffeId: nodeID})

			_, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node})
			s.Require().NoError(err)

			// Update attested node
			uresp, err := s.shim.UpdateAttestedNode(ctx, test.updateReq)
			s.RequireGRPCStatus(err, test.expCode, test.expMsg)
			if test.expCode != codes.OK {
				s.Require().Nil(uresp)
				return
			}
			s.Require().NoError(err)
			s.Require().NotNil(uresp)
			s.RequireProtoEqual(test.expUpdatedNode, uresp.Node)

			// Check a fresh fetch shows the updated attested node
			fresp, err := s.shim.FetchAttestedNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: test.updateReq.SpiffeId})
			s.Require().NoError(err)
			s.Require().NotNil(fresp)
			s.RequireProtoEqual(test.expUpdatedNode, fresp.Node)
		})
	}
}

func (s *PluginSuite) TestDeleteAttestedNode() {
	entry := &common.AttestedNode{
		SpiffeId:            "foo",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	// delete it before it exists
	_, err := s.shim.DeleteAttestedNode(ctx, &datastore.DeleteAttestedNodeRequest{SpiffeId: entry.SpiffeId})
	s.RequireGRPCStatus(err, codes.NotFound, _notFoundErrMsg)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: entry})
	s.Require().NoError(err)

	dresp, err := s.shim.DeleteAttestedNode(ctx, &datastore.DeleteAttestedNodeRequest{SpiffeId: entry.SpiffeId})
	s.Require().NoError(err)
	s.AssertProtoEqual(entry, dresp.Node)

	fresp, err := s.shim.FetchAttestedNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: entry.SpiffeId})
	s.Require().NoError(err)
	s.Nil(fresp.Node)
}

func (s *PluginSuite) TestNodeSelectors() {
	node1 := &common.AttestedNode{
		SpiffeId:            "foo",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	node2 := &common.AttestedNode{
		SpiffeId:            "bar",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	foo1 := []*common.Selector{
		{Type: "FOO1", Value: "1"},
	}
	foo2 := []*common.Selector{
		{Type: "FOO2", Value: "1"},
	}
	bar := []*common.Selector{
		{Type: "BAR", Value: "FIGHT"},
	}

	_, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node1})
	s.Require().NoError(err)

	_, err = s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node2})
	s.Require().NoError(err)

	// assert there are no selectors for foo
	selectors := s.getNodeSelectors("foo", true)
	s.Require().Empty(selectors)
	selectors = s.getNodeSelectors("foo", false)
	s.Require().Empty(selectors)

	// set selectors on foo and bar
	s.setNodeSelectors("foo", foo1)
	s.setNodeSelectors("bar", bar)

	// get foo selectors
	selectors = s.getNodeSelectors("foo", true)
	s.RequireProtoListEqual(foo1, selectors)
	selectors = s.getNodeSelectors("foo", false)
	s.RequireProtoListEqual(foo1, selectors)

	// replace foo selectors
	s.setNodeSelectors("foo", foo2)
	selectors = s.getNodeSelectors("foo", true)
	s.RequireProtoListEqual(foo2, selectors)
	selectors = s.getNodeSelectors("foo", false)
	s.RequireProtoListEqual(foo2, selectors)

	// delete foo selectors
	s.setNodeSelectors("foo", nil)
	selectors = s.getNodeSelectors("foo", true)
	s.Require().Empty(selectors)
	selectors = s.getNodeSelectors("foo", false)
	s.Require().Empty(selectors)

	// get bar selectors (make sure they weren't impacted by deleting foo)
	selectors = s.getNodeSelectors("bar", true)
	s.RequireProtoListEqual(bar, selectors)
	// get bar selectors (make sure they weren't impacted by deleting foo)
	selectors = s.getNodeSelectors("bar", false)
	s.RequireProtoListEqual(bar, selectors)
}

func (s *PluginSuite) TestListNodeSelectors() {
	s.T().Run("no selectors exist", func(t *testing.T) {
		req := &datastore.ListNodeSelectorsRequest{}
		resp := s.listNodeSelectors(req)
		s.Assert().Empty(resp.Selectors)
	})

	const numNonExpiredAttNodes = 3
	const attestationDataType = "fake_nodeattestor"
	nonExpiredAttNodes := make([]*common.AttestedNode, numNonExpiredAttNodes)
	for i := 0; i < numNonExpiredAttNodes; i++ {
		nonExpiredAttNodes[i] = &common.AttestedNode{
			SpiffeId:            fmt.Sprintf("spiffe://example.org/non-expired-node-%d", i),
			AttestationDataType: attestationDataType,
			CertSerialNumber:    fmt.Sprintf("non-expired serial %d-1", i),
			CertNotAfter:        time.Now().Add(time.Hour).Unix(),
			NewCertSerialNumber: fmt.Sprintf("non-expired serial %d-2", i),
			NewCertNotAfter:     time.Now().Add(2 * time.Hour).Unix(),
		}
	}

	const numExpiredAttNodes = 2
	expiredAttNodes := make([]*common.AttestedNode, numExpiredAttNodes)
	for i := 0; i < numExpiredAttNodes; i++ {
		expiredAttNodes[i] = &common.AttestedNode{
			SpiffeId:            fmt.Sprintf("spiffe://example.org/expired-node-%d", i),
			AttestationDataType: attestationDataType,
			CertSerialNumber:    fmt.Sprintf("expired serial %d-1", i),
			CertNotAfter:        time.Now().Add(-24 * time.Hour).Unix(),
			NewCertSerialNumber: fmt.Sprintf("expired serial %d-2", i),
			NewCertNotAfter:     time.Now().Add(-12 * time.Hour).Unix(),
		}
	}

	allAttNodesToCreate := append(nonExpiredAttNodes, expiredAttNodes...)
	selectorMap := make(map[string][]*common.Selector)
	for i, n := range allAttNodesToCreate {
		req := &datastore.CreateAttestedNodeRequest{
			Node: n,
		}

		_, err := s.shim.CreateAttestedNode(ctx, req)
		s.Require().NoError(err)

		selectors := []*common.Selector{
			{
				Type:  "foo",
				Value: strconv.Itoa(i),
			},
		}

		s.setNodeSelectors(n.SpiffeId, selectors)
		selectorMap[n.SpiffeId] = selectors
	}

	nonExpiredSelectorsMap := make(map[string][]*common.Selector, numNonExpiredAttNodes)
	for i := 0; i < numNonExpiredAttNodes; i++ {
		spiffeID := nonExpiredAttNodes[i].SpiffeId
		nonExpiredSelectorsMap[spiffeID] = selectorMap[spiffeID]
	}

	s.T().Run("list all", func(t *testing.T) {
		req := &datastore.ListNodeSelectorsRequest{}
		resp := s.listNodeSelectors(req)
		s.Require().Len(resp.Selectors, len(selectorMap))
	})

	s.T().Run("list unexpired", func(t *testing.T) {
		req := &datastore.ListNodeSelectorsRequest{
			ValidAt: &timestamppb.Timestamp{
				Seconds: time.Now().Unix(),
			},
		}

		resp := s.listNodeSelectors(req)
		s.Assert().Len(resp.Selectors, len(nonExpiredSelectorsMap))
		for _, n := range resp.Selectors {
			expectedSelectors, ok := nonExpiredSelectorsMap[n.SpiffeId]
			s.Assert().True(ok)
			s.AssertProtoListEqual(expectedSelectors, n.Selectors)
		}
	})
}

func (s *PluginSuite) TestSetNodeSelectorsUnderLoad() {
	selectors := []*common.Selector{
		{Type: "TYPE", Value: "VALUE"},
	}

	const numWorkers = 20

	resultCh := make(chan error, numWorkers)
	nextID := int32(0)

	// Create nodes for valid node selector IDs
	node := &common.AttestedNode{
		SpiffeId:            "foo",
		AttestationDataType: "aws-tag",
		CertSerialNumber:    "badcafe",
		CertNotAfter:        time.Now().Add(time.Hour).Unix(),
	}

	for i := 0; i < numWorkers+1; i++ {
		node.SpiffeId = fmt.Sprintf("ID%d", i)
		_, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node})
		s.Require().NoError(err)
	}

	for i := 0; i < numWorkers; i++ {
		go func() {
			id := fmt.Sprintf("ID%d", atomic.AddInt32(&nextID, 1))
			for j := 0; j < 10; j++ {
				_, err := s.shim.SetNodeSelectors(ctx, &datastore.SetNodeSelectorsRequest{
					Selectors: &datastore.NodeSelectors{
						SpiffeId:  id,
						Selectors: selectors,
					},
				})
				if err != nil {
					resultCh <- err
				}
			}
			resultCh <- nil
		}()
	}

	for i := 0; i < numWorkers; i++ {
		s.Require().NoError(<-resultCh)
	}
}
