// Package store implements a datastore shim
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/common/idutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CountAttestedNodes counts all attested nodes
func (s *Shim) CountAttestedNodes(ctx context.Context, req *datastore.CountAttestedNodesRequest) (*datastore.CountAttestedNodesResponse, error) {
	if s.Store == nil {
		return s.DataStore.CountAttestedNodes(ctx, req)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: "n|", End: "o", CountOnly: true})
	if err != nil {
		return nil, err
	}
	return &datastore.CountAttestedNodesResponse{Nodes: int32(res.Total)}, nil
}

// CreateAttestedNode stores the given attested node
func (s *Shim) CreateAttestedNode(ctx context.Context, req *datastore.CreateAttestedNodeRequest) (resp *datastore.CreateAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.CreateAttestedNode(ctx, req)
	}

	var v []byte
	k := fmt.Sprintf("n|%s", req.Node.SpiffeId)
	v, err = proto.Marshal(req.Node)
	if err != nil {
		return
	}
	_, err = s.Store.Create(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{{Key: k, Value: v}},
	})
	if err != nil {
		return
	}
	return &datastore.CreateAttestedNodeResponse{Node: req.Node}, nil

}

// DeleteAttestedNode deletes the given attested node
func (s *Shim) DeleteAttestedNode(ctx context.Context, req *datastore.DeleteAttestedNodeRequest) (resp *datastore.DeleteAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.DeleteAttestedNode(ctx, req)
	}

	req.SpiffeId, err = idutil.NormalizeSpiffeID(req.SpiffeId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	_, err = s.Store.Delete(ctx, &store.DeleteRequest{
		Ranges: []*store.Range{{Key: fmt.Sprintf("n|%s", req.SpiffeId)}},
	})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteAttestedNodeResponse{Node: &common.AttestedNode{}}, nil
}

// FetchAttestedNode fetches an existing attested node by SPIFFE ID
func (s *Shim) FetchAttestedNode(ctx context.Context, req *datastore.FetchAttestedNodeRequest) (resp *datastore.FetchAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.FetchAttestedNode(ctx, req)
	}

	req.SpiffeId, err = idutil.NormalizeSpiffeID(req.SpiffeId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	resp, _, err = s.fetchNode(ctx, req)

	return
}

// fetchNode fetches an existing attested node by SPIFFE ID along with the current version
func (s *Shim) fetchNode(ctx context.Context, req *datastore.FetchAttestedNodeRequest) (*datastore.FetchAttestedNodeResponse, int64, error) {
	res, err := s.Store.Get(ctx, &store.GetRequest{
		Key: fmt.Sprintf("n|%s", req.SpiffeId),
	})
	if err != nil {
		return nil, 0, err
	}

	var ver int64
	resp := &datastore.FetchAttestedNodeResponse{}
	if len(res.Kvs) == 1 {
		ver = res.Kvs[0].Version
		node := &common.AttestedNode{}
		err = proto.Unmarshal(res.Kvs[0].Value, node)
		if err != nil {
			return nil, 0, err
		}
		resp.Node = node
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("More than one node for %s", req.SpiffeId)
	}
	return resp, ver, nil
}

// ListAttestedNodes lists all attested nodes (pagination available)
func (s *Shim) ListAttestedNodes(ctx context.Context, req *datastore.ListAttestedNodesRequest) (resp *datastore.ListAttestedNodesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListAttestedNodes(ctx, req)
	}

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	if req.BySelectorMatch != nil && len(req.BySelectorMatch.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot list by empty selectors set")
	}

	return
}

// UpdateAttestedNode updates the given node's cert serial and expiration.
func (s *Shim) UpdateAttestedNode(ctx context.Context, req *datastore.UpdateAttestedNodeRequest) (resp *datastore.UpdateAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.UpdateAttestedNode(ctx, req)
	}

	return
}

// GetNodeSelectors gets node (agent) selectors by SPIFFE ID
func (s *Shim) GetNodeSelectors(ctx context.Context, req *datastore.GetNodeSelectorsRequest) (resp *datastore.GetNodeSelectorsResponse, err error) {
	if s.Store == nil {
		return s.DataStore.GetNodeSelectors(ctx, req)
	}

	return
}

// ListNodeSelectors gets node (agent) selectors by SPIFFE ID
func (s *Shim) ListNodeSelectors(ctx context.Context, req *datastore.ListNodeSelectorsRequest) (resp *datastore.ListNodeSelectorsResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListNodeSelectors(ctx, req)
	}

	return
}

// SetNodeSelectors sets node (agent) selectors by SPIFFE ID, deleting old selectors first
func (s *Shim) SetNodeSelectors(ctx context.Context, req *datastore.SetNodeSelectorsRequest) (resp *datastore.SetNodeSelectorsResponse, err error) {
	if s.Store == nil {
		return s.DataStore.SetNodeSelectors(ctx, req)
	}

	if req.Selectors == nil {
		return nil, errors.New("invalid request: missing selectors")
	}

	return
}
