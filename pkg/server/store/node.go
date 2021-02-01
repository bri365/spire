// Package store implements a datastore shim
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/common/idutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CountAttestedNodes counts all attested nodes
func (s *Shim) CountAttestedNodes(ctx context.Context,
	req *datastore.CountAttestedNodesRequest) (*datastore.CountAttestedNodesResponse, error) {
	if s.Store == nil {
		return s.DataStore.CountAttestedNodes(ctx, req)
	}

	// Set range to all node keys
	key := nodeKey("")
	end := allNodes
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, CountOnly: true})
	if err != nil {
		return nil, err
	}
	return &datastore.CountAttestedNodesResponse{Nodes: int32(res.Total)}, nil
}

// CreateAttestedNode stores the given attested node
func (s *Shim) CreateAttestedNode(ctx context.Context,
	req *datastore.CreateAttestedNodeRequest) (*datastore.CreateAttestedNodeResponse, error) {
	if s.Store == nil {
		return s.DataStore.CreateAttestedNode(ctx, req)
	}

	node := req.Node

	// build the node record key and value
	k := nodeKey(node.SpiffeId)
	v, err := proto.Marshal(node)
	if err != nil {
		return nil, err
	}

	// Create a list of items to add, starting with the attested node
	kvs := []*store.KeyValue{{Key: k, Value: v}}

	// Create index records for expiry, banned, and attestation type
	kvs = append(kvs, &store.KeyValue{Key: nodeExpKey(node.SpiffeId, node.CertNotAfter)})
	kvs = append(kvs, &store.KeyValue{Key: nodeBanKey(node.SpiffeId, node.CertSerialNumber)})
	kvs = append(kvs, &store.KeyValue{Key: nodeAdtKey(node.SpiffeId, node.AttestationDataType)})

	// Create index records for selectors
	for _, sel := range node.Selectors {
		kvs = append(kvs, &store.KeyValue{Key: nodeSelKey(node.SpiffeId, sel)})
	}

	s.log.Info(fmt.Sprintf("CAN kvs %v", kvs))

	_, err = s.Store.Create(ctx, &store.PutRequest{Kvs: kvs})
	if err != nil {
		return nil, err
	}
	return &datastore.CreateAttestedNodeResponse{Node: node}, nil
}

// DeleteAttestedNode deletes the given attested node
func (s *Shim) DeleteAttestedNode(ctx context.Context, req *datastore.DeleteAttestedNodeRequest) (*datastore.DeleteAttestedNodeResponse, error) {
	if s.Store == nil {
		return s.DataStore.DeleteAttestedNode(ctx, req)
	}

	var err error
	req.SpiffeId, err = idutil.NormalizeSpiffeID(req.SpiffeId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	// TODO delete index records
	_, err = s.Store.Delete(ctx, &store.DeleteRequest{Ranges: []*store.Range{{Key: nodeKey(req.SpiffeId)}}})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteAttestedNodeResponse{Node: &common.AttestedNode{}}, nil
}

// FetchAttestedNode fetches an existing attested node by SPIFFE ID
func (s *Shim) FetchAttestedNode(ctx context.Context,
	req *datastore.FetchAttestedNodeRequest) (resp *datastore.FetchAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.FetchAttestedNode(ctx, req)
	}

	if err != nil {
		return nil, err
	}

	resp, _, err = s.fetchNode(ctx, req)

	return
}

// fetchNode fetches an existing attested node by SPIFFE ID along with the current version
func (s *Shim) fetchNode(ctx context.Context,
	req *datastore.FetchAttestedNodeRequest) (*datastore.FetchAttestedNodeResponse, int64, error) {
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: nodeKey(req.SpiffeId)})
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
/*
type ListAttestedNodesRequest struct {
	ByExpiresBefore   *wrapperspb.Int64Value `protobuf:"bytes,1,opt,name=by_expires_before,json=byExpiresBefore,proto3" json:"by_expires_before,omitempty"`
	Pagination        *Pagination            `protobuf:"bytes,2,opt,name=pagination,proto3" json:"pagination,omitempty"`
	ByAttestationType string                 `protobuf:"bytes,3,opt,name=by_attestation_type,json=byAttestationType,proto3" json:"by_attestation_type,omitempty"`
	BySelectorMatch   *BySelectors           `protobuf:"bytes,4,opt,name=by_selector_match,json=bySelectorMatch,proto3" json:"by_selector_match,omitempty"`
	ByBanned          *wrapperspb.BoolValue  `protobuf:"bytes,5,opt,name=by_banned,json=byBanned,proto3" json:"by_banned,omitempty"`
	FetchSelectors    bool                   `protobuf:"varint,6,opt,name=fetch_selectors,json=fetchSelectors,proto3" json:"fetch_selectors,omitempty"`
	// contains filtered or unexported fields
}
*/
func (s *Shim) ListAttestedNodes(ctx context.Context, req *datastore.ListAttestedNodesRequest) (*datastore.ListAttestedNodesResponse, error) {
	if s.Store == nil {
		return s.DataStore.ListAttestedNodes(ctx, req)
	}

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	if req.BySelectorMatch != nil && len(req.BySelectorMatch.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot list by empty selectors set")
	}

	// NOTE: while it is possible to use the store revision from first call in subsequent calls
	// to ensure transactional consistency of index and item read operations, the current
	// implementation should suffice as the resulting node list is the intersection of indices
	// so added nodes won't appear, and empty fetches are not errors, so deleted nodes will not
	// appear either.
	idMaps := []map[string]bool{}

	// TODO ByAttestationType string
	if req.ByAttestationType != "" {
		s.log.Info(fmt.Sprintf("By attestation type %s", req.ByAttestationType))
	}

	// TODO ByBanned wrapped bool
	if req.ByBanned != nil {
		s.log.Info(fmt.Sprintf("By banned %t", req.ByBanned.Value))
	}

	if req.ByExpiresBefore != nil {
		s.log.Info(fmt.Sprintf("By expires before %d", req.ByExpiresBefore.Value))
		ids, err := s.nodeExpMap(ctx, req.ByExpiresBefore.Value)
		if err != nil {
			return nil, err
		}
		idMaps = append(idMaps, ids)
	}

	// TODO BySelectorMatch *BySelectors
	if req.BySelectorMatch != nil {
		s.log.Info(fmt.Sprintf("By selector %v %v", req.BySelectorMatch.Match, req.BySelectorMatch.Selectors))
	}

	count := len(idMaps)
	s.log.Info(fmt.Sprintf("%d node list(s) in idMaps", count))
	if count > 1 {
		s.log.Info(fmt.Sprintf("idMaps %v", idMaps))
		// intersect query maps into the first one
		for i := 1; i < count; i++ {
			tmp := map[string]bool{}
			for id := range idMaps[0] {
				// Add item if it appears in both maps
				if _, ok := idMaps[i][id]; ok {
					tmp[id] = true
				}
			}
			idMaps[0] = tmp
		}
	}

	if count > 0 {
		s.log.Info(fmt.Sprintf("idMaps[0] %v", idMaps[0]))
	}

	nodes := []*common.AttestedNode{}
	key := nodeKey("")
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 12 || p.Token[0:2] != nodePrefix {
				return nil, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			// TODO one bit larger than token
			key = fmt.Sprintf("%s ", p.Token)
		}
	}

	if count > 0 {
		// Get the specified list of nodes
		// NOTE: looping will not scale to desired limits; these should be served from cache
		// An interim approach would be to send batches of reads as a single transaction. Batches
		// would be PageSize if paginated or a few hundred to a thousand at a time.
		var i int64 = 1
		for id := range idMaps[0] {
			if p != nil && len(p.Token) > 0 && id < p.Token {
				continue
			}

			res, err := s.Store.Get(ctx, &store.GetRequest{Key: nodeKey(id)})
			if err != nil {
				return nil, err
			}

			if len(res.Kvs) == 1 {
				node := &common.AttestedNode{}
				err = proto.Unmarshal(res.Kvs[0].Value, node)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, node)
				if limit > 0 && i == limit {
					p.Token = node.SpiffeId
					break
				}
			}
		}
	} else {
		// No query constraints, get all nodes up to limit
		end := allNodes
		res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Limit: limit})
		if err != nil {
			return nil, err
		}

		for _, kv := range res.Kvs {
			node := &common.AttestedNode{}
			err = proto.Unmarshal(kv.Value, node)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
	}

	return &datastore.ListAttestedNodesResponse{Nodes: nodes}, nil
}

// UpdateAttestedNode updates the given node's cert serial and expiration.
func (s *Shim) UpdateAttestedNode(ctx context.Context, req *datastore.UpdateAttestedNodeRequest) (resp *datastore.UpdateAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.UpdateAttestedNode(ctx, req)
	}
	// TODO implement
	return
}

// GetNodeSelectors gets node (agent) selectors by SPIFFE ID
func (s *Shim) GetNodeSelectors(ctx context.Context, req *datastore.GetNodeSelectorsRequest) (resp *datastore.GetNodeSelectorsResponse, err error) {
	if s.Store == nil {
		return s.DataStore.GetNodeSelectors(ctx, req)
	}
	// TODO implement
	return
}

// ListNodeSelectors gets node (agent) selectors by SPIFFE ID
func (s *Shim) ListNodeSelectors(ctx context.Context, req *datastore.ListNodeSelectorsRequest) (resp *datastore.ListNodeSelectorsResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListNodeSelectors(ctx, req)
	}
	// TODO implement
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
	// TODO implement
	return
}

// nodeKey returns a string formatted key for an attested node
func nodeKey(id string) string {
	// e.g. "N|spiffie://example.com/clusterA/nodeN"
	return fmt.Sprintf("%s%s", nodePrefix, id)
}

// nodeAdtKey returns a string formatted key for an attested node indexed by attestation data type
func nodeAdtKey(id, adt string) string {
	// e.g. "IN|ADT|aws-tag|spiffie://example.com/clusterA/nodeN"
	return fmt.Sprintf("%s%s%s%s%s%s%s", indexKeyID, nodePrefix, ADT, delim, adt, delim, id)
}

// nodeAdtID returns the attested node id from the given attestation data type index key
func nodeAdtID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid attestation data type index key: %s", key)
	}
	return items[3], nil
}

// nodeAdtList returns a map of attested node ids by attestation data type
// The map facilitates easier intersection with other queries
func (s *Shim) nodeAdtMap(ctx context.Context, adt string) (map[string]bool, error) {
	key := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, nodePrefix, ADT, delim, adt, delim)
	end := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, nodePrefix, ADT, delim, adt, delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := nodeAdtID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// nodeBanKey returns a string formatted key for an attested node indexed by banned status
func nodeBanKey(id, csn string) string {
	// e.g. "IN|BAN|0|spiffie://example.com/clusterA/nodeN"
	banned := 0
	if csn == "" {
		banned = 1
	}
	return fmt.Sprintf("%s%s%s%s%d%s%s", indexKeyID, nodePrefix, BAN, delim, banned, delim, id)
}

// nodeBanID returns the attested node id from the given node banned (BAN) index key
func nodeBanID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid node banned index key: %s", key)
	}
	return items[3], nil
}

// nodeBanList returns a map of attested node ids that are either banned or not
// The map facilitates easier intersection with other queries
func (s *Shim) nodeBanMap(ctx context.Context, ban bool) (map[string]bool, error) {
	b := 0
	if ban {
		b = 1
	}
	key := fmt.Sprintf("%s%s%s%s%d%s", indexKeyID, nodePrefix, BAN, delim, b, delim)
	end := fmt.Sprintf("%s%s%s%s%d", indexKeyID, nodePrefix, BAN, delim, b+1)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := nodeBanID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// nodeExpKey returns a string formatted key for an attested node indexed by expiry in seconds
// NOTE: %d without leading zeroes for time.Unix will work for the next ~250 years
func nodeExpKey(id string, exp int64) string {
	// e.g. "IN|EXP|1611907252|spiffie://example.com/clusterA/nodeN"
	return fmt.Sprintf("%s%s%s%s%d%s%s", indexKeyID, nodePrefix, EXP, delim, exp, delim, id)
}

// nodeExpID returns the attested node id from the given node expiry (EXP) index key
func nodeExpID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid node expiry index key: %s", key)
	}
	return items[3], nil
}

// nodeExpList returns a map of attested node ids expiring before the given expiry
// The map facilitates easier intersection with other queries
func (s *Shim) nodeExpMap(ctx context.Context, exp int64) (map[string]bool, error) {
	key := fmt.Sprintf("%s%s%s%s", indexKeyID, nodePrefix, EXP, delim)
	end := fmt.Sprintf("%s%s%s%s%d", indexKeyID, nodePrefix, EXP, delim, exp)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := nodeExpID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// nodeSelKey returns a string formatted key for an attested node indexed by selector type and value
func nodeSelKey(id string, s *common.Selector) string {
	// e.g. "IN|TVS|type|value|spiffie://example.com/clusterA/nodeN"
	return fmt.Sprintf("%s%s%s%s%s%s%s%s%s", indexKeyID, nodePrefix, TVS, delim, s.Type, delim, s.Value, delim, id)
}

// nodeSelID returns the attested node id from the given selector index key
func nodeSelID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 5 {
		return "", fmt.Errorf("invalid node selector index key: %s", key)
	}
	return items[4], nil
}

// nodeSelList returns a map of attested node ids by selector match
// The map facilitates easier intersection with other queries
func (s *Shim) nodeSelMap(ctx context.Context, bySel *datastore.BySelectors) (map[string]bool, error) {
	//m := bySel.Match
	//sels := bySel.Selectors

	// TODO
	key := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, nodePrefix, TVS, delim, "sel", delim)
	end := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, nodePrefix, TVS, delim, "sel", delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := nodeSelID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}
