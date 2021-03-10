// Package store implements a datastore shim with the proposed new store interface.
package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CountAttestedNodes counts all attested nodes in the store
func (s *Shim) CountAttestedNodes(ctx context.Context,
	req *datastore.CountAttestedNodesRequest) (*datastore.CountAttestedNodesResponse, error) {
	// Fall back to SQL if store is not configured
	if s.Store == nil {
		return s.DataStore.CountAttestedNodes(ctx, req)
	}

	// Set range to all node keys
	key := NodeKey("")
	end := AllNodes

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, CountOnly: true})
	if err != nil {
		return nil, err
	}

	return &datastore.CountAttestedNodesResponse{Nodes: int32(res.Total)}, nil
}

// CreateAttestedNode adds the given attested node to the store.
func (s *Shim) CreateAttestedNode(ctx context.Context,
	req *datastore.CreateAttestedNodeRequest) (*datastore.CreateAttestedNodeResponse, error) {
	if s.Store == nil {
		return s.DataStore.CreateAttestedNode(ctx, req)
	}

	// Build the node record key and value
	n := req.Node
	k := NodeKey(n.SpiffeId)
	v, err := proto.Marshal(n)
	if err != nil {
		// Return gRPC InvalidArgument error?
		return nil, err
	}

	// Create a list of keys to add, starting with the attested node, ensuring it doesn't already exist
	put := []*store.KeyValue{{Key: k, Value: v, Compare: store.Compare_NOT_PRESENT}}

	// Create empty index records for attestation type and banned
	put = append(put, &store.KeyValue{Key: NodeAdtKey(n.SpiffeId, n.AttestationDataType)})
	put = append(put, &store.KeyValue{Key: NodeBanKey(n.SpiffeId, n.CertSerialNumber)})

	// Create index record for expiry with node selectors as content to simplify selector listing
	sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
	v, err = proto.Marshal(sel)
	if err != nil {
		return nil, err
	}
	put = append(put, &store.KeyValue{Key: NodeExpKey(n.SpiffeId, n.CertNotAfter), Value: v})

	// Create index records for individual selectors
	for _, sel := range n.Selectors {
		put = append(put, &store.KeyValue{Key: NodeSelKey(n.SpiffeId, sel)})
	}

	// One put operation for this transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: put}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		// TODO get most accurate error possible
		// st := status.Convert(err)
		return nil, err
	}

	return &datastore.CreateAttestedNodeResponse{Node: n}, nil
}

// DeleteAttestedNode deletes the given attested node.
func (s *Shim) DeleteAttestedNode(ctx context.Context,
	req *datastore.DeleteAttestedNodeRequest) (*datastore.DeleteAttestedNodeResponse, error) {
	if s.Store == nil {
		return s.DataStore.DeleteAttestedNode(ctx, req)
	}

	s.Log.Debug("DN", "req", req)

	// Get current attested node and version for transactional integrity
	fn, ver, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: node not found (DN)")
	}
	n := fn.Node

	// Build a list of delete operations to be performed as a transaction,
	// starting with the node at the version read above. The entire transaction
	// will fail if this record has been changed since the node was fetched.
	del := []*store.KeyValue{{Key: NodeKey(n.SpiffeId), Version: ver, Compare: store.Compare_EQUALS}}

	// Add index records for expiry, banned, and attestation type (any version)
	del = append(del, &store.KeyValue{Key: NodeExpKey(n.SpiffeId, n.CertNotAfter)})
	del = append(del, &store.KeyValue{Key: NodeBanKey(n.SpiffeId, n.CertSerialNumber)})
	del = append(del, &store.KeyValue{Key: NodeAdtKey(n.SpiffeId, n.AttestationDataType)})

	// Add index records for selectors
	for _, sel := range n.Selectors {
		del = append(del, &store.KeyValue{Key: NodeSelKey(n.SpiffeId, sel)})
	}

	// One delete operation for all keys in the transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: del}}

	// Invalidate cache entry here to prevent race condition with async watcher
	if s.c.nodeCacheInvalidate {
		s.removeNodeCacheEntry(n.SpiffeId)
	}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteAttestedNodeResponse{Node: n}, nil
}

// FetchAttestedNode fetches an existing attested node by SPIFFE ID.
func (s *Shim) FetchAttestedNode(ctx context.Context,
	req *datastore.FetchAttestedNodeRequest) (resp *datastore.FetchAttestedNodeResponse, err error) {
	if s.Store == nil {
		return s.DataStore.FetchAttestedNode(ctx, req)
	}

	if s.c.nodeCacheFetch {
		node := s.fetchNodeCacheEntry(req.SpiffeId)
		if node != nil {
			resp = &datastore.FetchAttestedNodeResponse{Node: node}
			return
		}
	}

	resp, _, err = s.fetchNode(ctx, req)
	if resp.Node == nil {
		return
	}

	if s.c.nodeCacheUpdate {
		s.Log.Warn("node cache miss", "id", resp.Node.SpiffeId)
		s.setNodeCacheEntry(req.SpiffeId, resp.Node)
	}

	return
}

// fetchNode fetches an existing attested node along with the current version for transactional integrity.
func (s *Shim) fetchNode(ctx context.Context,
	req *datastore.FetchAttestedNodeRequest) (*datastore.FetchAttestedNodeResponse, int64, error) {
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: NodeKey(req.SpiffeId)})
	if err != nil {
		return nil, 0, err
	}

	var ver int64
	resp := &datastore.FetchAttestedNodeResponse{}
	if len(res.Kvs) == 1 {
		ver = res.Kvs[0].Version
		n := &common.AttestedNode{}
		err = proto.Unmarshal(res.Kvs[0].Value, n)
		if err != nil {
			return nil, 0, err
		}
		resp.Node = n
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("more than one node for %s", req.SpiffeId)
	}

	return resp, ver, nil
}

// ListAttestedNodes lists all attested nodes, optionally filtered and/or paginated.
func (s *Shim) ListAttestedNodes(ctx context.Context,
	req *datastore.ListAttestedNodesRequest) (resp *datastore.ListAttestedNodesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListAttestedNodes(ctx, req)
	}

	resp, err = s.listAttestedNodes(ctx, 0, req)

	return
}

// listAttestedNodes lists all attested nodes, optionally filtered and/or paginated.
// Store revision is accepted and returned for consistency across paginated calls.
func (s *Shim) listAttestedNodes(ctx context.Context, revision int64,
	req *datastore.ListAttestedNodesRequest) (*datastore.ListAttestedNodesResponse, error) {
	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	if req.BySelectorMatch != nil && len(req.BySelectorMatch.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot list by empty selectors set")
	}

	// If specific rev not requested and we are filtering with one or more indices, get the
	// current store revision for use in subsequent calls to ensure transactional consistency.
	rev := revision
	if rev == 0 && (req.ByAttestationType != "" || req.ByBanned != nil || req.ByExpiresBefore != nil || req.BySelectorMatch != nil) {
		res, err := s.Store.Get(ctx, &store.GetRequest{Key: NodePrefix, End: AllNodes, Limit: 1})
		if err != nil {
			return nil, err
		}
		rev = res.Revision
	}

	// A set of node IDs for the filtered results - boolean maps make set intersection easier to read
	// NOTE: for performance reasons, organize the following filters with smallest expected results first
	idSets := []map[string]bool{}

	if req.ByAttestationType != "" {
		ids, err := s.nodeAdtMap(ctx, rev, req.ByAttestationType)
		if err != nil {
			return nil, err
		}
		idSets = append(idSets, ids)
	}

	if req.ByBanned != nil {
		ids, err := s.nodeBanMap(ctx, rev, req.ByBanned.Value)
		if err != nil {
			return nil, err
		}
		idSets = append(idSets, ids)
	}

	if req.ByExpiresBefore != nil {
		ids, err := s.nodeExpMap(ctx, rev, req.ByExpiresBefore.Value, false)
		if err != nil {
			return nil, err
		}
		idSets = append(idSets, ids)
	}

	if req.BySelectorMatch != nil {
		subset := map[string]bool{}
		for _, sel := range req.BySelectorMatch.Selectors {
			ids, err := s.nodeSelMap(ctx, rev, sel)
			if err != nil {
				return nil, err
			}
			switch req.BySelectorMatch.Match {
			case datastore.BySelectors_MATCH_EXACT:
				// The given selectors are the complete set for a node to match
				idSets = append(idSets, ids)
			case datastore.BySelectors_MATCH_SUBSET:
				// The given selectors are a subset (up to all) of a node
				// or a subset of the given selectors match the total selectors of a node.
				// Adding these together results in an overly optimistic node list which is culled later.
				for id := range ids {
					subset[id] = true
				}
			default:
				return nil, fmt.Errorf("unhandled match behavior %q", req.BySelectorMatch.Match)
			}
		}
		if len(subset) > 0 {
			idSets = append(idSets, subset)
		}
	}

	count := len(idSets)
	if count > 1 {
		// intersect each additional query set into the first set
		// resulting in a single set of IDs meeting all filter criteria
		// TODO extract this into a shared utility library
		for i := 1; i < count; i++ {
			tmp := map[string]bool{}
			for id := range idSets[0] {
				// Add item if it appears in both sets
				if idSets[i][id] {
					tmp[id] = true
				}
			}
			idSets[0] = tmp
		}
	}

	key := NodeKey("")
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 5 || p.Token[0:2] != NodePrefix {
				return nil, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			key = stringPlusOne(p.Token)
		}
	}

	lastKey := ""
	resp := &datastore.ListAttestedNodesResponse{}
	if count > 0 {
		// Create a sorted list of node IDs from resulting filter map
		ids := []string{}
		for id := range idSets[0] {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		// Get the specified list of nodes from the query filters
		// NOTE: looping will not scale to desired limits; these should be served from cache
		// An interim approach would be to send batches of reads as a single transaction
		// Batches would be PageSize if paginated or a few hundred to a thousand at a time
		var i int64 = 1
		for _, id := range ids {
			// Ignore entries from previous pages
			if p != nil && len(p.Token) > 0 && NodeKey(id) < key {
				continue
			}

			n := &common.AttestedNode{}
			cached := false
			// Serve from cache if available and revision is acceptable
			if s.c.nodeCacheEnabled && s.c.initialized {
				s.c.mu.RLock()
				if rev == s.c.storeRevision {
					var ok bool
					if n, ok = s.c.nodes[id]; ok {
						cached = true
					}
				}
				s.c.mu.RUnlock()
			}

			if !cached {
				// NOTE: looping one at a time will not scale to desired limits
				// Reads should be batched in transactions
				// Batches would be PageSize if paginated or a few hundred to a thousand at a time
				res, err := s.Store.Get(ctx, &store.GetRequest{Key: NodeKey(id), Revision: rev})
				if err != nil {
					return nil, err
				}

				if len(res.Kvs) != 1 {
					if len(res.Kvs) > 1 {
						s.Log.Error(fmt.Sprintf("LN too many entries %v", res.Kvs))
					}
					continue
				}

				if err = proto.Unmarshal(res.Kvs[0].Value, n); err != nil {
					return nil, err
				}
			}

			if req.BySelectorMatch != nil {
				// Remove the overly optimistic results from above
				if !s.nodeSelectorMatch(n, req.BySelectorMatch) {
					continue
				}
			} else if !req.FetchSelectors {
				// Do not return selectors if not requested or filtered by selectors
				n = nodeWithoutSelectors(n)
			}

			resp.Nodes = append(resp.Nodes, n)
			lastKey = NodeKey(n.SpiffeId)

			// If paginated, have we reached the page limit?
			if limit > 0 && i == limit {
				break
			}

			i++
		}
	} else {
		// No filters, get all nodes up to limit

		// Serve from cache if available, not paginated, and rev matches or was not specified
		if s.c.nodeCacheEnabled && s.c.initialized {
			s.c.mu.RLock()
			r := s.c.storeRevision
			if p == nil && (revision == 0 || revision == r) {
				resp.Nodes = make([]*common.AttestedNode, len(s.c.nodes))
				i := 0
				for _, n := range s.c.nodes {
					resp.Nodes[i] = n
					i++
				}
				s.c.mu.RUnlock()
				return resp, nil
			}
			s.c.mu.RUnlock()
		}

		// Pagination requested or cache does not support the requested rev
		// TODO pagination support requires a sorted array of IDs be maintained with the cache entries.
		a := s.clock.Now().UnixNano()
		res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: AllNodes, Limit: limit, Revision: rev})
		if err != nil {
			return nil, err
		}

		b := s.clock.Now().UnixNano()
		s.Log.Debug("listNodes Get", "key", key, "count", len(res.Kvs), "total", res.Total, "msec", (b-a)/1000000)
		for _, kv := range res.Kvs {
			n := &common.AttestedNode{}
			err = proto.Unmarshal(kv.Value, n)
			if err != nil {
				return nil, err
			}
			if !req.FetchSelectors {
				// Do not return selectors if not requested
				n = nodeWithoutSelectors(n)
			}
			resp.Nodes = append(resp.Nodes, n)
			lastKey = kv.Key
		}
		s.Log.Debug("listNodes for", "lastKey", lastKey, "msec", (s.clock.Now().UnixNano()-b)/1000000)
	}

	if p != nil {
		p.Token = ""
		// Note: In the event the total number of items exactly equals the page size,
		// there may be one extra list call that returns no items. This fact is used
		// in other parts of the code so it should not be optimized without consideration.
		if len(resp.Nodes) > 0 {
			p.Token = lastKey
		}
		resp.Pagination = p
	}

	return resp, nil
}

// nodeWithoutSelectors return a copy of an attested node without selectors.
func nodeWithoutSelectors(n *common.AttestedNode) *common.AttestedNode {
	return &common.AttestedNode{
		SpiffeId:            n.SpiffeId,
		AttestationDataType: n.AttestationDataType,
		CertNotAfter:        n.CertNotAfter,
		CertSerialNumber:    n.CertSerialNumber,
		NewCertNotAfter:     n.NewCertNotAfter,
		NewCertSerialNumber: n.NewCertSerialNumber,
		RevisionNumber:      n.RevisionNumber,
	}
}

// UpdateAttestedNode updates cert serial number and/or expiration for the given node.
func (s *Shim) UpdateAttestedNode(ctx context.Context,
	req *datastore.UpdateAttestedNodeRequest) (*datastore.UpdateAttestedNodeResponse, error) {
	if s.Store == nil {
		return s.DataStore.UpdateAttestedNode(ctx, req)
	}

	s.Log.Debug("UN", "req", req)

	// Get current attested node and version for transactional integrity
	fn, ver, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: node not found (UN)")
	}

	n := fn.Node
	changed := false

	// KeyValue arrays for delete and create operations
	del := []*store.KeyValue{}
	put := []*store.KeyValue{}

	if req.InputMask == nil {
		req.InputMask = protoutil.AllTrueCommonAgentMask
	}

	if req.InputMask.CertNotAfter && n.CertNotAfter != req.CertNotAfter {
		// Index record for expiry contains node selectors as content to simplify selector listing
		sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
		v, err := proto.Marshal(sel)
		if err != nil {
			return nil, err
		}

		del = append(del, &store.KeyValue{Key: NodeExpKey(n.SpiffeId, n.CertNotAfter)})
		put = append(put, &store.KeyValue{Key: NodeExpKey(n.SpiffeId, req.CertNotAfter), Value: v})
		n.CertNotAfter = req.CertNotAfter
		changed = true
	}

	if req.InputMask.CertSerialNumber {
		// Delete existing index key if different from new one
		oldKey := NodeBanKey(n.SpiffeId, n.CertSerialNumber)
		newKey := NodeBanKey(n.SpiffeId, req.CertSerialNumber)
		if newKey != oldKey {
			del = append(del, &store.KeyValue{Key: oldKey})
			put = append(put, &store.KeyValue{Key: newKey})
		}

		n.CertSerialNumber = req.CertSerialNumber
		changed = true
	}

	// New certificate fields are not indexed
	if req.InputMask.NewCertNotAfter {
		n.NewCertNotAfter = req.NewCertNotAfter
		changed = true
	}

	if req.InputMask.NewCertSerialNumber {
		n.NewCertSerialNumber = req.NewCertSerialNumber
		changed = true
	}

	if !changed {
		return &datastore.UpdateAttestedNodeResponse{Node: n}, nil
	}

	// Build updated node KeyValue
	k := NodeKey(n.SpiffeId)
	v, err := proto.Marshal(n)
	if err != nil {
		return nil, err
	}

	// Add put record for updated node, ensuring version has not changed since fetching above
	put = append(put, &store.KeyValue{Key: k, Value: v, Version: ver, Compare: store.Compare_EQUALS})

	// Transaction elements
	tx := []*store.SetRequestElement{}
	if len(del) > 0 {
		tx = append(tx, &store.SetRequestElement{Kvs: del, Operation: store.Operation_DELETE})
	}
	tx = append(tx, &store.SetRequestElement{Kvs: put, Operation: store.Operation_PUT})

	// Invalidate cache entry here to prevent race condition with async watcher
	if s.c.nodeCacheInvalidate {
		s.removeNodeCacheEntry(n.SpiffeId)
	}

	// Submit transaction
	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		return nil, err
	}

	return &datastore.UpdateAttestedNodeResponse{Node: n}, nil
}

// GetNodeSelectors gets node (agent) selectors for the given node.
func (s *Shim) GetNodeSelectors(ctx context.Context,
	req *datastore.GetNodeSelectorsRequest) (*datastore.GetNodeSelectorsResponse, error) {
	if s.Store == nil {
		return s.DataStore.GetNodeSelectors(ctx, req)
	}

	s.Log.Debug("GNS", "req", req)

	// get current attested node
	fn, _, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: node not found (GNS)")
	}

	ns := &datastore.NodeSelectors{SpiffeId: req.SpiffeId, Selectors: fn.Node.Selectors}
	return &datastore.GetNodeSelectorsResponse{Selectors: ns}, nil
}

// ListNodeSelectors gets all node (agent) selectors, optionally filtered by expires after.
func (s *Shim) ListNodeSelectors(ctx context.Context,
	req *datastore.ListNodeSelectorsRequest) (*datastore.ListNodeSelectorsResponse, error) {
	if s.Store == nil {
		return s.DataStore.ListNodeSelectors(ctx, req)
	}

	s.Log.Debug("LNS", "req", req)

	// Serve from cache if available
	if s.c.nodeCacheEnabled && s.c.initialized {
		s.c.mu.RLock()
		selectors := make([]*datastore.NodeSelectors, len(s.c.nodes))
		i := 0
		for _, n := range s.c.nodes {
			selectors[i] = &datastore.NodeSelectors{
				SpiffeId:  n.SpiffeId,
				Selectors: n.Selectors,
			}
			i++
		}
		s.c.mu.RUnlock()
		return &datastore.ListNodeSelectorsResponse{Selectors: selectors}, nil
	}

	// Node expiry index contains node selectors, get all or requested valid after nodes
	key := nodeExpPrefix
	end := nodeExpAll
	if req.ValidAt != nil {
		key = NodeExpKey("", req.ValidAt.Seconds)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

	selectors := make([]*datastore.NodeSelectors, 0, res.Total)
	for _, kv := range res.Kvs {
		sel := &datastore.NodeSelectors{}
		err = proto.Unmarshal(kv.Value, sel)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, sel)
	}

	return &datastore.ListNodeSelectorsResponse{Selectors: selectors}, nil
}

// SetNodeSelectors sets node (agent) selectors and deletes index keys for selectors no longer used.
// NOTE: the node may or may not exist when this is called
func (s *Shim) SetNodeSelectors(ctx context.Context,
	req *datastore.SetNodeSelectorsRequest) (*datastore.SetNodeSelectorsResponse, error) {
	if s.Store == nil {
		return s.DataStore.SetNodeSelectors(ctx, req)
	}

	if req.Selectors == nil {
		return nil, errors.New("invalid request: missing selectors")
	}

	s.Log.Debug("SNS", "req", req)

	// get current attested node and version for transactional integrity
	fn, ver, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.Selectors.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		s.Log.Debug("SNS not found", "node", req.Selectors.SpiffeId)
	}
	n := fn.Node

	// Build a list of existing index keys to delete any unused ones
	delKeys := map[string]bool{}
	if n != nil && n.Selectors != nil {
		for _, sel := range n.Selectors {
			delKeys[NodeSelKey(n.SpiffeId, sel)] = true
		}
	}

	del := []*store.KeyValue{}
	put := []*store.KeyValue{}
	tx := []*store.SetRequestElement{}

	// Start with node, if it exists, ensuring version matches the one read above
	if n != nil {
		// Build update record for node with new selectors
		n.Selectors = req.Selectors.Selectors
		k := NodeKey(n.SpiffeId)
		v, err := proto.Marshal(n)
		if err != nil {
			return nil, err
		}

		put = append(put, &store.KeyValue{Key: k, Value: v, Version: ver, Compare: store.Compare_EQUALS})

		// Index record for expiry contains node selectors as content to simplify selector listing, so upsert it
		sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
		v, err = proto.Marshal(sel)
		if err != nil {
			return nil, err
		}
		put = append(put, &store.KeyValue{Key: NodeExpKey(n.SpiffeId, n.CertNotAfter), Value: v})
	}

	// Add index records for new selectors
	for _, sel := range req.Selectors.Selectors {
		key := NodeSelKey(n.SpiffeId, sel)
		put = append(put, &store.KeyValue{Key: key})

		// Remove this key from the delete list as we are changing it
		delete(delKeys, key)
	}

	if len(delKeys) > 0 {
		// Delete remaining unused index keys
		for k := range delKeys {
			del = append(del, &store.KeyValue{Key: k})
		}

		// Add delete operation to transaction
		tx = append(tx, &store.SetRequestElement{Kvs: del, Operation: store.Operation_DELETE})
	}

	// Add put operation for node update and new selectors to transaction
	tx = append(tx, &store.SetRequestElement{Kvs: put, Operation: store.Operation_PUT})

	s.Log.Debug("SNS", "del", del, "put", put)

	// Invalidate cache entry here to prevent race condition with async watcher
	if n != nil {
		s.removeNodeCacheEntry(n.SpiffeId)
	}

	// Submit transaction
	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		return nil, err
	}

	return &datastore.SetNodeSelectorsResponse{}, nil
}

// IsNodeKey returns true if the given key is a properly formatted attested node key.
func IsNodeKey(key string) bool {
	items := strings.Split(key, Delim)
	if len(items) == 2 && items[0] == NodeKeyID {
		return true
	}
	return false
}

// NodeKey returns a string formatted key for an attested node.
// e.g. "N|spiffie://example.com/clusterA/nodeN"
func NodeKey(id string) string {
	return fmt.Sprintf("%s%s", NodePrefix, id)
}

// nodeIDFromKey returns the node id from the given node key.
func nodeIDFromKey(key string) (string, error) {
	items := strings.Split(key, Delim)
	if len(items) != 2 || items[0] != NodeKeyID {
		return "", fmt.Errorf("invalid node key: %s", key)
	}
	return items[1], nil
}

// NodeAdtKey returns a string formatted key for an attested node indexed by attestation data type.
// e.g. "NI|ADT|aws-tag|spiffie://example.com/clusterA/nodeN"
func NodeAdtKey(id, adt string) string {
	return fmt.Sprintf("%s%s%s%s%s%s", nodeIndex, ADT, Delim, adt, Delim, id)
}

// nodeAdtID returns the attested node id from the given attestation data type (ADT) index key.
func nodeAdtID(key string) (string, error) {
	items := strings.Split(key, Delim)
	// TODO additional checks to ensure a properly formatted index?
	if len(items) != 4 {
		return "", fmt.Errorf("invalid attestation data type index key: %s", key)
	}
	return items[3], nil
}

// nodeAdtMap returns a map of attested node ids by attestation data type.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) nodeAdtMap(ctx context.Context, rev int64, adt string) (map[string]bool, error) {
	key := fmt.Sprintf("%s%s%s%s%s", nodeIndex, ADT, Delim, adt, Delim)
	end := fmt.Sprintf("%s%s%s%s%s", nodeIndex, ADT, Delim, adt, Delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
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

// NodeBanKey returns a string formatted key for an attested node indexed by banned status.
// e.g. "NI|BAN|0|spiffie://example.com/clusterA/nodeN"
func NodeBanKey(id, csn string) string {
	banned := 0
	if csn == "" {
		banned = 1
	}
	return fmt.Sprintf("%s%s%s%d%s%s", nodeIndex, BAN, Delim, banned, Delim, id)
}

// nodeBanID returns the attested node id from the given node banned (BAN) index key.
func nodeBanID(key string) (string, error) {
	items := strings.Split(key, Delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid node banned index key: %s", key)
	}
	return items[3], nil
}

// nodeBanMap returns a map of attested node ids that are either banned or not.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) nodeBanMap(ctx context.Context, rev int64, ban bool) (map[string]bool, error) {
	b := 0
	if ban {
		b = 1
	}
	key := fmt.Sprintf("%s%s%s%d%s", nodeIndex, BAN, Delim, b, Delim)
	end := fmt.Sprintf("%s%s%s%d", nodeIndex, BAN, Delim, b+1)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
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

// NodeExpKey returns a string formatted key for an attested node indexed by expiry in seconds.
// e.g. "NI|EXP|1611907252|spiffie://example.com/clusterA/nodeN"
// NOTE: %d without leading zeroes for time.Unix will work for the next ~250 years
func NodeExpKey(id string, exp int64) string {
	return fmt.Sprintf("%s%s%s%d%s%s", nodeIndex, EXP, Delim, exp, Delim, id)
}

// nodeExpID returns the attested node id from the given node expiry (EXP) index key.
func nodeExpID(key string) (string, error) {
	items := strings.Split(key, Delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid node expiry index key: %s", key)
	}
	return items[3], nil
}

// nodeExpMap returns a map of attested node ids expiring before or after the given expiry.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) nodeExpMap(ctx context.Context, rev, exp int64, after bool) (map[string]bool, error) {
	// Set range to all index keys after or before the given time
	key, end := "", ""
	if after {
		key = fmt.Sprintf("%s%s%s%d", nodeIndex, EXP, Delim, exp)
		end = fmt.Sprintf("%s%s%s", nodeIndex, EXP, Delend)
	} else {
		key = fmt.Sprintf("%s%s%s", nodeIndex, EXP, Delim)
		end = fmt.Sprintf("%s%s%s%d", nodeIndex, EXP, Delim, exp)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
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

// NodeSelKey returns a string formatted key for an attested node indexed by selector type and value.
// e.g. "NI|TVI|a-type|a-value|spiffie://example.com/clusterA/nodeN"
func NodeSelKey(id string, s *common.Selector) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s%s", nodeIndex, TVI, Delim, s.Type, Delim, s.Value, Delim, id)
}

// nodeSelID returns the attested node id from the given type-value (TVI) index key
func nodeSelID(key string) (string, error) {
	items := strings.Split(key, Delim)
	if len(items) != 5 {
		return "", fmt.Errorf("invalid node selector index key: %s", key)
	}
	return items[4], nil
}

// nodeSelMap returns a map of attested node ids by selector match.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) nodeSelMap(ctx context.Context, rev int64, sel *common.Selector) (map[string]bool, error) {
	// Set range to all index keys for this type and value
	key := fmt.Sprintf("%s%s%s%s%s%s%s", nodeIndex, TVI, Delim, sel.Type, Delim, sel.Value, Delim)
	end := fmt.Sprintf("%s%s%s%s%s%s%s", nodeIndex, TVI, Delim, sel.Type, Delim, sel.Value, Delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
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

// selectorMatch verifies the node selectors properly match the requested selectors.
//   - Exact match is true if both lists are identical
//   - Subset match is true if all node selectors are present in requested selectors
func (s *Shim) nodeSelectorMatch(node *common.AttestedNode, req *datastore.BySelectors) bool {
	nodeSelectors := selectorMap(node.Selectors)
	reqSelectors := selectorMap(req.Selectors)
	switch req.Match {
	case datastore.BySelectors_MATCH_EXACT:
		// Are the requested selectors the complete set of node selectors?
		if reflect.DeepEqual(nodeSelectors, reqSelectors) {
			return true
		}
	case datastore.BySelectors_MATCH_SUBSET:
		// Do all node selectors exist in request selectors?
		for nodeType, nodeValue := range nodeSelectors {
			reqValue, ok := reqSelectors[nodeType]
			if ok {
				if reqValue != nodeValue {
					return false
				}
			} else {
				return false
			}
		}
		return true
	default:
		s.Log.Warn(fmt.Sprintf("Unknown match %v", req.Match))
	}
	return false
}

// selectorMap returns the given selectors as a map.
func selectorMap(selectors []*common.Selector) map[string]string {
	sm := map[string]string{}
	for _, sel := range selectors {
		sm[sel.Type] = sel.Value
	}
	return sm
}
