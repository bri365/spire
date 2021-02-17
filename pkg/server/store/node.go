package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CountAttestedNodes counts all attested nodes in the store
func (s *Shim) CountAttestedNodes(ctx context.Context,
	req *datastore.CountAttestedNodesRequest) (*datastore.CountAttestedNodesResponse, error) {

	// Fall back to SQL if store is not configured
	if s.Store == nil {
		return s.DataStore.CountAttestedNodes(ctx, req)
	}

	// Set range to all node keys
	key := nodeKey("")
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
	k := nodeKey(n.SpiffeId)
	v, err := proto.Marshal(n)
	if err != nil {
		// Return gRPC InvalidArgument error?
		return nil, err
	}

	// Create a list of keys to add, starting with the attested node, ensuring it doesn't already exist
	put := []*store.KeyValue{{Key: k, Value: v, Compare: store.Compare_NOT_PRESENT}}

	// Create empty index records for attestation type and banned
	put = append(put, &store.KeyValue{Key: nodeAdtKey(n.SpiffeId, n.AttestationDataType)})
	put = append(put, &store.KeyValue{Key: nodeBanKey(n.SpiffeId, n.CertSerialNumber)})

	// Create index record for expiry with node selectors as content to simplify selector listing
	sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
	v, err = proto.Marshal(sel)
	if err != nil {
		return nil, err
	}
	put = append(put, &store.KeyValue{Key: nodeExpKey(n.SpiffeId, n.CertNotAfter), Value: v})

	// Create index records for individual selectors
	for _, sel := range n.Selectors {
		put = append(put, &store.KeyValue{Key: nodeSelKey(n.SpiffeId, sel)})
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

	// Get current attested node and version for transactional integrity
	fn, ver, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}
	n := fn.Node

	// Build a list of delete operations to be performed as a transaction,
	// starting with the node at the version read above. The entire transaction
	// will fail if this record has been changed since the node was fetched.
	del := []*store.KeyValue{{Key: nodeKey(n.SpiffeId), Version: ver, Compare: store.Compare_EQUALS}}

	// Add index records for expiry, banned, and attestation type (any version)
	del = append(del, &store.KeyValue{Key: nodeExpKey(n.SpiffeId, n.CertNotAfter)})
	del = append(del, &store.KeyValue{Key: nodeBanKey(n.SpiffeId, n.CertSerialNumber)})
	del = append(del, &store.KeyValue{Key: nodeAdtKey(n.SpiffeId, n.AttestationDataType)})

	// Add index records for selectors
	for _, sel := range n.Selectors {
		del = append(del, &store.KeyValue{Key: nodeSelKey(n.SpiffeId, sel)})
	}

	// One delete operation for all keys in the transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: del}}

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

	resp, _, err = s.fetchNode(ctx, req)
	if resp.Node == nil {
		return
	}

	return
}

// fetchNode fetches an existing attested node along with the current version for transactional integrity.
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
		n := &common.AttestedNode{}
		err = proto.Unmarshal(res.Kvs[0].Value, n)
		if err != nil {
			return nil, 0, err
		}
		resp.Node = n
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("More than one node for %s", req.SpiffeId)
	}

	return resp, ver, nil
}

// ListAttestedNodes lists all attested nodes, optionally filtered and/or paginated.
func (s *Shim) ListAttestedNodes(ctx context.Context,
	req *datastore.ListAttestedNodesRequest) (resp *datastore.ListAttestedNodesResponse, err error) {

	if s.Store == nil {
		return s.DataStore.ListAttestedNodes(ctx, req)
	}

	resp, _, err = s.listAttestedNodes(ctx, 0, req)

	return
}

// listAttestedNodes lists all attested nodes, optionally filtered and/or paginated.
// Store revision is accepted and returned for consistency across paginated calls.
func (s *Shim) listAttestedNodes(ctx context.Context, revision int64,
	req *datastore.ListAttestedNodesRequest) (*datastore.ListAttestedNodesResponse, int64, error) {

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	if req.BySelectorMatch != nil && len(req.BySelectorMatch.Selectors) == 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "cannot list by empty selectors set")
	}

	// If a specific store revision was not requested, get the current store revision for use in
	// subsequent calls to ensure transactional consistency of index read operations.
	rev := revision
	if rev == 0 {
		res, err := s.Store.Get(ctx, &store.GetRequest{Key: NodePrefix, End: AllNodes, Limit: 1})
		if err != nil {
			return nil, 0, err
		}
		rev = res.Revision
	}

	// A set of node IDs for the filtered results - boolean maps make set intersection easier to read
	// NOTE: for performance reasons, organize the following filters with smallest expected results first
	idSets := []map[string]bool{}

	if req.ByAttestationType != "" {
		ids, err := s.nodeAdtMap(ctx, rev, req.ByAttestationType)
		if err != nil {
			return nil, 0, err
		}
		idSets = append(idSets, ids)
	}

	if req.ByBanned != nil {
		ids, err := s.nodeBanMap(ctx, rev, req.ByBanned.Value)
		if err != nil {
			return nil, 0, err
		}
		idSets = append(idSets, ids)
	}

	if req.ByExpiresBefore != nil {
		ids, err := s.nodeExpMap(ctx, rev, req.ByExpiresBefore.Value, false)
		if err != nil {
			return nil, 0, err
		}
		idSets = append(idSets, ids)
	}

	if req.BySelectorMatch != nil {
		subset := map[string]bool{}
		for _, sel := range req.BySelectorMatch.Selectors {
			ids, err := s.nodeSelMap(ctx, rev, sel)
			if err != nil {
				return nil, 0, err
			}
			if req.BySelectorMatch.Match == datastore.BySelectors_MATCH_EXACT {
				// The given selectors are the complete set for a node to match
				idSets = append(idSets, ids)
			} else if req.BySelectorMatch.Match == datastore.BySelectors_MATCH_SUBSET {
				// The given selectors are a subset (up to all) of a node
				// or a subset of the given selectors match the total selectors of a node.
				// Adding these together results in an overly optimistic node list which is culled later.
				for id := range ids {
					subset[id] = true
				}
			} else {
				return nil, 0, fmt.Errorf("unhandled match behavior %q", req.BySelectorMatch.Match)
			}
		}
		if len(subset) > 0 {
			idSets = append(idSets, subset)
		}
	}

	count := len(idSets)
	if count > 1 {
		// intersect each additional query set into the first set
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

	key := nodeKey("")
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 5 || p.Token[0:2] != NodePrefix {
				return nil, 0, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			// TODO one bit larger than token
			key = fmt.Sprintf("%s!", p.Token)
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
			if p != nil && len(p.Token) > 0 && nodeKey(id) < key {
				continue
			}

			res, err := s.Store.Get(ctx, &store.GetRequest{Key: nodeKey(id), Revision: rev})
			if err != nil {
				return nil, 0, err
			}

			if len(res.Kvs) == 1 {
				n := &common.AttestedNode{}
				err = proto.Unmarshal(res.Kvs[0].Value, n)
				if err != nil {
					return nil, 0, err
				}
				if req.BySelectorMatch != nil {
					if !s.nodeSelectorMatch(n, req.BySelectorMatch) {
						// Failed selector match - skip this item
						continue
					}
				} else if req.FetchSelectors == false {
					// Do not return selectors if not requested or filtered by selectors
					n.Selectors = nil
				}
				resp.Nodes = append(resp.Nodes, n)
				lastKey = nodeKey(n.SpiffeId)

				if limit > 0 && i == limit {
					break
				}
			}
			i++
		}
	} else {
		// No filters, get all nodes up to limit
		res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: AllNodes, Limit: limit, Revision: rev})
		if err != nil {
			return nil, 0, err
		}

		for _, kv := range res.Kvs {
			n := &common.AttestedNode{}
			err = proto.Unmarshal(kv.Value, n)
			if err != nil {
				return nil, 0, err
			}
			if req.FetchSelectors == false {
				n.Selectors = nil
			}
			resp.Nodes = append(resp.Nodes, n)
			lastKey = kv.Key
		}
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

	return resp, rev, nil
}

// UpdateAttestedNode updates cert serial number and/or expiration for the given node.
func (s *Shim) UpdateAttestedNode(ctx context.Context,
	req *datastore.UpdateAttestedNodeRequest) (*datastore.UpdateAttestedNodeResponse, error) {

	if s.Store == nil {
		return s.DataStore.UpdateAttestedNode(ctx, req)
	}

	// Get current attested node and version for transactional integrity
	fn, ver, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
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

		del = append(del, &store.KeyValue{Key: nodeExpKey(n.SpiffeId, n.CertNotAfter)})
		put = append(put, &store.KeyValue{Key: nodeExpKey(n.SpiffeId, req.CertNotAfter), Value: v})
		n.CertNotAfter = req.CertNotAfter
		changed = true
	}

	if req.InputMask.CertSerialNumber {
		// Delete existing index key if different from new one
		oldKey := nodeBanKey(n.SpiffeId, n.CertSerialNumber)
		newKey := nodeBanKey(n.SpiffeId, req.CertSerialNumber)
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
	k := nodeKey(n.SpiffeId)
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

	// get current attested node
	fn, _, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
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

	// Start with a reasonable size array to reduce the number of reallocations
	selectors := make([]*datastore.NodeSelectors, 0, 256)

	// Node expiry index contains node selectors, get all or requested valid after nodes
	key := nodeExpPrefix
	end := nodeExpAll
	if req.ValidAt != nil {
		key = nodeExpKey("", req.ValidAt.Seconds)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

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
func (s *Shim) SetNodeSelectors(ctx context.Context,
	req *datastore.SetNodeSelectorsRequest) (*datastore.SetNodeSelectorsResponse, error) {

	if s.Store == nil {
		return s.DataStore.SetNodeSelectors(ctx, req)
	}

	if req.Selectors == nil {
		return nil, errors.New("invalid request: missing selectors")
	}

	// get current attested node and version for transactional integrity
	fn, ver, err := s.fetchNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: req.Selectors.SpiffeId})
	if err != nil {
		return nil, err
	}
	if fn.Node == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}
	n := fn.Node

	// Build a list of existing index keys to delete any unused ones
	delKeys := map[string]bool{}
	if n.Selectors != nil {
		for _, sel := range n.Selectors {
			delKeys[nodeSelKey(n.SpiffeId, sel)] = true
		}
	}

	// Build update record for node with new selectors
	n.Selectors = req.Selectors.Selectors
	k := nodeKey(n.SpiffeId)
	v, err := proto.Marshal(n)
	if err != nil {
		return nil, err
	}

	// Build kvs list for put operation, starting with node and ensuring version is the same as read above
	put := []*store.KeyValue{{Key: k, Value: v, Version: ver, Compare: store.Compare_EQUALS}}

	// Add index records for new selectors
	for _, sel := range n.Selectors {
		key := nodeSelKey(n.SpiffeId, sel)
		put = append(put, &store.KeyValue{Key: key})

		// Remove this key from the delete list as we are changing it
		if _, ok := delKeys[key]; ok {
			delete(delKeys, key)
		}
	}

	// Index record for expiry contains node selectors as content to simplify selector listing, so upsert it
	sel := &datastore.NodeSelectors{SpiffeId: n.SpiffeId, Selectors: n.Selectors}
	v, err = proto.Marshal(sel)
	if err != nil {
		return nil, err
	}
	put = append(put, &store.KeyValue{Key: nodeExpKey(n.SpiffeId, n.CertNotAfter), Value: v})

	tx := []*store.SetRequestElement{}

	if len(delKeys) > 0 {
		// Delete remaining unused index keys
		del := []*store.KeyValue{}
		for k := range delKeys {
			del = append(del, &store.KeyValue{Key: k})
		}

		// Add delete operation to transaction
		tx = append(tx, &store.SetRequestElement{Kvs: del, Operation: store.Operation_DELETE})
	}

	// Add put operation for node update and new selectors to transaction
	tx = append(tx, &store.SetRequestElement{Kvs: put, Operation: store.Operation_PUT})

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

// nodeKey returns a string formatted key for an attested node.
// e.g. "N|spiffie://example.com/clusterA/nodeN"
func nodeKey(id string) string {
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

// nodeAdtKey returns a string formatted key for an attested node indexed by attestation data type.
// e.g. "NI|ADT|aws-tag|spiffie://example.com/clusterA/nodeN"
func nodeAdtKey(id, adt string) string {
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

// nodeBanKey returns a string formatted key for an attested node indexed by banned status.
// e.g. "NI|BAN|0|spiffie://example.com/clusterA/nodeN"
func nodeBanKey(id, csn string) string {
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

// nodeExpKey returns a string formatted key for an attested node indexed by expiry in seconds.
// e.g. "NI|EXP|1611907252|spiffie://example.com/clusterA/nodeN"
// NOTE: %d without leading zeroes for time.Unix will work for the next ~250 years
func nodeExpKey(id string, exp int64) string {
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

// nodeSelKey returns a string formatted key for an attested node indexed by selector type and value.
// e.g. "NI|TVI|a-type|a-value|spiffie://example.com/clusterA/nodeN"
func nodeSelKey(id string, s *common.Selector) string {
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
	if req.Match == datastore.BySelectors_MATCH_EXACT {
		// Are the requested selectors the complete set of node selectors?
		if reflect.DeepEqual(nodeSelectors, reqSelectors) {
			return true
		}
	} else if req.Match == datastore.BySelectors_MATCH_SUBSET {
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
	} else {
		s.log.Warn(fmt.Sprintf("Unknown match %v", req.Match))
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
