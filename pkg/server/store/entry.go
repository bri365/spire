// Package store implements a datastore shim with the proposed new store interface.
package store

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CountRegistrationEntries counts all registrations.
func (s *Shim) CountRegistrationEntries(ctx context.Context,
	req *datastore.CountRegistrationEntriesRequest) (*datastore.CountRegistrationEntriesResponse, error) {

	// Fall back to SQL if store is not configured
	if s.Store == nil {
		return s.DataStore.CountRegistrationEntries(ctx, req)
	}

	// Set range to all entry keys
	key := entryKey("")
	end := allEntries

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, CountOnly: true})
	if err != nil {
		return nil, err
	}
	return &datastore.CountRegistrationEntriesResponse{Entries: int32(res.Total)}, nil
}

// CreateRegistrationEntry adds the given registration entry to the store.
func (s *Shim) CreateRegistrationEntry(ctx context.Context,
	req *datastore.CreateRegistrationEntryRequest) (*datastore.CreateRegistrationEntryResponse, error) {

	if s.Store == nil {
		return s.DataStore.CreateRegistrationEntry(ctx, req)
	}

	// TODO: Validations should be done in the ProtoBuf level [https://github.com/spiffe/spire/issues/44]
	err := validateRegistrationEntry(req.Entry)
	if err != nil {
		return nil, err
	}

	if req.Entry.EntryId != "" {
		return nil, status.Error(codes.InvalidArgument, "store-etcd: invalid request: EntryId not empty")
	}

	req.Entry.EntryId, err = newRegistrationEntryID()
	if err != nil {
		return nil, err
	}

	// Build the entry record key and value
	e := req.Entry
	k := entryKey(e.EntryId)
	v, err := proto.Marshal(e)
	if err != nil {
		// Return gRPC InvalidArgument error?
		return nil, err
	}

	// Create a list of keys to add, starting with the registered entry, ensuring it doesn't already exist
	put := []*store.KeyValue{{Key: k, Value: v, Compare: store.Compare_NOT_PRESENT}}

	// Create empty index records for parent ID and SPIFFE ID
	put = append(put, &store.KeyValue{Key: entryPidKey(e.EntryId, e.ParentId)})
	put = append(put, &store.KeyValue{Key: entrySidKey(e.EntryId, e.SpiffeId)})

	// Create index record for expiry with selectors as content to simplify listing by selector
	// NOTE: NodeSelectors works here as SpiffeId and EntryId are both strings
	sel := &datastore.NodeSelectors{SpiffeId: e.EntryId, Selectors: e.Selectors}
	v, err = proto.Marshal(sel)
	if err != nil {
		return nil, err
	}
	put = append(put, &store.KeyValue{Key: entryExpKey(e.EntryId, e.EntryExpiry), Value: v})

	// Create index records for federation by bundle domain
	for _, domain := range e.FederatesWith {
		put = append(put, &store.KeyValue{Key: entryFedByDomainKey(e.EntryId, domain)})
	}

	// Create index records for individual selectors
	for _, sel := range e.Selectors {
		put = append(put, &store.KeyValue{Key: entrySelKey(e.EntryId, sel)})
	}

	// One put operation for this transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: put}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		// TODO get most accurate error possible
		// st := status.Convert(err)
		return nil, err
	}

	return &datastore.CreateRegistrationEntryResponse{Entry: e}, nil
}

// DeleteRegistrationEntry deletes the given registration entry.
func (s *Shim) DeleteRegistrationEntry(ctx context.Context,
	req *datastore.DeleteRegistrationEntryRequest) (*datastore.DeleteRegistrationEntryResponse, error) {

	if s.Store == nil {
		return s.DataStore.DeleteRegistrationEntry(ctx, req)
	}

	// Get current registration entry and version for transactional integrity
	fe, ver, err := s.fetchEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: req.EntryId})
	if err != nil {
		return nil, err
	}
	if fe.Entry == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}
	e := fe.Entry

	// Build a list of delete operations to be performed as a transaction,
	// starting with the entry at the version read above. The entire transaction
	// will fail if this record has been changed since the entry was fetched.
	del := []*store.KeyValue{{Key: entryKey(e.EntryId), Version: ver, Compare: store.Compare_EQUALS}}

	// Add index records for expiry, parent ID, and SPIFFE ID (any version)
	del = append(del, &store.KeyValue{Key: entryExpKey(e.EntryId, e.EntryExpiry)})
	del = append(del, &store.KeyValue{Key: entryPidKey(e.EntryId, e.ParentId)})
	del = append(del, &store.KeyValue{Key: entrySidKey(e.EntryId, e.SpiffeId)})

	// Add index records for federation by bundle domain
	for _, domain := range e.FederatesWith {
		del = append(del, &store.KeyValue{Key: entryFedByDomainKey(e.EntryId, domain)})
	}

	// Add index records for selectors
	for _, sel := range e.Selectors {
		del = append(del, &store.KeyValue{Key: entrySelKey(e.EntryId, sel)})
	}

	// One delete operation for all keys in the transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: del}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteRegistrationEntryResponse{Entry: &common.RegistrationEntry{}}, nil
}

// FetchRegistrationEntry fetches an existing registration by entry ID
func (s *Shim) FetchRegistrationEntry(ctx context.Context,
	req *datastore.FetchRegistrationEntryRequest) (resp *datastore.FetchRegistrationEntryResponse, err error) {

	if s.Store == nil {
		return s.DataStore.FetchRegistrationEntry(ctx, req)
	}

	resp, _, err = s.fetchEntry(ctx, req)

	return
}

// fetchRegistrationEntry fetches an existing registration by entry ID
func (s *Shim) fetchEntry(ctx context.Context,
	req *datastore.FetchRegistrationEntryRequest) (*datastore.FetchRegistrationEntryResponse, int64, error) {

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: entryKey(req.EntryId)})
	if err != nil {
		return nil, 0, err
	}

	var ver int64
	resp := &datastore.FetchRegistrationEntryResponse{}
	if len(res.Kvs) == 1 {
		ver = res.Kvs[0].Version
		entry := &common.RegistrationEntry{}
		err = proto.Unmarshal(res.Kvs[0].Value, entry)
		if err != nil {
			return nil, 0, err
		}
		resp.Entry = entry
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("More than one entry for %s", req.EntryId)
	}
	return resp, ver, nil
}

// ListRegistrationEntries lists all registrations (pagination available)
func (s *Shim) ListRegistrationEntries(ctx context.Context,
	req *datastore.ListRegistrationEntriesRequest) (*datastore.ListRegistrationEntriesResponse, error) {

	if s.Store == nil {
		return s.DataStore.ListRegistrationEntries(ctx, req)
	}

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	if req.BySelectors != nil && len(req.BySelectors.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot list by empty selector set")
	}

	// Get the current store revision for use in subsequent calls to ensure
	// transactional consistency of all item and index read operations.
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: entryPrefix, End: allEntries, Limit: 1})
	if err != nil {
		return nil, err
	}
	rev := res.Revision

	// A collection of IDs for the filtered results - maps make intersection easier
	// NOTE: for performance reasons, organize the following filters with smallest expected results first
	idMaps := []map[string]bool{}

	if req.BySpiffeId != nil && req.BySpiffeId.Value != "" {
		ids, err := s.entrySidMap(ctx, rev, req.BySpiffeId.Value)
		if err != nil {
			return nil, err
		}
		idMaps = append(idMaps, ids)
	}

	if req.ByParentId != nil && req.ByParentId.Value != "" {
		ids, err := s.entryPidMap(ctx, rev, req.ByParentId.Value)
		if err != nil {
			return nil, err
		}
		idMaps = append(idMaps, ids)
	}

	if req.ByFederatesWith != nil {
		subset := map[string]bool{}
		for _, domain := range req.ByFederatesWith.TrustDomains {
			ids, err := s.entryFedByDomainMap(ctx, rev, domain)
			if err != nil {
				return nil, err
			}
			if req.ByFederatesWith.Match == datastore.ByFederatesWith_MATCH_EXACT {
				// The given selectors are the complete set for an entry to match
				idMaps = append(idMaps, ids)
			} else if req.ByFederatesWith.Match == datastore.ByFederatesWith_MATCH_SUBSET {
				// The given selectors are a subset (up to all) of an entry
				// or a subset of the given selectors match the total selectors of an entry.
				// Adding these together results in an overly optimistic node list which is culled later.
				for id := range ids {
					subset[id] = true
				}
			} else {
				return nil, fmt.Errorf("unhandled match behavior %q", req.ByFederatesWith.Match)
			}
		}
		if len(subset) > 0 {
			idMaps = append(idMaps, subset)
		}
	}

	if req.BySelectors != nil {
		subset := map[string]bool{}
		for _, sel := range req.BySelectors.Selectors {
			ids, err := s.entrySelMap(ctx, rev, sel)
			if err != nil {
				return nil, err
			}
			if req.BySelectors.Match == datastore.BySelectors_MATCH_EXACT {
				// The given selectors are the complete set for an entry to match
				idMaps = append(idMaps, ids)
			} else if req.BySelectors.Match == datastore.BySelectors_MATCH_SUBSET {
				// The given selectors are a subset (up to all) of an entry
				// or a subset of the given selectors match the total selectors of an entry.
				// Adding these together results in an overly optimistic node list which is culled later.
				for id := range ids {
					subset[id] = true
				}
			} else {
				return nil, fmt.Errorf("unhandled match behavior %q", req.BySelectors.Match)
			}
		}
		if len(subset) > 0 {
			idMaps = append(idMaps, subset)
		}
	}

	count := len(idMaps)
	if count > 1 {
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

	key := nodeKey("")
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 5 || p.Token[0:2] != entryPrefix {
				return nil, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			// TODO one bit larger than token
			key = fmt.Sprintf("%s!", p.Token)
		}
	}

	lastKey := ""
	resp := &datastore.ListRegistrationEntriesResponse{}
	if count > 0 {
		// Create a sorted list of node IDs from resulting filter map
		ids := []string{}
		for id := range idMaps[0] {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		// Get the specified list of registered entries from the query filters
		// NOTE: looping will not scale to desired limits; these should be served from cache
		// An interim approach would be to send batches of reads as a single transaction
		// Batches would be PageSize if paginated or a few hundred to a thousand at a time
		var i int64 = 1
		for _, id := range ids {
			if p != nil && len(p.Token) > 0 && entryKey(id) < key {
				continue
			}

			res, err := s.Store.Get(ctx, &store.GetRequest{Key: entryKey(id), Revision: rev})
			if err != nil {
				return nil, err
			}

			if len(res.Kvs) == 1 {
				e := &common.RegistrationEntry{}
				if err = proto.Unmarshal(res.Kvs[0].Value, e); err != nil {
					return nil, err
				}
				if req.BySelectors != nil {
					if !s.entrySelectorMatch(e, req.BySelectors) {
						continue
					}
				}
				resp.Entries = append(resp.Entries, e)
				lastKey = entryKey(e.EntryId)

				if limit > 0 && i == limit {
					break
				}
			}
			i++
		}
	} else {
		// No filters, get all registered entries up to limit
		res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: allEntries, Limit: limit, Revision: rev})
		if err != nil {
			return nil, err
		}

		for _, kv := range res.Kvs {
			e := &common.RegistrationEntry{}
			if err = proto.Unmarshal(kv.Value, e); err != nil {
				return nil, err
			}
			resp.Entries = append(resp.Entries, e)
			lastKey = kv.Key
		}
	}

	if p != nil {
		p.Token = ""
		// Set token only if there may be more items than returned
		// if len(resp.Entries) == int(p.PageSize) {

		// NOTE: the SQL implementation appears to set the token on the last page regardless
		if len(resp.Entries) > 0 {
			p.Token = lastKey
		}
		resp.Pagination = p
	}

	return resp, nil
}

// PruneRegistrationEntries deletes all entries which have expired before the given time
func (s *Shim) PruneRegistrationEntries(ctx context.Context,
	req *datastore.PruneRegistrationEntriesRequest) (*datastore.PruneRegistrationEntriesResponse, error) {
	if s.Store == nil {
		return s.DataStore.PruneRegistrationEntries(ctx, req)
	}

	// Start range with EntryExpiry of 1 to exclude entries with no expiration (EntryExpiry = 0)
	start := fmt.Sprintf("%s%s%s%s%d", indexKeyID, entryPrefix, EXP, delim, 1)
	// End range with requested expiration time
	end := fmt.Sprintf("%s%s%s%s%d", indexKeyID, entryPrefix, EXP, delim, req.ExpiresBefore)

	// Get matching index keys
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: start, End: end})
	if err != nil {
		return nil, err
	}

	// Delete expired registered entries
	for _, kv := range res.Kvs {
		id, err := entryExpID(kv.Key)
		if err != nil {
			return nil, err
		}

		_, err = s.DeleteRegistrationEntry(ctx, &datastore.DeleteRegistrationEntryRequest{EntryId: id})
		if err != nil {
			return nil, err
		}
	}

	return &datastore.PruneRegistrationEntriesResponse{}, nil
}

// UpdateRegistrationEntry updates an existing registration entry
func (s *Shim) UpdateRegistrationEntry(ctx context.Context,
	req *datastore.UpdateRegistrationEntryRequest) (*datastore.UpdateRegistrationEntryResponse, error) {

	if s.Store == nil {
		return s.DataStore.UpdateRegistrationEntry(ctx, req)
	}

	// TODO: Validations should be done in the ProtoBuf level [https://github.com/spiffe/spire/issues/44]
	if err := validateRegistrationEntryForUpdate(req.Entry, req.Mask); err != nil {
		return nil, err
	}

	// Get current registration entry and version for transactional integrity
	fe, ver, err := s.fetchEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: req.Entry.EntryId})
	if err != nil {
		return nil, err
	}
	if fe.Entry == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}
	e := fe.Entry
	r := req.Entry
	changed := false

	// KeyValue arrays for delete and create operations
	del := []*store.KeyValue{}
	put := []*store.KeyValue{}

	// If values changed then update entry and index keys
	if (req.Mask == nil || req.Mask.SpiffeId) && e.SpiffeId != r.SpiffeId {
		del = append(del, &store.KeyValue{Key: entrySidKey(e.EntryId, e.SpiffeId)})
		put = append(put, &store.KeyValue{Key: entrySidKey(e.EntryId, r.SpiffeId)})
		e.SpiffeId = r.SpiffeId
		changed = true
	}

	if (req.Mask == nil || req.Mask.ParentId) && e.ParentId != r.ParentId {
		del = append(del, &store.KeyValue{Key: entryPidKey(e.EntryId, e.ParentId)})
		put = append(put, &store.KeyValue{Key: entryPidKey(e.EntryId, r.ParentId)})
		e.ParentId = r.ParentId
		changed = true
	}

	if (req.Mask == nil || req.Mask.EntryExpiry) && e.EntryExpiry != r.EntryExpiry {
		// Index record for expiry contains selectors as content to simplify listing by selector
		// NOTE: NodeSelectors works here as SpiffeId and EntryId are both strings
		sel := &datastore.NodeSelectors{SpiffeId: e.EntryId, Selectors: e.Selectors}
		v, err := proto.Marshal(sel)
		if err != nil {
			return nil, err
		}

		del = append(del, &store.KeyValue{Key: entryExpKey(e.EntryId, e.EntryExpiry)})
		put = append(put, &store.KeyValue{Key: entryExpKey(e.EntryId, r.EntryExpiry), Value: v})
		e.EntryExpiry = r.EntryExpiry
		changed = true
	}

	if req.Mask == nil || req.Mask.Selectors {
		// Build a list of existing index keys to delete any unused ones
		delKeys := map[string]bool{}
		if e.Selectors != nil {
			for _, sel := range e.Selectors {
				delKeys[nodeSelKey(e.EntryId, sel)] = true
			}
		}

		// Add index records for new selectors
		for _, sel := range e.Selectors {
			key := entrySelKey(e.EntryId, sel)
			put = append(put, &store.KeyValue{Key: key})

			// No need to delete the key if we are changing it
			if _, ok := delKeys[key]; ok {
				delete(delKeys, key)
			}
		}

		// Delete remaining unused index keys
		if len(delKeys) > 0 {
			for k := range delKeys {
				del = append(del, &store.KeyValue{Key: k})
			}
		}

		put = append(put, &store.KeyValue{Key: entryExpKey(e.EntryId, e.EntryExpiry)})
		e.Selectors = r.Selectors
		changed = true
	}

	if req.Mask == nil || req.Mask.FederatesWith {
		// Build a map of previous domains to check for changes
		previous := map[string]bool{}
		if e.FederatesWith != nil {
			for _, domain := range e.FederatesWith {
				previous[domain] = true
			}
		}

		// Add new domains
		for _, domain := range r.FederatesWith {
			if _, ok := previous[domain]; ok {
				delete(previous, domain)
			} else {
				put = append(put, &store.KeyValue{Key: entryFedByDomainKey(e.EntryId, domain)})
			}
		}

		// Delete remaining unused index keys
		if len(previous) > 0 {
			for domain := range previous {
				del = append(del, &store.KeyValue{Key: entryFedByDomainKey(e.EntryId, domain)})
			}
		}

		e.FederatesWith = r.FederatesWith
		changed = true
	}

	if (req.Mask == nil || req.Mask.Admin) && e.Admin != r.Admin {
		changed = true
		e.Admin = r.Admin
	}

	if req.Mask == nil || req.Mask.DnsNames {
		changed = true
		e.DnsNames = r.DnsNames
	}

	if (req.Mask == nil || req.Mask.Downstream) && e.Downstream != r.Downstream {
		changed = true
		e.Downstream = r.Downstream
	}

	if (req.Mask == nil || req.Mask.Ttl) && e.Ttl != r.Ttl {
		changed = true
		e.Ttl = r.Ttl
	}

	if !changed {
		return &datastore.UpdateRegistrationEntryResponse{Entry: e}, nil
	}

	// Increment every time the entry is updated
	// NOTE: we could use the store's record version for this field
	e.RevisionNumber++

	// Build the entry record key and value
	k := entryKey(e.EntryId)
	v, err := proto.Marshal(e)
	if err != nil {
		// Return gRPC InvalidArgument error?
		return nil, err
	}

	// Add put record for updated entry, ensuring version has not changed since fetching above
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

	return &datastore.UpdateRegistrationEntryResponse{Entry: e}, nil
}

func validateRegistrationEntry(entry *common.RegistrationEntry) error {
	if entry == nil {
		return status.Error(codes.InvalidArgument, "store-etcd: invalid request: missing registered entry")
	}

	if entry.Selectors == nil || len(entry.Selectors) == 0 {
		return status.Error(codes.InvalidArgument, "store-etcd: invalid registration entry: missing selector list")
	}

	if len(entry.SpiffeId) == 0 {
		return status.Error(codes.InvalidArgument, "store-etcd: invalid registration entry: missing SPIFFE ID")
	}

	if entry.Ttl < 0 {
		return status.Error(codes.InvalidArgument, "store-etcd: invalid registration entry: TTL is not set")
	}

	return nil
}

func validateRegistrationEntryForUpdate(entry *common.RegistrationEntry, mask *common.RegistrationEntryMask) error {
	if entry == nil {
		return status.Error(codes.InvalidArgument, "invalid request: missing registered entry")
	}

	if (mask == nil || mask.Selectors) && (entry.Selectors == nil || len(entry.Selectors) == 0) {
		return status.Error(codes.InvalidArgument, "invalid registration entry: missing selector list")
	}

	if (mask == nil || mask.SpiffeId) && entry.SpiffeId == "" {
		return status.Error(codes.InvalidArgument, "invalid registration entry: missing SPIFFE ID")
	}

	if (mask == nil || mask.Ttl) && (entry.Ttl < 0) {
		return status.Error(codes.InvalidArgument, "invalid registration entry: TTL is not set")
	}

	return nil
}

func newRegistrationEntryID() (string, error) {
	u, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

// entryKey returns a string formatted key for a registered entry
func entryKey(id string) string {
	// e.g. "E|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a"
	return fmt.Sprintf("%s%s", entryPrefix, id)
}

// entryExpKey returns a string formatted key for a registered entry indexed by expiry in seconds.
// e.g. "IE|EXP|1611907252|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a"
// NOTE: %d without leading zeroes for time.Unix will work for the next ~250 years
func entryExpKey(id string, exp int64) string {
	return fmt.Sprintf("%s%s%s%s%d%s%s", indexKeyID, entryPrefix, EXP, delim, exp, delim, id)
}

// entryExpID returns the registered entry id from the given entry expiry (EXP) index key.
func entryExpID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid entry expiry index key: %s", key)
	}
	return items[3], nil
}

// entryExpMap returns a map of registered entry ids expiring before or after the given expiry.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) entryExpMap(ctx context.Context, rev, exp int64, after bool) (map[string]bool, error) {
	// Set range to all index keys after or before the given time
	key, end := "", ""
	if after {
		key = fmt.Sprintf("%s%s%s%s%d", indexKeyID, entryPrefix, EXP, delim, exp)
		end = fmt.Sprintf("%s%s%s%s", indexKeyID, entryPrefix, EXP, delend)
	} else {
		key = fmt.Sprintf("%s%s%s%s", indexKeyID, entryPrefix, EXP, delim)
		end = fmt.Sprintf("%s%s%s%s%d", indexKeyID, entryPrefix, EXP, delim, exp)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := entryExpID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// entryFedByDomainKey returns a string formatted key for a federated bundle domain indexed by registered entry id.
// e.g. "IE|FED|spiffe://example.org|01242e4a-4563-4bf3-b000-12345678c12a"
func entryFedByDomainKey(id, domain string) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s", indexKeyID, entryPrefix, FED, delim, domain, delim, id)
}

// entryFedByDomainID returns the registered entry id from the given federation (FED) index key.
func entryFedByDomainID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid entry federation key: %s", key)
	}
	return items[3], nil
}

// entryFedByDomainMap returns a map of registered entry ids with the given federated domain.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) entryFedByDomainMap(ctx context.Context, rev int64, domain string) (map[string]bool, error) {
	// Set range to all index keys with the given parent ID
	key := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, entryPrefix, FED, delim, domain, delim)
	end := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, entryPrefix, FED, delim, domain, delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := entryFedByDomainID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// entryPidKey returns a string formatted key for a registered entry indexed by expiry in seconds.
// e.g. "IE|PID|01242e4a-4563-4bf3-b000-12345678c12a|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a"
func entryPidKey(id, pid string) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s", indexKeyID, entryPrefix, PID, delim, pid, delim, id)
}

// entryPidID returns the registered entry id from the given entry expiry (PID) index key.
func entryPidID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid entry expiry index key: %s", key)
	}
	return items[3], nil
}

// entryPidMap returns a map of registered entry ids with the given parent ID.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) entryPidMap(ctx context.Context, rev int64, pid string) (map[string]bool, error) {
	// Set range to all index keys with the given parent ID
	key := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, entryPrefix, PID, delim, pid, delim)
	end := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, entryPrefix, PID, delim, pid, delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := entryPidID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// entrySidKey returns a string formatted key for a registered entry indexed by expiry in seconds.
// e.g. "IE|SID|01242e4a-4563-4bf3-b000-12345678c12a|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a"
func entrySidKey(id, sid string) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s", indexKeyID, entryPrefix, SID, delim, sid, delim, id)
}

// entrySidID returns the registered entry id from the given entry expiry (SID) index key.
func entrySidID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid entry expiry index key: %s", key)
	}
	return items[3], nil
}

// entrySidMap returns a map of registered entry ids with the given parent ID.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) entrySidMap(ctx context.Context, rev int64, sid string) (map[string]bool, error) {
	// Set range to all index keys with the given parent ID
	key := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, entryPrefix, SID, delim, sid, delim)
	end := fmt.Sprintf("%s%s%s%s%s%s", indexKeyID, entryPrefix, SID, delim, sid, delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := entrySidID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// entrySelKey returns a string formatted key for a registered entry indexed by selector type and value.
// e.g. "IE|TVI|a-type|a-value|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a"
func entrySelKey(id string, s *common.Selector) string {
	return fmt.Sprintf("%s%s%s%s%s%s%s%s%s", indexKeyID, entryPrefix, TVI, delim, s.Type, delim, s.Value, delim, id)
}

// entrySelID returns the registered entry id from the given type-value (TVI) index key
func entrySelID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 5 {
		return "", fmt.Errorf("invalid entry selector index key: %s", key)
	}
	return items[4], nil
}

// entrySelMap returns a map of registered entry ids by selector match.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) entrySelMap(ctx context.Context, rev int64, sel *common.Selector) (map[string]bool, error) {
	// Set range to all index keys for this type and value
	key := fmt.Sprintf("%s%s%s%s%s%s%s%s", indexKeyID, entryPrefix, TVI, delim, sel.Type, delim, sel.Value, delim)
	end := fmt.Sprintf("%s%s%s%s%s%s%s%s", indexKeyID, entryPrefix, TVI, delim, sel.Type, delim, sel.Value, delend)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Revision: rev})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := entrySelID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}

// selectorMatch verifies the entry selectors properly match the requested selectors.
//   - Exact match is true if both lists are identical
//   - Subset match is true if all requested selectors are present in the registered entry
//   - Subset match is true if all registered entry selectors are present in requested selectors
func (s *Shim) entrySelectorMatch(entry *common.RegistrationEntry, req *datastore.BySelectors) bool {
	entrySelectors := selectorMap(entry.Selectors)
	reqSelectors := selectorMap(req.Selectors)
	if req.Match == datastore.BySelectors_MATCH_EXACT {
		// Are the requested selectors the complete set of entry selectors?
		if reflect.DeepEqual(entrySelectors, reqSelectors) {
			return true
		}
	} else if req.Match == datastore.BySelectors_MATCH_SUBSET {
		// Do all request selectors exist in entry selectors?
		reqMatch := true
		for reqType, reqValue := range reqSelectors {
			entryValue, ok := entrySelectors[reqType]
			if ok {
				if entryValue != reqValue {
					reqMatch = false
				}
			} else {
				reqMatch = false
			}
		}
		// Do all entry selectors exist in request selectors?
		entryMatch := true
		for entryType, entryValue := range entrySelectors {
			reqValue, ok := reqSelectors[entryType]
			if ok {
				if reqValue != entryValue {
					entryMatch = false
				}
			} else {
				entryMatch = false
			}
		}
		// Either satisfies "match subset"
		if reqMatch || entryMatch {
			return true
		}
	} else {
		s.log.Warn(fmt.Sprintf("Unknown match %v", req.Match))
	}
	return false
}
