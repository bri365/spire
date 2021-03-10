// Package store implements a datastore shim with the proposed new store interface.
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/common/idutil"
	"github.com/spiffe/spire/pkg/common/protoutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AppendBundle creates or updates the given bundle
func (s *Shim) AppendBundle(ctx context.Context,
	req *datastore.AppendBundleRequest) (*datastore.AppendBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.AppendBundle(ctx, req)
	}

	bundle := req.Bundle
	id := bundle.TrustDomainId

	// TODO consider getting the current bundle from cache
	fr, ver, err := s.fetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
	if err != nil {
		return nil, err
	}

	if fr.Bundle == nil {
		_, err := s.CreateBundle(ctx, &datastore.CreateBundleRequest{Bundle: bundle})
		if err != nil {
			return nil, err
		}
		return &datastore.AppendBundleResponse{Bundle: bundle}, nil
	}

	bundle, changed := bundleutil.MergeBundles(fr.Bundle, bundle)
	if changed {
		_, err := s.updateBundle(ctx, &datastore.UpdateBundleRequest{Bundle: bundle}, ver)
		if err != nil {
			return nil, err
		}
		return &datastore.AppendBundleResponse{Bundle: bundle}, nil
	}

	return &datastore.AppendBundleResponse{Bundle: bundle}, nil
}

// CountBundles retrieves the total number of bundles in the store.
func (s *Shim) CountBundles(ctx context.Context,
	req *datastore.CountBundlesRequest) (*datastore.CountBundlesResponse, error) {

	if s.Store == nil {
		return s.DataStore.CountBundles(ctx, req)
	}

	// Set range to all bundle keys
	key := BundleKey("")
	end := AllBundles
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, CountOnly: true})
	if err != nil {
		return nil, err
	}
	return &datastore.CountBundlesResponse{Bundles: int32(res.Total)}, nil
}

// CreateBundle stores the given bundle
func (s *Shim) CreateBundle(ctx context.Context,
	req *datastore.CreateBundleRequest) (*datastore.CreateBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.CreateBundle(ctx, req)
	}

	var err error
	req.Bundle.TrustDomainId, err = idutil.NormalizeSpiffeID(req.Bundle.TrustDomainId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	// build the bundle key and value
	b := req.Bundle
	k := BundleKey(b.TrustDomainId)
	v, err := proto.Marshal(req.Bundle)
	if err != nil {
		return nil, err
	}

	// Add key, value, and not present to ensure bundle doesn't already exist
	kvs := []*store.KeyValue{{Key: k, Value: v, Compare: store.Compare_NOT_PRESENT}}

	// One put operation for this transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: kvs}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		st := status.Convert(err)
		if st.Code() == codes.Aborted {
			return nil, status.Error(codes.AlreadyExists, "store-etcd: record already exists")
		}
		return nil, err
	}

	return &datastore.CreateBundleResponse{Bundle: b}, nil
}

// DeleteBundle removes the given bundle from the store
func (s *Shim) DeleteBundle(ctx context.Context,
	req *datastore.DeleteBundleRequest) (*datastore.DeleteBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.DeleteBundle(ctx, req)
	}

	id, err := idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	// Get current bundle and version for transactional integrity
	fb, ver, err := s.fetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
	if err != nil {
		return nil, err
	}
	if fb.Bundle == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: bundle not found (DB)")
	}
	b := fb.Bundle

	// Verify no registered entries are federated with this trust domain
	gr, err := s.Store.Get(ctx, &store.GetRequest{
		Key: entryFedByDomainKey("", id),
		End: entryFedByDomainEnd(id),
	})
	if len(gr.Kvs) > 0 {
		if req.Mode == datastore.DeleteBundleRequest_RESTRICT {
			// Default behavior
			msg := fmt.Sprintf("store-etcd: cannot delete bundle; federated with %d registration entries", len(gr.Kvs))
			return nil, status.Error(codes.NotFound, msg)
		}

		for _, kv := range gr.Kvs {
			entryID, err := entryFedByDomainID(kv.Key)
			if err != nil {
				return nil, err
			}

			// Delete or remove the bundle from this registered entry as requested
			// NOTE: for performance reasons at scale, these can be collected in
			// transactions of a few hundred operations each.
			if req.Mode == datastore.DeleteBundleRequest_DELETE {
				_, err = s.DeleteRegistrationEntry(ctx, &datastore.DeleteRegistrationEntryRequest{EntryId: entryID})
				if err != nil {
					return nil, err
				}
			} else if req.Mode == datastore.DeleteBundleRequest_DISSOCIATE {
				fe, _, err := s.fetchEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: entryID})
				if err != nil {
					return nil, err
				}

				fe.Entry.FederatesWith = removeString(fe.Entry.FederatesWith, id)

				_, err = s.UpdateRegistrationEntry(ctx, &datastore.UpdateRegistrationEntryRequest{
					Entry: fe.Entry,
					Mask:  &common.RegistrationEntryMask{FederatesWith: true},
				})
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// Build delete list, ensuring bundle version matches read from above
	del := []*store.KeyValue{{
		Key:     BundleKey(id),
		Compare: store.Compare_EQUALS,
		Version: ver,
	}}

	// One delete operation for this transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: del}}

	// Invalidate cache entry here to prevent race condition with async watcher
	if s.c.bundleCacheInvalidate {
		s.removeBundleCacheEntry(id)
	}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		st := status.Convert(err)
		if st.Code() == codes.Aborted {
			return nil, status.Error(codes.Aborted, "store-etcd: version changed")
		}
		return nil, err
	}

	return &datastore.DeleteBundleResponse{Bundle: b}, nil
}

// FetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) FetchBundle(ctx context.Context,
	req *datastore.FetchBundleRequest) (resp *datastore.FetchBundleResponse, err error) {

	if s.Store == nil {
		return s.DataStore.FetchBundle(ctx, req)
	}

	req.TrustDomainId, err = idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return
	}

	bundle := s.fetchBundleCacheEntry(req.TrustDomainId)
	if bundle != nil {
		resp = &datastore.FetchBundleResponse{Bundle: bundle}
		// TODO fall through on cache miss?
		return
	}

	resp, _, err = s.fetchBundle(ctx, req)
	if resp.Bundle == nil {
		return
	}

	return
}

// fetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) fetchBundle(ctx context.Context,
	req *datastore.FetchBundleRequest) (*datastore.FetchBundleResponse, int64, error) {

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: BundleKey(req.TrustDomainId)})
	if err != nil {
		return nil, 0, err
	}

	var ver int64
	resp := &datastore.FetchBundleResponse{}
	if len(res.Kvs) == 1 {
		ver = res.Kvs[0].Version
		b := &common.Bundle{}
		err = proto.Unmarshal(res.Kvs[0].Value, b)
		if err != nil {
			return nil, 0, err
		}
		resp.Bundle = b
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("More than one bundle for %s", req.TrustDomainId)
	}
	return resp, ver, nil
}

// ListBundles retrieves an optionally paginated list of all bundles.
func (s *Shim) ListBundles(ctx context.Context,
	req *datastore.ListBundlesRequest) (resp *datastore.ListBundlesResponse, err error) {

	if s.Store == nil {
		return s.DataStore.ListBundles(ctx, req)
	}

	resp, _, err = s.listBundles(ctx, 0, req)

	return
}

// listBundles retrieves an optionally paginated list of bundles.
// Store revision is accepted and returned for consistency across paginated calls.
func (s *Shim) listBundles(ctx context.Context, rev int64,
	req *datastore.ListBundlesRequest) (*datastore.ListBundlesResponse, int64, error) {

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}

	resp := &datastore.ListBundlesResponse{}
	lastKey := ""

	// Start with all bundle identifiers and limit of 0 (no limit)
	key := BundleKey(" ")
	end := AllBundles
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 12 || p.Token[0:2] != BundlePrefix {
				return nil, 0, status.Errorf(codes.InvalidArgument, "invalid token '%s'", p.Token)
			}
			key = stringPlusOne(p.Token)
		}
		p.Token = ""
		resp.Pagination = p
	}

	// Serve from cache if available and revision is acceptable
	if s.c.initialized && s.c.bundleCacheEnabled {
		s.c.mu.RLock()
		if rev == 0 || rev == s.c.storeRevision {
			index := s.cacheIndexFromKey(key)
			if limit > 0 && limit < int64(len(index)) {
				index = index[:limit]
			}
			resp.Bundles = make([]*common.Bundle, len(index))
			for i, id := range index {
				resp.Bundles[i] = s.c.bundles[id]
				lastKey = id
				if limit > 0 && int64(i) == limit-1 {
					break
				}
			}
			if p != nil && len(resp.Bundles) > 0 {
				p.Token = BundleKey(lastKey)
			}
			s.c.mu.RUnlock()
			return resp, s.c.storeRevision, nil
		}
		s.c.mu.RUnlock()
	}

	// Cache not available or does not support the requested rev
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Limit: limit, Revision: rev})
	if err != nil {
		return nil, 0, err
	}

	for _, kv := range res.Kvs {
		b := &common.Bundle{}
		err = proto.Unmarshal(kv.Value, b)
		if err != nil {
			return nil, 0, err
		}
		resp.Bundles = append(resp.Bundles, b)
		lastKey = kv.Key
	}

	if p != nil && len(resp.Bundles) > 0 {
		// Note: In the event the total number of items exactly equals the page size,
		// there may be one extra list call that returns no items.
		// This behavior is used in the cache load bundles function.
		p.Token = lastKey
	}
	return resp, res.Revision, nil
}

// PruneBundle removes expired certs and keys from a bundle
func (s *Shim) PruneBundle(ctx context.Context,
	req *datastore.PruneBundleRequest) (*datastore.PruneBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.PruneBundle(ctx, req)
	}

	id := req.TrustDomainId
	fr, ver, err := s.fetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
	if err != nil {
		return nil, err
	}

	if fr.Bundle == nil {
		// No bundle to prune
		return &datastore.PruneBundleResponse{}, nil
	}

	// Prune
	newBundle, changed, err := bundleutil.PruneBundle(fr.Bundle, time.Unix(req.ExpiresBefore, 0), s.Log)
	if err != nil {
		return nil, fmt.Errorf("prune failed: %v", err)
	}

	// Update only if bundle was modified
	if changed {
		_, err := s.updateBundle(ctx, &datastore.UpdateBundleRequest{Bundle: newBundle}, ver)
		if err != nil {
			return nil, fmt.Errorf("unable to write new bundle: %v", err)
		}
	}

	return &datastore.PruneBundleResponse{BundleChanged: changed}, nil
}

// SetBundle creates or updates the given bundle
func (s *Shim) SetBundle(ctx context.Context,
	req *datastore.SetBundleRequest) (*datastore.SetBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.SetBundle(ctx, req)
	}

	bundle := req.Bundle
	id := bundle.TrustDomainId
	fr, ver, err := s.fetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
	if err != nil {
		return nil, err
	}

	if fr.Bundle == nil {
		_, err := s.CreateBundle(ctx, &datastore.CreateBundleRequest{Bundle: bundle})
		if err != nil {
			return nil, err
		}
		return &datastore.SetBundleResponse{Bundle: bundle}, nil
	}

	_, err = s.updateBundle(ctx, &datastore.UpdateBundleRequest{Bundle: bundle}, ver)
	if err != nil {
		return nil, err
	}

	return &datastore.SetBundleResponse{Bundle: bundle}, nil
}

// UpdateBundle replaces the existing bundle with new bundle elements
func (s *Shim) UpdateBundle(ctx context.Context,
	req *datastore.UpdateBundleRequest) (*datastore.UpdateBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.UpdateBundle(ctx, req)
	}

	return s.updateBundle(ctx, req, 0)
}

// updateBundle replaces the existing bundle with one or more new elements
// Implement opportunistic locking if given an object version from a previous read operation.
func (s *Shim) updateBundle(ctx context.Context,
	req *datastore.UpdateBundleRequest, ver int64) (*datastore.UpdateBundleResponse, error) {

	// Get current bundle to update
	id := req.Bundle.TrustDomainId
	fr, current, err := s.fetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
	if err != nil {
		return nil, err
	}

	if fr.Bundle == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: bundle not found (UB)")
	}

	if ver > 0 && ver != current {
		return nil, status.Error(codes.Aborted, "store-etcd: version not found (UB)")
	}

	m := req.InputMask
	if m == nil {
		m = protoutil.AllTrueCommonBundleMask
	}

	if m.RefreshHint {
		fr.Bundle.RefreshHint = req.Bundle.RefreshHint
	}

	if m.RootCas {
		fr.Bundle.RootCas = req.Bundle.RootCas
	}

	if m.JwtSigningKeys {
		fr.Bundle.JwtSigningKeys = req.Bundle.JwtSigningKeys
	}

	var v []byte
	k := BundleKey(req.Bundle.TrustDomainId)
	v, err = proto.Marshal(req.Bundle)
	if err != nil {
		return nil, err
	}

	// Ensure bundle version equals previous get version to commit transaction
	kvs := []*store.KeyValue{{Key: k, Value: v, Version: current, Compare: store.Compare_EQUALS}}

	// One put operation for this transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: kvs}}

	// Invalidate cache entry here to prevent race condition with async watcher
	if s.c.bundleCacheInvalidate {
		s.removeBundleCacheEntry(id)
	}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		return nil, err
	}

	return &datastore.UpdateBundleResponse{Bundle: req.Bundle}, nil
}

// IsBundleKey returns true if the given key is a properly formatted bundle key.
func IsBundleKey(key string) bool {
	items := strings.Split(key, Delim)
	if len(items) == 2 && items[0] == BundleKeyID {
		return true
	}
	return false
}

// BundleKey returns a string formatted key for a bundle
func BundleKey(id string) string {
	// e.g. "B|spiffie://example.com"
	return fmt.Sprintf("%s%s", BundlePrefix, id)
}

// bundleIDFromKey returns the bundle id from the given bundle key.
func bundleIDFromKey(key string) (string, error) {
	items := strings.Split(key, Delim)
	if len(items) != 2 || items[0] != BundleKeyID {
		return "", fmt.Errorf("invalid bundle key: %s", key)
	}
	return items[1], nil
}
