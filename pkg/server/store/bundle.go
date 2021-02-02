// Package store implements a datastore shim
package store

import (
	"context"
	"fmt"
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
	key := bundleKey("")
	end := allBundles
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

	// build the bundle key and value
	k := bundleKey(req.Bundle.TrustDomainId)
	v, err := proto.Marshal(req.Bundle)
	if err != nil {
		return nil, err
	}

	// Add key, value, and not present to ensure bundle doesn't already exist
	kvs := []*store.KeyValue{{Key: k, Value: v, Compare: store.Compare_NOT_PRESENT}}

	// One put operation for this transaction
	elements := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: kvs}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: elements})
	if err != nil {
		st := status.Convert(err)
		if st.Code() == codes.Aborted {
			return nil, status.Error(codes.AlreadyExists, "store-etcd: record already exists")
		}
		return nil, err
	}

	return &datastore.CreateBundleResponse{Bundle: req.Bundle}, nil
}

// DeleteBundle removes the given bundle from the store
// NOTE a returned bundle is currently not consumed by any caller, so it is not provided.
func (s *Shim) DeleteBundle(ctx context.Context,
	req *datastore.DeleteBundleRequest) (*datastore.DeleteBundleResponse, error) {

	if s.Store == nil {
		return s.DataStore.DeleteBundle(ctx, req)
	}

	trustDomainID, err := idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	// Add key, value, and compare present to ensure bundle exists
	kvs := []*store.KeyValue{{Key: bundleKey(trustDomainID), Compare: store.Compare_PRESENT}}

	// One put operation for this transaction
	elements := []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: kvs}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: elements})
	if err != nil {
		st := status.Convert(err)
		if st.Code() == codes.Aborted {
			return nil, status.Error(codes.NotFound, "store-etcd: record not found")
		}
		return nil, err
	}

	return &datastore.DeleteBundleResponse{Bundle: &common.Bundle{}}, nil
}

// FetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) FetchBundle(ctx context.Context,
	req *datastore.FetchBundleRequest) (resp *datastore.FetchBundleResponse, err error) {

	if s.Store == nil {
		return s.DataStore.FetchBundle(ctx, req)
	}

	req.TrustDomainId, err = idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	resp, _, err = s.fetchBundle(ctx, req)

	return
}

// fetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) fetchBundle(ctx context.Context,
	req *datastore.FetchBundleRequest) (*datastore.FetchBundleResponse, int64, error) {

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: bundleKey(req.TrustDomainId)})
	if err != nil {
		return nil, 0, err
	}

	var ver int64
	resp := &datastore.FetchBundleResponse{}
	if len(res.Kvs) == 1 {
		ver = res.Kvs[0].Version
		bundle := &common.Bundle{}
		err = proto.Unmarshal(res.Kvs[0].Value, bundle)
		if err != nil {
			return nil, 0, err
		}
		resp.Bundle = bundle
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("More than one bundle for %s", req.TrustDomainId)
	}
	return resp, ver, nil
}

// ListBundles retrieves an optionally paginated list of all bundles.
func (s *Shim) ListBundles(ctx context.Context,
	req *datastore.ListBundlesRequest) (*datastore.ListBundlesResponse, error) {

	if s.Store == nil {
		return s.DataStore.ListBundles(ctx, req)
	}

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}

	// Start with all bundle identifiers and limit of 0 (no limit)
	key := bundleKey("")
	end := allBundles
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 12 || p.Token[0:2] != key {
				return nil, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			// TODO one bit larger than token
			key = fmt.Sprintf("%s ", p.Token)
		}
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Limit: limit})
	if err != nil {
		return nil, err
	}

	lastKey := ""
	resp := &datastore.ListBundlesResponse{}
	for _, kv := range res.Kvs {
		b := &common.Bundle{}
		err = proto.Unmarshal(kv.Value, b)
		if err != nil {
			return nil, err
		}
		resp.Bundles = append(resp.Bundles, b)
		lastKey = kv.Key
	}

	if p != nil {
		p.Token = ""
		// Set token only if there may be more bundles than returned
		if len(resp.Bundles) == int(p.PageSize) {
			p.Token = lastKey
		}
		resp.Pagination = p
	}
	return resp, nil
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
	newBundle, changed, err := bundleutil.PruneBundle(fr.Bundle, time.Unix(req.ExpiresBefore, 0), s.log)
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
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}

	if ver > 0 && ver != current {
		return nil, status.Error(codes.Aborted, "store-etcd: version not found")
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
	k := bundleKey(req.Bundle.TrustDomainId)
	v, err = proto.Marshal(req.Bundle)
	if err != nil {
		return nil, err
	}

	// Ensure bundle version equals previous get version to commit transaction
	kvs := []*store.KeyValue{{Key: k, Value: v, Version: current, Compare: store.Compare_EQUALS}}

	// One put operation for this transaction
	elements := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: kvs}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: elements})
	if err != nil {
		return nil, err
	}
	return &datastore.UpdateBundleResponse{Bundle: req.Bundle}, nil
}

// bundleKey returns a string formatted key for a bundle
func bundleKey(id string) string {
	// e.g. "B|spiffie://example.com"
	return fmt.Sprintf("%s%s", bundlePrefix, id)
}
