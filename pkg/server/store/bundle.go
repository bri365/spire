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
func (s *Shim) AppendBundle(ctx context.Context, req *datastore.AppendBundleRequest) (*datastore.AppendBundleResponse, error) {
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
func (s *Shim) CountBundles(ctx context.Context, req *datastore.CountBundlesRequest) (*datastore.CountBundlesResponse, error) {
	if s.Store == nil {
		return s.DataStore.CountBundles(ctx, req)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: "b|", End: "c", CountOnly: true})
	if err != nil {
		return nil, err
	}
	return &datastore.CountBundlesResponse{Bundles: int32(res.Total)}, nil
}

// CreateBundle stores the given bundle
func (s *Shim) CreateBundle(ctx context.Context, req *datastore.CreateBundleRequest) (resp *datastore.CreateBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.CreateBundle(ctx, req)
	}

	var v []byte
	k := fmt.Sprintf("b|%s", req.Bundle.TrustDomainId)
	v, err = proto.Marshal(req.Bundle)
	if err != nil {
		return
	}
	_, err = s.Store.Create(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{{Key: k, Value: v}},
	})
	if err == nil {
		resp = &datastore.CreateBundleResponse{
			Bundle: req.Bundle,
		}
	}
	return
}

// DeleteBundle removes the given bundle from the store
// NOTE a returned bundle is currently not consumed by any caller, so it is not provided.
func (s *Shim) DeleteBundle(ctx context.Context, req *datastore.DeleteBundleRequest) (resp *datastore.DeleteBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.DeleteBundle(ctx, req)
	}

	trustDomainID, err := idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	if err != nil {
		return nil, err
	}

	_, err = s.Store.Delete(ctx, &store.DeleteRequest{
		Ranges: []*store.Range{{Key: fmt.Sprintf("b|%s", trustDomainID)}},
	})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteBundleResponse{Bundle: &common.Bundle{}}, nil
}

// FetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) FetchBundle(ctx context.Context, req *datastore.FetchBundleRequest) (resp *datastore.FetchBundleResponse, err error) {
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
func (s *Shim) fetchBundle(ctx context.Context, req *datastore.FetchBundleRequest) (*datastore.FetchBundleResponse, int64, error) {
	res, err := s.Store.Get(ctx, &store.GetRequest{
		Key: fmt.Sprintf("b|%s", req.TrustDomainId),
	})
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
func (s *Shim) ListBundles(ctx context.Context, req *datastore.ListBundlesRequest) (resp *datastore.ListBundlesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListBundles(ctx, req)
	}

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}

	// Start with all bundle identifiers and limit of 0 (no limit)
	key := "b|"
	end := "c|"
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			if len(p.Token) < 12 || p.Token[0:2] != "b|" {
				return nil, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			// TODO one bit larger than token
			key = fmt.Sprintf("%sA", p.Token)
		}
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Limit: limit})
	if err != nil {
		return nil, err
	}

	lastKey := ""
	resp = &datastore.ListBundlesResponse{}
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
	return
}

// PruneBundle removes expired certs and keys from a bundle
func (s *Shim) PruneBundle(ctx context.Context, req *datastore.PruneBundleRequest) (resp *datastore.PruneBundleResponse, err error) {
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
func (s *Shim) SetBundle(ctx context.Context, req *datastore.SetBundleRequest) (*datastore.SetBundleResponse, error) {
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
func (s *Shim) UpdateBundle(ctx context.Context, req *datastore.UpdateBundleRequest) (resp *datastore.UpdateBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.UpdateBundle(ctx, req)
	}

	return s.updateBundle(ctx, req, 0)
}

// updateBundle replaces the existing bundle with one or more new elements
// Implement opportunistic locking if given an object version from a previous read operation.
func (s *Shim) updateBundle(ctx context.Context, req *datastore.UpdateBundleRequest, ver int64) (resp *datastore.UpdateBundleResponse, err error) {
	id := req.Bundle.TrustDomainId
	fr, err := s.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
	if err != nil {
		return nil, err
	}
	if fr.Bundle == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
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
	k := fmt.Sprintf("b|%s", req.Bundle.TrustDomainId)
	v, err = proto.Marshal(req.Bundle)
	if err != nil {
		return
	}
	_, err = s.Store.Update(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{{Key: k, Value: v, Version: ver}},
	})
	if err == nil {
		resp = &datastore.UpdateBundleResponse{
			Bundle: req.Bundle,
		}
	}
	return
}
