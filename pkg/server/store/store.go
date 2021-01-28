// Package store implements a datastore shim
package store

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-hclog"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	"github.com/spiffe/spire/pkg/common/idutil"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Shim defines a datastore shim to allow the new Store API
// to coexist with the existing datastore API during prototyping.
type Shim struct {
	datastore.DataStore
	store.Store

	log hclog.Logger
}

var storeLoaded = false

// New returns an initialized storage shim.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger) *Shim {
	return &Shim{DataStore: ds, Store: st, log: logger}
}

// AppendBundle creates or updates the given bundle
func (s *Shim) AppendBundle(ctx context.Context, req *datastore.AppendBundleRequest) (*datastore.AppendBundleResponse, error) {
	if s.Store == nil {
		return s.DataStore.AppendBundle(ctx, req)
	}

	s.log.Info("Store AppendBundle")
	bundle := req.Bundle
	id := bundle.TrustDomainId
	fr, err := s.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: id})
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
		_, err := s.UpdateBundle(ctx, &datastore.UpdateBundleRequest{Bundle: bundle})
		if err != nil {
			return nil, err
		}
		return &datastore.AppendBundleResponse{Bundle: bundle}, nil
	}
	return &datastore.AppendBundleResponse{Bundle: bundle}, nil
}

// CountBundles retrieves the total number of bundles in the store.
func (s *Shim) CountBundles(ctx context.Context, req *datastore.CountBundlesRequest) (resp *datastore.CountBundlesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.CountBundles(ctx, req)
	}

	s.log.Info("Store CountBundles")
	res, err := s.Store.Get(ctx, &store.GetRequest{Key: "b|", End: "c", CountOnly: true})
	if err != nil {
		return nil, err
	}
	resp = &datastore.CountBundlesResponse{Bundles: int32(res.Total)}
	return
}

// CreateBundle stores the given bundle
func (s *Shim) CreateBundle(ctx context.Context, req *datastore.CreateBundleRequest) (resp *datastore.CreateBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.CreateBundle(ctx, req)
	}

	s.log.Info("Store CreateBundle")
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
// NOTE: A returned bundle is currently not consumed by any caller, so it is not provided.
func (s *Shim) DeleteBundle(ctx context.Context, req *datastore.DeleteBundleRequest) (resp *datastore.DeleteBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.DeleteBundle(ctx, req)
	}

	s.log.Info("Store DeleteBundle")
	trustDomainID, err := idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	_, err = s.Store.Delete(ctx, &store.DeleteRequest{
		Ranges: []*store.Range{{Key: fmt.Sprintf("b|%s", trustDomainID)}},
	})
	if err == nil {
		resp = &datastore.DeleteBundleResponse{
			Bundle: &common.Bundle{},
		}
	}
	return
}

// FetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) FetchBundle(ctx context.Context, req *datastore.FetchBundleRequest) (resp *datastore.FetchBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.FetchBundle(ctx, req)
	}

	s.log.Info("Store FetchBundle")
	trustDomainID, err := idutil.NormalizeSpiffeID(req.TrustDomainId, idutil.AllowAnyTrustDomain())
	resp = &datastore.FetchBundleResponse{}
	res, err := s.Store.Get(ctx, &store.GetRequest{
		Key: fmt.Sprintf("b|%s", trustDomainID),
	})
	if err == nil {
		if len(res.Kvs) == 1 {
			bundle := &common.Bundle{}
			err = proto.Unmarshal(res.Kvs[0].Value, bundle)
			resp = &datastore.FetchBundleResponse{
				Bundle: bundle,
			}
		} else if len(res.Kvs) > 1 {
			return resp, fmt.Errorf("More than one bundle for %s", trustDomainID)
		}
	}
	return
}

// ListBundles retrieves an optionally paginated list of all bundles.
func (s *Shim) ListBundles(ctx context.Context, req *datastore.ListBundlesRequest) (resp *datastore.ListBundlesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListBundles(ctx, req)
	}

	s.log.Info("Store ListBundles")
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
	}
	return
}

// UpdateBundle replaces the existing bundle
// TODO apply bundle mask to update only selected fields
// Implement with opportunistic locking by checking modified version on write
func (s *Shim) UpdateBundle(ctx context.Context, req *datastore.UpdateBundleRequest) (resp *datastore.UpdateBundleResponse, err error) {
	if s.Store == nil {
		return s.DataStore.UpdateBundle(ctx, req)
	}

	s.log.Info("Store UpdateBundle")
	var v []byte
	k := fmt.Sprintf("b|%s", req.Bundle.TrustDomainId)
	v, err = proto.Marshal(req.Bundle)
	if err != nil {
		return
	}
	_, err = s.Store.Update(ctx, &store.PutRequest{
		Kvs: []*store.KeyValue{{Key: k, Value: v}},
	})
	if err == nil {
		resp = &datastore.UpdateBundleResponse{
			Bundle: req.Bundle,
		}
	}
	return
}
