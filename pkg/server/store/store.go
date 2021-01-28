// Package store implements a datastore shim
package store

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-hclog"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
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

// CreateBundle stores the given bundle
func (s *Shim) CreateBundle(ctx context.Context, req *datastore.CreateBundleRequest) (resp *datastore.CreateBundleResponse, err error) {
	if s.Store == nil {
		resp, err = s.DataStore.CreateBundle(ctx, req)
	} else {
		var v []byte
		s.log.Info("Store CreateBundle")
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
	}
	return
}

// FetchBundle retrieves the given bundle by SpiffieID
func (s *Shim) FetchBundle(ctx context.Context, req *datastore.FetchBundleRequest) (resp *datastore.FetchBundleResponse, err error) {
	if s.Store == nil {
		resp, err = s.DataStore.FetchBundle(ctx, req)
	} else {
		resp = &datastore.FetchBundleResponse{}

		res, err := s.Store.Get(ctx, &store.GetRequest{
			Key: fmt.Sprintf("b|%s", req.TrustDomainId),
		})
		if err == nil {
			if len(res.Kvs) == 1 {
				bundle := &common.Bundle{}
				err = proto.Unmarshal(res.Kvs[0].Value, bundle)
				resp.Bundle = bundle
			} else if len(res.Kvs) > 1 {
				return resp, fmt.Errorf("More than one bundle for %s", req.TrustDomainId)
			}
		}
	}
	return
}
