// Package store implements a datastore shim
package store

import (
	"context"
	"fmt"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CountRegistrationEntries counts all registrations
func (s *Shim) CountRegistrationEntries(ctx context.Context, req *datastore.CountRegistrationEntriesRequest) (resp *datastore.CountRegistrationEntriesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.CountRegistrationEntries(ctx, req)
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: "e|", End: "f", CountOnly: true})
	if err != nil {
		return nil, err
	}
	return &datastore.CountRegistrationEntriesResponse{Entries: int32(res.Total)}, nil
}

// CreateRegistrationEntry stores the given registration entry
func (s *Shim) CreateRegistrationEntry(ctx context.Context, req *datastore.CreateRegistrationEntryRequest) (*datastore.CreateRegistrationEntryResponse, error) {
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

	// Create entry record
	k := fmt.Sprintf("e|%s", req.Entry.SpiffeId)
	v, err := proto.Marshal(req.Entry)
	if err != nil {
		return nil, err
	}
	kvs := []*store.KeyValue{{Key: k, Value: v}}

	// Create index records as needed

	_, err = s.Store.Create(ctx, &store.PutRequest{
		Kvs: kvs,
	})
	if err != nil {
		return nil, err
	}
	return &datastore.CreateRegistrationEntryResponse{Entry: req.Entry}, nil
}

// FetchRegistrationEntry fetches an existing registration by entry ID
func (s *Shim) FetchRegistrationEntry(ctx context.Context, req *datastore.FetchRegistrationEntryRequest) (resp *datastore.FetchRegistrationEntryResponse, err error) {
	if s.Store == nil {
		return s.DataStore.FetchRegistrationEntry(ctx, req)
	}

	return
}

// ListRegistrationEntries lists all registrations (pagination available)
func (s *Shim) ListRegistrationEntries(ctx context.Context, req *datastore.ListRegistrationEntriesRequest) (resp *datastore.ListRegistrationEntriesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListRegistrationEntries(ctx, req)
	}

	return
}

// UpdateRegistrationEntry updates an existing registration entry
func (s *Shim) UpdateRegistrationEntry(ctx context.Context, req *datastore.UpdateRegistrationEntryRequest) (resp *datastore.UpdateRegistrationEntryResponse, err error) {
	if s.Store == nil {
		return s.DataStore.UpdateRegistrationEntry(ctx, req)
	}

	return
}

// DeleteRegistrationEntry deletes the given registration
func (s *Shim) DeleteRegistrationEntry(ctx context.Context, req *datastore.DeleteRegistrationEntryRequest) (resp *datastore.DeleteRegistrationEntryResponse, err error) {
	if s.Store == nil {
		return s.DataStore.DeleteRegistrationEntry(ctx, req)
	}

	return
}

// PruneRegistrationEntries takes a registration entry message, and deletes all entries which have expired
// before the date in the message
func (s *Shim) PruneRegistrationEntries(ctx context.Context, req *datastore.PruneRegistrationEntriesRequest) (resp *datastore.PruneRegistrationEntriesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.PruneRegistrationEntries(ctx, req)
	}

	return
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

func newRegistrationEntryID() (string, error) {
	u, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
