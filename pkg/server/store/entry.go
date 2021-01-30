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
func (s *Shim) CountRegistrationEntries(ctx context.Context, req *datastore.CountRegistrationEntriesRequest) (*datastore.CountRegistrationEntriesResponse, error) {
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

	// Create entry record key and value
	k := entryKey(req.Entry.SpiffeId)
	v, err := proto.Marshal(req.Entry)
	if err != nil {
		return nil, err
	}

	// Create a list of items to add, starting with the registered entry
	kvs := []*store.KeyValue{{Key: k, Value: v}}

	// Create index records for TODO

	_, err = s.Store.Create(ctx, &store.PutRequest{Kvs: kvs})
	if err != nil {
		return nil, err
	}
	return &datastore.CreateRegistrationEntryResponse{Entry: req.Entry}, nil
}

// DeleteRegistrationEntry deletes the given registration
func (s *Shim) DeleteRegistrationEntry(ctx context.Context, req *datastore.DeleteRegistrationEntryRequest) (*datastore.DeleteRegistrationEntryResponse, error) {
	if s.Store == nil {
		return s.DataStore.DeleteRegistrationEntry(ctx, req)
	}

	_, err := s.Store.Delete(ctx, &store.DeleteRequest{Ranges: []*store.Range{{Key: entryKey(req.EntryId)}}})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteRegistrationEntryResponse{Entry: &common.RegistrationEntry{}}, nil
}

// FetchRegistrationEntry fetches an existing registration by entry ID
func (s *Shim) FetchRegistrationEntry(ctx context.Context, req *datastore.FetchRegistrationEntryRequest) (resp *datastore.FetchRegistrationEntryResponse, err error) {
	if s.Store == nil {
		return s.DataStore.FetchRegistrationEntry(ctx, req)
	}

	resp, _, err = s.fetchEntry(ctx, req)

	return
}

// fetchRegistrationEntry fetches an existing registration by entry ID
func (s *Shim) fetchEntry(ctx context.Context, req *datastore.FetchRegistrationEntryRequest) (*datastore.FetchRegistrationEntryResponse, int64, error) {
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
func (s *Shim) ListRegistrationEntries(ctx context.Context, req *datastore.ListRegistrationEntriesRequest) (resp *datastore.ListRegistrationEntriesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.ListRegistrationEntries(ctx, req)
	}

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}
	if req.BySelectors != nil && len(req.BySelectors.Selectors) == 0 {
		return nil, status.Error(codes.InvalidArgument, "cannot list by empty selector set")
	}

	// TODO

	return
}

// PruneRegistrationEntries takes a registration entry message, and deletes all entries which have expired
// before the date in the message
func (s *Shim) PruneRegistrationEntries(ctx context.Context, req *datastore.PruneRegistrationEntriesRequest) (resp *datastore.PruneRegistrationEntriesResponse, err error) {
	if s.Store == nil {
		return s.DataStore.PruneRegistrationEntries(ctx, req)
	}

	// TODO

	return
}

// UpdateRegistrationEntry updates an existing registration entry
func (s *Shim) UpdateRegistrationEntry(ctx context.Context, req *datastore.UpdateRegistrationEntryRequest) (resp *datastore.UpdateRegistrationEntryResponse, err error) {
	if s.Store == nil {
		return s.DataStore.UpdateRegistrationEntry(ctx, req)
	}

	// TODO

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

// entryKey returns a string formatted key for a registered entry
func entryKey(id string) string {
	// e.g. "E|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a"
	return fmt.Sprintf("%s%s", entryPrefix, id)
}
