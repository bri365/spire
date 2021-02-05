// Package store implements a datastore shim with the proposed new store interface.
package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateJoinToken adds the given join token to the store.
func (s *Shim) CreateJoinToken(ctx context.Context,
	req *datastore.CreateJoinTokenRequest) (*datastore.CreateJoinTokenResponse, error) {

	if s.Store == nil {
		return s.DataStore.CreateJoinToken(ctx, req)
	}

	if req.JoinToken == nil {
		return nil, status.Errorf(codes.InvalidArgument, "store-etcd: empty join token")
	}

	// Build the entry record key and value
	j := req.JoinToken
	k := tokenKey(j.Token)
	v, err := proto.Marshal(j)
	if err != nil {
		return nil, err
	}

	// Create a list of keys to add, starting with the join token, ensuring it doesn't already exist
	put := []*store.KeyValue{{Key: k, Value: v, Compare: store.Compare_NOT_PRESENT}}

	// Add empty index record for expiry
	put = append(put, &store.KeyValue{Key: tokenExpKey(j.Token, j.Expiry)})

	// One put operation for this transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_PUT, Kvs: put}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		if status.Convert(err).Code() == codes.Aborted {
			msg := fmt.Sprintf("join token %s already exists", req.JoinToken.Token)
			return nil, status.Error(codes.Aborted, msg)
		}
		return nil, err
	}

	return &datastore.CreateJoinTokenResponse{JoinToken: req.JoinToken}, nil
}

// DeleteJoinToken deletes the given join token from the store.
func (s *Shim) DeleteJoinToken(ctx context.Context,
	req *datastore.DeleteJoinTokenRequest) (*datastore.DeleteJoinTokenResponse, error) {

	if s.Store == nil {
		return s.DataStore.DeleteJoinToken(ctx, req)
	}

	// Get current join token and version for transactional integrity
	ft, ver, err := s.fetchToken(ctx, &datastore.FetchJoinTokenRequest{Token: req.Token})
	if err != nil {
		return nil, err
	}
	if ft.JoinToken == nil {
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}
	j := ft.JoinToken

	// Build a list of delete operations to be performed as a transaction,
	// starting with the token at the version read above. The entire transaction
	// will fail if this record has been changed since the entry was fetched.
	del := []*store.KeyValue{{Key: tokenKey(j.Token), Version: ver, Compare: store.Compare_EQUALS}}

	// Add index record for expiry (any version)
	del = append(del, &store.KeyValue{Key: tokenExpKey(j.Token, j.Expiry)})

	// One delete operation for all keys in the transaction
	tx := []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: del}}

	_, err = s.Store.Set(ctx, &store.SetRequest{Elements: tx})
	if err != nil {
		return nil, err
	}

	return &datastore.DeleteJoinTokenResponse{JoinToken: j}, nil
}

// FetchJoinToken fetches an existing join token.
func (s *Shim) FetchJoinToken(ctx context.Context,
	req *datastore.FetchJoinTokenRequest) (resp *datastore.FetchJoinTokenResponse, err error) {

	if s.Store == nil {
		return s.DataStore.FetchJoinToken(ctx, req)
	}

	resp, _, err = s.fetchToken(ctx, req)

	return
}

// fetchJoinToken fetches an existing join token and the token version.
func (s *Shim) fetchToken(ctx context.Context,
	req *datastore.FetchJoinTokenRequest) (*datastore.FetchJoinTokenResponse, int64, error) {

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: tokenKey(req.Token)})
	if err != nil {
		return nil, 0, err
	}

	var ver int64
	resp := &datastore.FetchJoinTokenResponse{}
	if len(res.Kvs) == 1 {
		ver = res.Kvs[0].Version
		token := &datastore.JoinToken{}
		err = proto.Unmarshal(res.Kvs[0].Value, token)
		if err != nil {
			return nil, 0, err
		}
		resp.JoinToken = token
	} else if len(res.Kvs) > 1 {
		return resp, 0, fmt.Errorf("More than one entry for %s", req.Token)
	}
	return resp, ver, nil
}

// PruneJoinTokens deletes all tokens which have expired before the given time.
func (s *Shim) PruneJoinTokens(ctx context.Context,
	req *datastore.PruneJoinTokensRequest) (*datastore.PruneJoinTokensResponse, error) {

	if s.Store == nil {
		return s.DataStore.PruneJoinTokens(ctx, req)
	}

	ids, err := s.tokenExpMap(ctx, req.ExpiresBefore)
	if err != nil {
		return nil, err
	}

	// Delete expired registered entries
	for id := range ids {
		_, err = s.DeleteJoinToken(ctx, &datastore.DeleteJoinTokenRequest{Token: id})
		if err != nil {
			return nil, err
		}
	}

	return &datastore.PruneJoinTokensResponse{}, nil
}

// tokenKey returns a string formatted key for a join token
func tokenKey(id string) string {
	// e.g. "T|5fee2e4a-1fe3"
	return fmt.Sprintf("%s%s", tokenPrefix, id)
}

// tokenExpKey returns a string formatted key for a join token indexed by expiry in seconds.
// e.g. "IT|EXP|1611907252|abcd1234"
// NOTE: %d without leading zeroes for time.Unix will work for the next ~250 years
func tokenExpKey(id string, exp int64) string {
	return fmt.Sprintf("%s%s%s%s%d%s%s", indexKeyID, tokenPrefix, EXP, delim, exp, delim, id)
}

// tokenExpID returns the join token id from the given token expiry (EXP) index key.
func tokenExpID(key string) (string, error) {
	items := strings.Split(key, delim)
	if len(items) != 4 {
		return "", fmt.Errorf("invalid token expiry index key: %s", key)
	}
	return items[3], nil
}

// tokenExpMap returns a map of join token ids expiring before the given expiry.
// A specific store revision may be supplied for transactional consistency; 0 = current revision
// Use of a map facilitates easier intersection with other filters
func (s *Shim) tokenExpMap(ctx context.Context, exp int64) (map[string]bool, error) {
	// Set range to all index keys before the given time
	key := fmt.Sprintf("%s%s%s%s", indexKeyID, tokenPrefix, EXP, delim)
	end := fmt.Sprintf("%s%s%s%s%d", indexKeyID, tokenPrefix, EXP, delim, exp)

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end})
	if err != nil {
		return nil, err
	}

	ids := map[string]bool{}
	for _, kv := range res.Kvs {
		id, err := tokenExpID(kv.Key)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	return ids, nil
}
