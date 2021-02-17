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
	// NOTE: since the key is the token, we could save store space
	// by not including the token again in the value
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

	// Invalidate cache entry here to prevent race condition with async watcher
	s.removeTokenCacheEntry(j.Token)

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

	token := s.fetchTokenCacheEntry(req.Token)
	if token != nil {
		resp = &datastore.FetchJoinTokenResponse{JoinToken: token}
		return
	}

	resp, _, err = s.fetchToken(ctx, req)
	if resp.JoinToken == nil {
		return
	}

	s.setTokenCacheEntry(req.Token, token)

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

// listJoinTokens retrieves an optionally paginated list of all join tokens.
// Store revision is accepted and returned for consistency across paginated calls.
func (s *Shim) listJoinTokens(ctx context.Context, rev int64,
	req *datastore.ListJoinTokensRequest) (*datastore.ListJoinTokensResponse, int64, error) {

	if req.Pagination != nil && req.Pagination.PageSize == 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "cannot paginate with pagesize = 0")
	}

	// Start with all token identifiers and limit of 0 (no limit)
	key := tokenKey("")
	end := allTokens
	var limit int64

	p := req.Pagination
	if p != nil {
		limit = int64(p.PageSize)
		if len(p.Token) > 0 {
			// Validate token
			if len(p.Token) < 3 || p.Token[0:2] != key {
				return nil, 0, status.Errorf(codes.InvalidArgument, "could not parse token '%s'", p.Token)
			}
			// TODO one bit larger than token
			key = fmt.Sprintf("%s ", p.Token)
		}
	}

	res, err := s.Store.Get(ctx, &store.GetRequest{Key: key, End: end, Limit: limit, Revision: rev})
	if err != nil {
		return nil, 0, err
	}

	lastKey := ""
	resp := &datastore.ListJoinTokensResponse{}
	for _, kv := range res.Kvs {
		t := &datastore.JoinToken{}
		err = proto.Unmarshal(kv.Value, t)
		if err != nil {
			return nil, 0, err
		}
		resp.JoinTokens = append(resp.JoinTokens, t)
		lastKey = kv.Key
	}

	if p != nil {
		p.Token = ""
		// Note: In the event the total number of items exactly equals the page size,
		// there may be one extra list call that returns no items. This fact is used
		// in other parts of the code so it should not be optimized without consideration.
		if len(resp.JoinTokens) > 0 {
			p.Token = lastKey
		}
		resp.Pagination = p
	}
	return resp, res.Revision, nil
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
	// NOTE: for improved performance, send these in transactions
	// of one hundred or so rather than one at a time
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

// tokenIDFromKey returns the token id from the given token key.
func tokenIDFromKey(key string) (string, error) {
	items := strings.Split(key, Delim)
	if len(items) != 2 || items[0] != tokenKeyID {
		return "", fmt.Errorf("invalid token key: %s", key)
	}
	return items[1], nil
}

// tokenExpKey returns a string formatted key for a join token indexed by expiry in seconds.
// e.g. "TI|EXP|1611907252|abcd1234"
// NOTE: %d without leading zeroes for time.Unix will work for the next ~250 years
func tokenExpKey(id string, exp int64) string {
	return fmt.Sprintf("%s%s%s%d%s%s", tokenIndex, EXP, Delim, exp, Delim, id)
}

// tokenExpID returns the join token id from the given token expiry (EXP) index key.
func tokenExpID(key string) (string, error) {
	items := strings.Split(key, Delim)
	// TODO additional checks to ensure a properly formatted index?
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
	key := fmt.Sprintf("%s%s%s", tokenIndex, EXP, Delim)
	end := fmt.Sprintf("%s%s%s%d", tokenIndex, EXP, Delim, exp)

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
