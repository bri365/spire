package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/roguesoftware/etcd/clientv3"
	"github.com/roguesoftware/etcd/mvcc/mvccpb"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The cache is implemented as separate sections for each object type.
// As reads are expected to significantly outweight writes, cache segments are
// protected with a RWMutex - allowing multiple parallel read operations.
//
// The cache is loaded from the backing store at startup and maintained through
// watch functions that stream backend write events (create, delete, update).
// For etcd, the watch feature is builtin. For other KV stores and SQL databases,
// the watch feature may be implemented in an event table, and polling the backend
// store revision or another lightweight change indicator.
//
// The cached elements are stored in unordered maps, indexed by object ID.
// If order is important, an index array of ID strings can be maintained with
// something like the following referenced in the load and watch functions.
//
// func insert(ss []string, s string) []string {
//     i := sort.SearchStrings(ss, s)
//     ss = append(ss, "")
//     copy(ss[i+1:], ss[i:])
//     ss[i] = s
//     return ss
// }
//
// func remove(ss []string, s string) []string {
//     i := sort.SearchStrings(ss, s)
//     if ss[i] != s { return ss } // or return error
//     return append(ss[:i], ss[i+1:]...)
// }
//

// cacheIndex consists of an ordered list and count of cached keys (object IDs).
type cacheIndex struct {
	Count int
	Keys  []string
}

// Cache represents the in-memory cache for Store objects.
type Cache struct {
	// Interval in seconds between server heartbeat messages
	heartbeatInterval time.Duration
	mu                sync.RWMutex
	initialized       bool
	storeRevision     int64

	bundleCacheEnabled    bool
	bundleCacheInvalidate bool
	bundleCacheUpdate     bool
	bundleIndex           *cacheIndex
	bundles               map[string]*common.Bundle

	entryCacheEnabled    bool
	entryCacheInvalidate bool
	entryCacheUpdate     bool
	entryIndex           *cacheIndex
	entries              map[string]*common.RegistrationEntry

	nodeCacheEnabled    bool
	nodeCacheFetch      bool
	nodeCacheInvalidate bool
	nodeCacheUpdate     bool
	nodeIndex           *cacheIndex
	nodes               map[string]*common.AttestedNode

	tokenCacheEnabled    bool
	tokenCacheInvalidate bool
	tokenCacheUpdate     bool
	tokenIndex           *cacheIndex
	tokens               map[string]*datastore.JoinToken
}

// Store cache constants
const (
	TxEotTTL = 60

	loadPageSize = 10000
)

// NewCache returns an initialized cache object.
func NewCache(cfg *Configuration) Cache {
	return Cache{
		bundles: map[string]*common.Bundle{},
		entries: map[string]*common.RegistrationEntry{},
		nodes:   map[string]*common.AttestedNode{},
		tokens:  map[string]*datastore.JoinToken{},

		bundleIndex: &cacheIndex{Keys: []string{}},
		entryIndex:  &cacheIndex{Keys: []string{}},
		nodeIndex:   &cacheIndex{Keys: []string{}},
		tokenIndex:  &cacheIndex{Keys: []string{}},

		bundleCacheEnabled:    cfg.EnableBundleCache,
		bundleCacheInvalidate: cfg.EnableBundleCacheInvalidate,
		bundleCacheUpdate:     cfg.EnableBundleCacheUpdate,

		entryCacheEnabled:    cfg.EnableEntryCache,
		entryCacheInvalidate: cfg.EnableEntryCacheInvalidate,
		entryCacheUpdate:     cfg.EnableEntryCacheUpdate,

		nodeCacheEnabled:    cfg.EnableNodeCache,
		nodeCacheFetch:      cfg.EnableNodeCacheFetch,
		nodeCacheInvalidate: cfg.EnableNodeCacheInvalidate,
		nodeCacheUpdate:     cfg.EnableNodeCacheUpdate,

		tokenCacheEnabled:    cfg.EnableTokenCache,
		tokenCacheInvalidate: cfg.EnableTokenCacheInvalidate,
		tokenCacheUpdate:     cfg.EnableTokenCacheUpdate,

		heartbeatInterval: time.Duration(cfg.HeartbeatInterval) * time.Second,
	}
}

// InitializeCache loads cache data and starts the watcher tasks.
// Bulk loading is performed on the same store revision.
// After the initial bulk load, watcher routines are started to
// continually update the cache as store updates are procesed.
// NOTE: for faster startup, we could start a go routine to load
// and validate the cache data.
func (s *Shim) InitializeCache() error {
	s.Log.Debug("Initializing store cache")
	// rev is a unique store revision and serves as both the heartbeat ID for this server
	// as well as the initial cache revision
	rev, err := s.startHeartbeatService()
	if err != nil {
		return err
	}

	if s.c.bundleCacheEnabled {
		err = s.loadBundles(rev)
		if err != nil {
			return err
		}
		go s.watchBundles(rev + 1)
	}

	if s.c.entryCacheEnabled {
		err = s.loadEntries(rev)
		if err != nil {
			return err
		}
		go s.watchEntries(rev + 1)
	}

	if s.c.nodeCacheEnabled {
		err = s.loadNodes(rev)
		if err != nil {
			return err
		}
		go s.watchNodes(rev + 1)
	}

	if s.c.tokenCacheEnabled {
		err = s.loadTokens(rev)
		if err != nil {
			return err
		}
		go s.watchTokens(rev + 1)
	}

	s.c.mu.Lock()
	s.c.initialized = true
	s.c.storeRevision = rev
	s.c.mu.Unlock()

	// TODO handle exit conditions from the watch routines?

	return nil
}

// loadBundles performs the initial cache load for bundles
func (s *Shim) loadBundles(rev int64) error {
	s.Log.Debug("Cache loading bundles", "rev", rev)
	start := s.clock.Now().UnixNano()

	// Cache is being initialized so it is safe to hold the lock the whole time
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	token := ""
	for {
		br, r, err := s.listBundles(context.TODO(), rev, &datastore.ListBundlesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return err
		}

		if r != rev {
			return fmt.Errorf("LB revision returned (%d) does not match requested (%d)", r, rev)
		}

		for _, b := range br.Bundles {
			s.c.bundles[b.TrustDomainId] = b
			s.c.bundleIndex.Keys = append(s.c.bundleIndex.Keys, b.TrustDomainId)
		}

		count := len(br.Bundles)
		token = br.Pagination.Token
		s.Log.Debug("Cache LB", "count", count, "token", token)

		if count < loadPageSize {
			break
		}
	}

	sort.Strings(s.c.bundleIndex.Keys)
	s.c.bundleIndex.Count = len(s.c.bundleIndex.Keys)
	deltaMsec := (s.clock.Now().UnixNano() - start) / 1000000
	s.Log.Info("Loaded bundles", "count", s.c.bundleIndex.Count, "msec", deltaMsec)

	return nil
}

// Add or update the given bundle in the cache
func (s *Shim) setBundleCacheEntry(id string, bundle *common.Bundle) {
	if s.c.bundleCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		s.c.bundles[id] = bundle
		s.insertIndexKey(s.c.bundleIndex, id)
		s.c.mu.Unlock()
	}
}

// Fetch the given bundle from the cache
func (s *Shim) fetchBundleCacheEntry(id string) *common.Bundle {
	if !s.c.bundleCacheEnabled && s.c.initialized {
		return nil
	}
	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	return s.c.bundles[id]
}

// Remove the given bundle from the cache
func (s *Shim) removeBundleCacheEntry(id string) {
	if s.c.bundleCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		delete(s.c.bundles, id)
		s.removeIndexKey(s.c.bundleIndex, id)
		s.c.mu.Unlock()
	}
}

// loadEntries performs the initial cache load for registration entries
func (s *Shim) loadEntries(rev int64) error {
	s.Log.Debug("Cache loading registration entries", "rev", rev)
	start := s.clock.Now().UnixNano()

	// Cache is being initialized so it is safe to hold the lock the whole time
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	token := ""
	for {
		er, r, err := s.listRegistrationEntries(context.TODO(), rev, &datastore.ListRegistrationEntriesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return err
		}

		if r != rev {
			return fmt.Errorf("Cache LE revision returned (%d) does not match requested (%d)", r, rev)
		}

		for _, e := range er.Entries {
			s.c.entries[e.EntryId] = e
			s.c.entryIndex.Keys = append(s.c.entryIndex.Keys, e.EntryId)
		}

		count := len(er.Entries)
		token = er.Pagination.Token
		s.Log.Debug("Cache LE", "count", count, "token", token)

		if count < loadPageSize {
			break
		}
	}

	sort.Strings(s.c.entryIndex.Keys)
	s.c.entryIndex.Count = len(s.c.entryIndex.Keys)
	deltaMsec := (s.clock.Now().UnixNano() - start) / 1000000
	s.Log.Info("Loaded entries", "count", s.c.entryIndex.Count, "msec", deltaMsec)

	return nil
}

// Add or update the given attested entry in the cache
func (s *Shim) setEntryCacheEntry(id string, entry *common.RegistrationEntry) {
	if s.c.entryCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		s.c.entries[id] = entry
		s.insertIndexKey(s.c.entryIndex, id)
		s.c.mu.Unlock()
	}
}

// Fetch the given attested entry from the cache
func (s *Shim) fetchEntryCacheEntry(id string) *common.RegistrationEntry {
	if !s.c.entryCacheEnabled && s.c.initialized {
		return nil
	}
	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	return s.c.entries[id]
}

// Remove the given registration entry from the cache
func (s *Shim) removeEntryCacheEntry(id string) {
	if s.c.entryCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		delete(s.c.entries, id)
		s.removeIndexKey(s.c.entryIndex, id)
		s.c.mu.Unlock()
	}
}

// loadNodes performs the initial cache load for attested nodes.
func (s *Shim) loadNodes(rev int64) error {
	s.Log.Debug("Cache loading attested nodes", "rev", rev)
	start := s.clock.Now().UnixNano()

	// Cache is being initialized so it is safe to hold the lock the whole time
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	token := ""
	for {
		nr, r, err := s.listAttestedNodes(context.TODO(), rev, &datastore.ListAttestedNodesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return err
		}

		if r != rev {
			return fmt.Errorf("LN revision returned (%d) does not match requested (%d)", r, rev)
		}

		for _, n := range nr.Nodes {
			s.c.nodes[n.SpiffeId] = n
			s.c.nodeIndex.Keys = append(s.c.nodeIndex.Keys, n.SpiffeId)
		}

		count := len(nr.Nodes)
		token = nr.Pagination.Token
		s.Log.Debug("Cache LN", "count", count, "token", token)

		if count < loadPageSize {
			break
		}
	}

	sort.Strings(s.c.nodeIndex.Keys)
	s.c.nodeIndex.Count = len(s.c.nodeIndex.Keys)
	deltaMsec := (s.clock.Now().UnixNano() - start) / 1000000
	s.Log.Info("Loaded nodes", "count", s.c.nodeIndex.Count, "msec", deltaMsec)

	return nil
}

// Add or update the given attested node in the cache
func (s *Shim) setNodeCacheEntry(id string, node *common.AttestedNode) {
	if s.c.nodeCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		s.c.nodes[id] = node
		s.insertIndexKey(s.c.nodeIndex, id)
		s.c.mu.Unlock()
	}
}

// Fetch the given attested node from the cache
func (s *Shim) fetchNodeCacheEntry(id string) *common.AttestedNode {
	if !s.c.nodeCacheEnabled && s.c.initialized {
		return nil
	}
	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	return s.c.nodes[id]
}

// Remove the given attested node from the cache
func (s *Shim) removeNodeCacheEntry(id string) {
	if s.c.initialized && s.c.nodeCacheEnabled && s.c.nodeCacheInvalidate {
		s.c.mu.Lock()
		delete(s.c.nodes, id)
		s.removeIndexKey(s.c.nodeIndex, id)
		s.c.mu.Unlock()
	}
}

// loadTokens performs the initial cache load for join tokens.
func (s *Shim) loadTokens(rev int64) error {
	s.Log.Debug("Cache loading join tokens", "rev", rev)
	start := s.clock.Now().UnixNano()

	// Cache is being initialized so it is safe to hold the lock the whole time
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	token := ""
	for {
		tr, r, err := s.listJoinTokens(context.TODO(), rev, &datastore.ListJoinTokensRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return err
		}

		if r != rev {
			return fmt.Errorf("LT revision returned (%d) does not match requested (%d)", r, rev)
		}

		for _, t := range tr.JoinTokens {
			s.c.tokens[t.Token] = t
			s.c.tokenIndex.Keys = append(s.c.tokenIndex.Keys, t.Token)
		}

		count := len(tr.JoinTokens)
		token = tr.Pagination.Token
		s.Log.Debug("Cache LT", "count", count, "token", token)

		if count < loadPageSize {
			break
		}
	}

	sort.Strings(s.c.tokenIndex.Keys)
	s.c.tokenIndex.Count = len(s.c.tokenIndex.Keys)
	deltaMsec := (s.clock.Now().UnixNano() - start) / 1000000
	s.Log.Info("Loaded tokens", "count", s.c.tokenIndex.Count, "msec", deltaMsec)

	return nil
}

// Add or update the given join token in the cache
func (s *Shim) setTokenCacheEntry(id string, token *datastore.JoinToken) {
	if s.c.tokenCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		s.c.tokens[id] = token
		s.insertIndexKey(s.c.tokenIndex, id)
		s.c.mu.Unlock()
	}
}

// Fetch the given join token from the cache
func (s *Shim) fetchTokenCacheEntry(id string) *datastore.JoinToken {
	if !s.c.tokenCacheEnabled && s.c.initialized {
		return nil
	}
	s.c.mu.RLock()
	defer s.c.mu.RUnlock()
	return s.c.tokens[id]
}

// Remove the given join token from the cache
func (s *Shim) removeTokenCacheEntry(id string) {
	if s.c.tokenCacheEnabled && s.c.initialized {
		s.c.mu.Lock()
		delete(s.c.tokens, id)
		s.removeIndexKey(s.c.tokenIndex, id)
		s.c.mu.Unlock()
	}
}

// watchBundles receives a stream of updates (deletes or puts) for bundles,
// starting at the given store revision, and applies the changes to cache.
func (s *Shim) watchBundles(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}

	// Using BundleKeyID instead of BundlePrefix will also return index updates
	bc := s.Etcd.Watch(context.TODO(), BundlePrefix, opts...)
	s.Log.Debug("Watching bundle updates", "rev", rev)

	for w := range bc {
		if w.Err() != nil {
			if status.Convert(w.Err()).Code() != codes.Canceled {
				s.Log.Error("Bundle watch channel error", "error", w.Err())
			}
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No bundle updates for 10 minutes")
			// TODO reset timeout counter
			continue
		}

		// Hold the lock for all updates to give other routines the most recent data.
		s.c.mu.Lock()
		for _, e := range w.Events {
			if e.Type == mvccpb.DELETE {
				id, err := bundleIDFromKey(string(e.Kv.Key))
				if err != nil {
					s.Log.Error("BWD error", err)
				} else {
					delete(s.c.bundles, id)
					s.removeIndexKey(s.c.bundleIndex, id)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else if e.Type == mvccpb.PUT {
				bundle := &common.Bundle{}
				err := proto.Unmarshal(e.Kv.Value, bundle)
				if err != nil {
					s.Log.Error("BWP error", err, "value", e.Kv.Value)
				} else {
					s.c.bundles[bundle.TrustDomainId] = bundle
					s.insertIndexKey(s.c.bundleIndex, bundle.TrustDomainId)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else {
				s.Log.Error("BW unknown event", "type", e.Type, "kv", e.Kv)
			}
		}
		s.c.mu.Unlock()
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// watchEntries receives a stream of updates (deletes or puts) for registration entries,
// starting at the given store revision, and applies the changes to cache.
func (s *Shim) watchEntries(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}

	// Using EntryKeyID instead of EntryPrefix will also return index updates
	ec := s.Etcd.Watch(context.TODO(), EntryPrefix, opts...)
	s.Log.Debug("Watching entry updates", "rev", rev)

	for w := range ec {
		if w.Err() != nil {
			if status.Convert(w.Err()).Code() != codes.Canceled {
				s.Log.Error("Entry watch channel error", "error", w.Err())
			}
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No entry updates for 10 minutes")
			// TODO reset timeout counter
			continue
		}

		// Hold the lock for all updates to give other routines the most recent data.
		s.c.mu.Lock()
		for _, e := range w.Events {
			if e.Type == mvccpb.DELETE {
				id, err := entryIDFromKey(string(e.Kv.Key))
				if err != nil {
					s.Log.Error("EWD error", err)
				} else {
					delete(s.c.entries, id)
					s.removeIndexKey(s.c.entryIndex, id)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else if e.Type == mvccpb.PUT {
				entry := &common.RegistrationEntry{}
				err := proto.Unmarshal(e.Kv.Value, entry)
				if err != nil {
					s.Log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
				} else {
					s.c.entries[entry.EntryId] = entry
					s.insertIndexKey(s.c.entryIndex, entry.EntryId)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else {
				s.Log.Error("EW unknown event", "type", e.Type, "kv", e.Kv)
			}
		}
		s.c.mu.Unlock()
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// watchNodes receives a stream of updates (deletes or puts) for attested nodes,
// starting at the given store revision, and applies the changes to cache.
func (s *Shim) watchNodes(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}

	// Using NodeKeyID instead of NodePrefix will also return index updates
	nc := s.Etcd.Watch(context.TODO(), NodePrefix, opts...)
	s.Log.Debug("Watching node updates", "rev", rev)

	for w := range nc {
		if w.Err() != nil {
			if status.Convert(w.Err()).Code() != codes.Canceled {
				s.Log.Error("Node watch channel error", "error", w.Err())
			}
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No node updates for 10 minutes")
			// TODO reset timeout counter
			continue
		}

		// Hold the lock for all updates to give other routines the most recent data.
		s.c.mu.Lock()
		for _, e := range w.Events {
			if e.Type == mvccpb.DELETE {
				id, err := nodeIDFromKey(string(e.Kv.Key))
				if err != nil {
					s.Log.Error("NWD error", err)
				} else {
					delete(s.c.nodes, id)
					s.removeIndexKey(s.c.nodeIndex, id)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else if e.Type == mvccpb.PUT {
				node := &common.AttestedNode{}
				err := proto.Unmarshal(e.Kv.Value, node)
				if err != nil {
					s.Log.Error("NWP error", err, "value", e.Kv.Value)
				} else {
					s.c.nodes[node.SpiffeId] = node
					s.insertIndexKey(s.c.nodeIndex, node.SpiffeId)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else {
				s.Log.Error("NW unknown event", "type", e.Type, "kv", e.Kv)
			}
		}
		s.c.mu.Unlock()
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// watchTokens receives a stream of updates (deletes or puts) for join tokens,
// starting at the given store revision, and applies the changes to cache.
func (s *Shim) watchTokens(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}

	// Using TokenKeyID instead of TokenPrefix will also return index updates
	tc := s.Etcd.Watch(context.TODO(), TokenPrefix, opts...)
	s.Log.Debug("Watching token updates", "rev", rev)

	for w := range tc {
		if w.Err() != nil {
			if status.Convert(w.Err()).Code() != codes.Canceled {
				s.Log.Error("Token watch channel error", "error", w.Err())
			}
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No join token updates for 10 minutes")
			// TODO reset timeout counter
			continue
		}

		// Hold the lock for all updates to give other routines the most recent data.
		s.c.mu.Lock()
		for _, e := range w.Events {
			if e.Type == mvccpb.DELETE {
				id, err := tokenIDFromKey(string(e.Kv.Key))
				if err != nil {
					s.Log.Error("TWD error", err)
				} else {
					delete(s.c.tokens, id)
					s.removeIndexKey(s.c.tokenIndex, id)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else if e.Type == mvccpb.PUT {
				token := &datastore.JoinToken{}
				err := proto.Unmarshal(e.Kv.Value, token)
				if err != nil {
					s.Log.Error("TWP error", err, "value", e.Kv.Value)
				} else {
					s.c.tokens[token.Token] = token
					s.insertIndexKey(s.c.tokenIndex, token.Token)
					s.c.storeRevision = e.Kv.ModRevision
				}
			} else {
				s.Log.Error(fmt.Sprintf("Unknown watch event %v", e))
			}
		}
		s.c.mu.Unlock()
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// insertIndexKey inserts the given key into the given index if it does not already exist.
// NOTE: This function must be called with s.c.mu.Lock() already held.
func (s *Shim) insertIndexKey(ci *cacheIndex, key string) {
	i := sort.SearchStrings(ci.Keys, key)
	if i < len(ci.Keys) && ci.Keys[i] == key {
		// Already exists
		return
	}
	ci.Keys = append(ci.Keys, "")
	copy(ci.Keys[i+1:], ci.Keys[i:])
	ci.Keys[i] = key
	ci.Count = len(ci.Keys)
	return
}

// removeIndexKey deletes the given key from the given index if it exists.
// NOTE: This function must be called with s.c.mu.Lock() already held.
func (s *Shim) removeIndexKey(ci *cacheIndex, key string) {
	i := sort.SearchStrings(ci.Keys, key)
	if i < len(ci.Keys) && ci.Keys[i] == key {
		ci.Keys = append(ci.Keys[:i], ci.Keys[i+1:]...)
		ci.Count = len(ci.Keys)
		return
	}
	// Not found
	return
}

// indexFromKey returns a slice of keys from the proper cache index,
// beginning with where the object ID is or would be, if not present.
// NOTE: This function must be called with s.c.mu.RLock() already held.
func (s *Shim) cacheIndexFromKey(key string) []string {
	id := ""
	var ci *cacheIndex
	if IsBundleKey(key) {
		ci = s.c.bundleIndex
		id, _ = bundleIDFromKey(key)
	} else if IsEntryKey(key) {
		ci = s.c.entryIndex
		id, _ = entryIDFromKey(key)
	} else if IsNodeKey(key) {
		ci = s.c.nodeIndex
		id, _ = nodeIDFromKey(key)
	} else if IsTokenKey(key) {
		ci = s.c.tokenIndex
		id, _ = tokenIDFromKey(key)
	} else {
		s.Log.Error("iFK Unknown key", "key", key)
		return []string{}
	}

	// Locate the index offsets for the beginning and end of the range
	start := sort.SearchStrings(ci.Keys, id)
	if start == ci.Count {
		return []string{}
	}

	return ci.Keys[start:]
}
