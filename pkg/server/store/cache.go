package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/roguesoftware/etcd/clientv3"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
)

// Cache represents the in-memory cache for Store objects.
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
type Cache struct {
	// Interval in seconds between server heartbeat messages
	hbInterval  time.Duration
	initialized bool

	bundleMu            sync.RWMutex
	bundleCacheEnabled  bool
	bundleStoreRevision int64
	bundles             map[string]*common.Bundle

	entryMu            sync.RWMutex
	entryCacheEnabled  bool
	entryStoreRevision int64
	entries            map[string]*common.RegistrationEntry

	nodeMu            sync.RWMutex
	nodeCacheEnabled  bool
	nodeStoreRevision int64
	nodes             map[string]*common.AttestedNode

	tokenMu            sync.RWMutex
	tokenCacheEnabled  bool
	tokenStoreRevision int64
	tokens             map[string]*datastore.JoinToken
}

// Store cache constants
const (
	HeartbeatDefaultInterval = 5
	TxEotTTL                 = 60

	hbTTL        = 1
	loadPageSize = 10000
	watchUpdate  = 0
	watchDelete  = 1
)

// NewCache returns an initialized cache object.
func NewCache(cfg *Configuration) Cache {
	return Cache{
		bundles: map[string]*common.Bundle{},
		entries: map[string]*common.RegistrationEntry{},
		nodes:   map[string]*common.AttestedNode{},
		tokens:  map[string]*datastore.JoinToken{},

		bundleCacheEnabled: !cfg.DisableBundleCache,
		entryCacheEnabled:  !cfg.DisableEntryCache,
		nodeCacheEnabled:   !cfg.DisableNodeCache,
		tokenCacheEnabled:  !cfg.DisableTokenCache,

		hbInterval: time.Duration(cfg.HeartbeatInterval) * time.Second,
	}
}

// Initialize loads cache data and starts the watcher tasks.
// Bulk loading is performed on the same store revision.
// After the initial bulk load, watcher routines are started to
// continually update the cache as store updates are procesed.
// NOTE: for faster startup, we could start a go routine to load
// and validate the cache data.
func (s *Shim) Initialize() error {
	// Start with the latest store revision (rev = 0)
	var rev int64
	var err error

	if s.c.bundleCacheEnabled {
		rev, err = s.loadBundles(rev)
		if err != nil {
			return err
		}
		go s.watchBundles(rev + 1)
	}

	if s.c.entryCacheEnabled {
		rev, err = s.loadEntries(rev)
		if err != nil {
			return err
		}
		go s.watchEntries(rev + 1)
	}

	if s.c.nodeCacheEnabled {
		_, err = s.loadNodes(rev)
		if err != nil {
			return err
		}
		go s.watchNodes(rev + 1)
	}

	if s.c.tokenCacheEnabled {
		_, err = s.loadTokens(rev)
		if err != nil {
			return err
		}
		go s.watchTokens(rev + 1)
	}

	s.c.initialized = true
	s.startHeartbeatService()

	// TODO handle exit conditions from the five previous routines

	return nil
}

// loadBundles performs the initial cache load for bundles
func (s *Shim) loadBundles(revision int64) (int64, error) {
	s.c.bundleMu.Lock()
	defer s.c.bundleMu.Unlock()

	rev := revision
	token := ""
	for {
		br, r, err := s.listBundles(context.TODO(), rev, &datastore.ListBundlesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		if rev == 0 {
			// First read returns the current store revision if requested revision is 0
			rev = r
		} else if r != rev {
			// Requested store revision was not honored - something wrong with etcd?
			return 0, fmt.Errorf("LB revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(br.Bundles)
		token = br.Pagination.Token
		s.Log.Debug(fmt.Sprintf("load bundles count: %d, token: %s, rev: %d", count, token, rev))

		if count == 0 {
			break
		}

		for _, b := range br.Bundles {
			s.c.bundles[b.TrustDomainId] = b
		}
	}

	s.c.bundleStoreRevision = rev

	return rev, nil
}

// Add or update the given bundle in the cache
func (s *Shim) setBundleCacheEntry(id string, bundle *common.Bundle) {
	if s.c.bundleCacheEnabled {
		s.c.bundleMu.Lock()
		s.c.bundles[id] = bundle
		s.c.bundleMu.Unlock()
	}
}

// Fetch the given bundle from the cache
func (s *Shim) fetchBundleCacheEntry(id string) *common.Bundle {
	if !s.c.bundleCacheEnabled {
		return nil
	}
	s.c.bundleMu.RLock()
	defer s.c.bundleMu.RUnlock()
	return s.c.bundles[id]
}

// Remove the given bundle from the cache
func (s *Shim) removeBundleCacheEntry(id string) {
	if s.c.bundleCacheEnabled {
		s.c.bundleMu.Lock()
		delete(s.c.bundles, id)
		s.c.bundleMu.Unlock()
	}
}

// loadEntries performs the initial cache load for registration entries
func (s *Shim) loadEntries(revision int64) (int64, error) {
	s.c.entryMu.Lock()
	defer s.c.entryMu.Unlock()

	rev := revision
	token := ""
	for {
		er, r, err := s.listRegistrationEntries(context.TODO(), rev, &datastore.ListRegistrationEntriesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		if rev == 0 {
			// First read returns the current store revision if requested revision is 0
			rev = r
		} else if r != rev {
			// Requested store version was not honored - something wrong with etcd?
			return 0, fmt.Errorf("LE revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(er.Entries)
		token = er.Pagination.Token
		s.Log.Debug(fmt.Sprintf("load entries count: %d, token: %s, rev: %d", count, token, rev))

		if count == 0 {
			break
		}

		for _, e := range er.Entries {
			s.c.entries[e.EntryId] = e
		}
	}

	s.c.entryStoreRevision = rev

	return rev, nil
}

// Add or update the given attested entry in the cache
func (s *Shim) setEntryCacheEntry(id string, entry *common.RegistrationEntry) {
	if s.c.entryCacheEnabled {
		s.c.entryMu.Lock()
		s.c.entries[id] = entry
		s.c.entryMu.Unlock()
	}
}

// Fetch the given attested entry from the cache
func (s *Shim) fetchEntryCacheEntry(id string) *common.RegistrationEntry {
	if !s.c.entryCacheEnabled {
		return nil
	}
	s.c.entryMu.RLock()
	defer s.c.entryMu.RUnlock()
	return s.c.entries[id]
}

// Remove the given registration entry from the cache
func (s *Shim) removeEntryCacheEntry(id string) {
	if s.c.entryCacheEnabled {
		s.c.entryMu.Lock()
		delete(s.c.entries, id)
		s.c.entryMu.Unlock()
	}
}

// loadNodes performs the initial cache load for attested nodes.
func (s *Shim) loadNodes(revision int64) (int64, error) {
	s.c.nodeMu.Lock()
	defer s.c.nodeMu.Unlock()

	rev := revision
	token := ""
	for {
		nr, r, err := s.listAttestedNodes(context.TODO(), rev, &datastore.ListAttestedNodesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		if rev == 0 {
			// First read returns the current store revision if requested revision is 0
			rev = r
		} else if r != rev {
			// Requested store version was not honored - something wrong with etcd?
			return 0, fmt.Errorf("LN revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(nr.Nodes)
		token = nr.Pagination.Token
		s.Log.Debug(fmt.Sprintf("load nodes count: %d, token: %s, rev: %d", count, token, rev))

		if count == 0 {
			break
		}

		for _, n := range nr.Nodes {
			s.c.nodes[n.SpiffeId] = n
		}
	}

	s.c.nodeStoreRevision = rev

	return rev, nil
}

// Add or update the given attested node in the cache
func (s *Shim) setNodeCacheEntry(id string, node *common.AttestedNode) {
	if s.c.nodeCacheEnabled {
		s.c.nodeMu.Lock()
		s.c.nodes[id] = node
		s.c.nodeMu.Unlock()
	}
}

// Fetch the given attested node from the cache
func (s *Shim) fetchNodeCacheEntry(id string) *common.AttestedNode {
	if !s.c.nodeCacheEnabled {
		return nil
	}
	s.c.nodeMu.RLock()
	defer s.c.nodeMu.RUnlock()
	return s.c.nodes[id]
}

// Remove the given attested node from the cache
func (s *Shim) removeNodeCacheEntry(id string) {
	if s.c.nodeCacheEnabled {
		s.c.nodeMu.Lock()
		delete(s.c.nodes, id)
		s.c.nodeMu.Unlock()
	}
}

// loadTokens performs the initial cache load for join tokens.
func (s *Shim) loadTokens(revision int64) (int64, error) {
	s.c.tokenMu.Lock()
	defer s.c.tokenMu.Unlock()

	rev := revision
	token := ""
	for {
		tr, r, err := s.listJoinTokens(context.TODO(), rev, &datastore.ListJoinTokensRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		if rev == 0 {
			// First read returns the current store revision if requested revision is 0
			rev = r
		} else if r != rev {
			// Requested store version was not honored - something wrong with etcd?
			return 0, fmt.Errorf("LT revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(tr.JoinTokens)
		token = tr.Pagination.Token
		s.Log.Debug(fmt.Sprintf("load tokens count: %d, token: %s, rev: %d", count, token, rev))

		if count == 0 {
			break
		}

		for _, t := range tr.JoinTokens {
			s.c.tokens[t.Token] = t
		}
	}

	s.c.tokenStoreRevision = rev

	return rev, nil
}

// Add or update the given join token in the cache
func (s *Shim) setTokenCacheEntry(id string, token *datastore.JoinToken) {
	if s.c.tokenCacheEnabled {
		s.c.tokenMu.Lock()
		s.c.tokens[id] = token
		s.c.tokenMu.Unlock()
	}
}

// Fetch the given join token from the cache
func (s *Shim) fetchTokenCacheEntry(id string) *datastore.JoinToken {
	if !s.c.tokenCacheEnabled {
		return nil
	}
	s.c.tokenMu.RLock()
	defer s.c.tokenMu.RUnlock()
	return s.c.tokens[id]
}

// Remove the given join token from the cache
func (s *Shim) removeTokenCacheEntry(id string) {
	if s.c.tokenCacheEnabled {
		s.c.tokenMu.Lock()
		delete(s.c.tokens, id)
		s.c.tokenMu.Unlock()
	}
}

// watchBundlesAndRegistrations receives a stream of updates (deletes or puts)
// for bundles, starting at the given store revision.
// Note: this is called as a goroutine and only returns on an error
func (s *Shim) watchBundles(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	// Using bundleKeyID instead of bundlePrefix will also return index updates
	bChan := s.Etcd.Watch(context.TODO(), bundlePrefix, opts...)
	s.Log.Debug(fmt.Sprintf("Watching bundle updates from %d", rev))

	for w := range bChan {
		if w.Err() != nil {
			s.Log.Error(fmt.Sprintf("bChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No bundle updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.Log.Debug(fmt.Sprintf("%s %s at %d", e.Type, e.Kv.Key, time.Now().UnixNano()))

			bundle := &common.Bundle{}
			err := proto.Unmarshal(e.Kv.Value, bundle)
			if err != nil {
				s.Log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			if e.Type == watchDelete {
				s.c.bundleMu.Lock()
				delete(s.c.bundles, bundle.TrustDomainId)
				s.c.bundleStoreRevision = e.Kv.ModRevision
				s.c.bundleMu.Unlock()
			} else if e.Type == watchUpdate {
				s.c.bundleMu.Lock()
				s.c.bundles[bundle.TrustDomainId] = bundle
				s.c.bundleStoreRevision = e.Kv.ModRevision
				s.c.bundleMu.Unlock()
			} else {
				s.Log.Error(fmt.Sprintf("Unknown watch event %v", e))
			}
		}
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// watchEntries receives a stream of updates (deletes or puts)
// for registration entries, starting at the given store revision.
// Note: this is called as a goroutine and only returns on an error
func (s *Shim) watchEntries(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	// Using entryKeyID instead of entryPrefix will also return index updates
	eChan := s.Etcd.Watch(context.TODO(), entryPrefix, opts...)
	s.Log.Debug(fmt.Sprintf("Watching entry updates from %d", rev))

	for w := range eChan {
		if w.Err() != nil {
			s.Log.Error(fmt.Sprintf("eChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No entry updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.Log.Debug(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			entry := &common.RegistrationEntry{}
			err := proto.Unmarshal(e.Kv.Value, entry)
			if err != nil {
				s.Log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			if e.Type == watchDelete {
				s.c.entryMu.Lock()
				delete(s.c.entries, entry.EntryId)
				s.c.entryStoreRevision = e.Kv.ModRevision
				s.c.entryMu.Unlock()
			} else if e.Type == watchUpdate {
				s.c.entryMu.Lock()
				s.c.entries[entry.EntryId] = entry
				s.c.entryStoreRevision = e.Kv.ModRevision
				s.c.entryMu.Unlock()
			} else {
				s.Log.Error(fmt.Sprintf("Unknown watch event %v", e))
			}
		}
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// watchNodes receives a stream of updates (deletes or puts)
// for attested nodes, starting at the given store revision.
// Note: this is called as a goroutine and only returns on an error
func (s *Shim) watchNodes(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	// Using nodeKeyID instead of nodePrefix will also return index updates
	nChan := s.Etcd.Watch(context.TODO(), nodePrefix, opts...)
	s.Log.Debug(fmt.Sprintf("Watching node updates from %d", rev))

	for w := range nChan {
		if w.Err() != nil {
			s.Log.Error(fmt.Sprintf("nChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No node updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.Log.Debug(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			node := &common.AttestedNode{}
			err := proto.Unmarshal(e.Kv.Value, node)
			if err != nil {
				s.Log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			if e.Type == watchDelete {
				s.c.nodeMu.Lock()
				delete(s.c.nodes, node.SpiffeId)
				s.c.nodeStoreRevision = e.Kv.ModRevision
				s.c.nodeMu.Unlock()
			} else if e.Type == watchUpdate {
				s.c.nodeMu.Lock()
				s.c.nodes[node.SpiffeId] = node
				s.c.nodeStoreRevision = e.Kv.ModRevision
				s.c.nodeMu.Unlock()
			} else {
				s.Log.Error(fmt.Sprintf("Unknown watch event %v", e))
			}
		}
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// watchTokens receives a stream of updates (deletes or puts)
// for join tokens, starting at the given store revision.
// Note: this is called as a goroutine and only returns on an error
func (s *Shim) watchTokens(rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	// Using tokenKeyID instead of tokenPrefix will also return index updates
	tChan := s.Etcd.Watch(context.TODO(), tokenPrefix, opts...)
	s.Log.Debug(fmt.Sprintf("Watching token updates from %d", rev))

	for w := range tChan {
		if w.Err() != nil {
			s.Log.Error(fmt.Sprintf("tChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.Log.Info("No join token updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.Log.Debug(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			token := &datastore.JoinToken{}
			err := proto.Unmarshal(e.Kv.Value, token)
			if err != nil {
				s.Log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			if e.Type == watchDelete {
				s.c.tokenMu.Lock()
				delete(s.c.tokens, token.Token)
				s.c.tokenStoreRevision = e.Kv.ModRevision
				s.c.tokenMu.Unlock()
			} else if e.Type == watchUpdate {
				s.c.tokenMu.Lock()
				s.c.tokens[token.Token] = token
				s.c.tokenStoreRevision = e.Kv.ModRevision
				s.c.tokenMu.Unlock()
			} else {
				s.Log.Error(fmt.Sprintf("Unknown watch event %v", e))
			}
		}
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// startHeartbeatService initializes inter- and intra-server heartbeat monitoring.
// Heartbeats are short-lived records in the store PUT by servers and
// responded to by all other servers.
// Heartbeat keys include originator and responder IDs and the value is
// a timestamp from the originator. In this way, servers can track the
// latency from database write to async watch update across servers and time.
// TODO use heartbeat data to modulate write response time to ensure (improve)
// inter-server cache coherency.
func (s *Shim) startHeartbeatService() {
	// Secure a unique store revision with an empty heartbeat
	ctx := context.TODO()
	rev, err := s.sendHB(ctx, "", "", 1)
	if err != nil {
		s.Log.Error(fmt.Sprintf("Error getting heartbeat ID %v", err))
		return
	}

	if s.c.hbInterval == 0 {
		// s.Log.Warn("Heartbeat disabled")
		return
	}

	s.Log.Debug(fmt.Sprintf("Starting heartbeat with id %d", rev))
	go s.hbReply(context.TODO(), rev+1)
	go s.hbSend(rev + 1)
}

// Send periodic heartbeat messages
func (s *Shim) hbSend(rev int64) {
	id := fmt.Sprintf("%d", rev)
	// Loop every interval forever
	ticker := s.clock.Ticker(s.c.hbInterval)
	for t := range ticker.C {
		s.Log.Debug(fmt.Sprintf("Sending heartbeat %q at %d", id, t.UnixNano()))
		s.sendHB(context.TODO(), id, "", t.UnixNano())
	}
}

// Reply to heartbeat messages from other servers.
func (s *Shim) hbReply(ctx context.Context, rev int64) {
	// Watch heartbeat records created after we initialized
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	hChan := s.Etcd.Watch(context.Background(), heartbeatPrefix, opts...)

	id := fmt.Sprintf("%d", rev)
	for w := range hChan {
		if w.Err() != nil {
			s.Log.Error(fmt.Sprintf("Heartbeat channel error %v", w.Err()))
			return
		}

		if w.IsProgressNotify() {
			s.Log.Error("No heartbeats for 10 minutes")
		}

		for _, e := range w.Events {
			if e.Type == 1 {
				// Ignore delete operations
				continue
			}
			originator, responder, ts := s.parseHB(e)
			delta := float64(s.clock.Now().UnixNano()-ts) / 1000000.0
			if originator == id {
				if responder == "" {
					s.Log.Info(fmt.Sprintf("self heartbeat in %.2fms", delta))
				} else {
					s.Log.Info(fmt.Sprintf("reply heartbeat from %s in %.2fms", responder, delta))
				}
			} else if originator != "" && responder == "" {
				// reply to foreign heartbeat
				s.Log.Debug(fmt.Sprintf("reply to %s", originator))
				_, err := s.sendHB(ctx, originator, id, ts)
				if err != nil {
					s.Log.Error(fmt.Sprintf("Error sending heartbeat reply to %s %v", originator, err))
				}
			}
		}
	}
}

// Send a heartbeat and return the store revision.
// Heartbeats are formatted as "H|<originator>|<responder>"
func (s *Shim) sendHB(ctx context.Context, orig, resp string, ts int64) (int64, error) {
	lease, err := s.Etcd.Grant(ctx, hbTTL)
	if err != nil {
		s.Log.Error("Failed to acquire heartbeat lease")
		return 0, err
	}

	key := fmt.Sprintf("%s%s%s%s", heartbeatPrefix, orig, delim, resp)
	value := fmt.Sprintf("%d", ts)
	res, err := s.Etcd.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}

	return res.Header.Revision, nil
}

// Parse a heartbeat and return the originator and responder ID strings and the timestamp
func (s *Shim) parseHB(hb *clientv3.Event) (string, string, int64) {
	ts, err := strconv.ParseInt(string(hb.Kv.Value), 10, 64)
	if err != nil {
		s.Log.Error(fmt.Sprintf("Invalid heartbeat payload %q %v", string(hb.Kv.Value), hb))
		return "", "", 0
	}
	items := strings.Split(string(hb.Kv.Key), delim)
	if len(items) == 2 {
		return items[1], "", ts
	}

	if len(items) == 3 {
		return items[1], items[2], ts
	}

	s.Log.Error(fmt.Sprintf("Invalid heartbeat %q", string(hb.Kv.Key)))

	return "", "", 0
}
