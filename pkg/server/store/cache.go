package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-hclog"
	"github.com/roguesoftware/etcd/clientv3"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
)

// Cache represents the in-memory cache for Store object groups
type Cache struct {
	datastore.DataStore

	clock clock.Clock
	log   hclog.Logger

	bundleMu            sync.Mutex
	bundleCacheEnabled  bool
	bundleStoreRevision int64
	bundles             map[string]*common.Bundle

	entryMu            sync.Mutex
	entryCacheEnabled  bool
	entryStoreRevision int64
	entries            map[string]*common.RegistrationEntry

	nodeMu            sync.Mutex
	nodeCacheEnabled  bool
	nodeStoreRevision int64
	nodes             map[string]*common.AttestedNode

	tokenMu            sync.Mutex
	tokenCacheEnabled  bool
	tokenStoreRevision int64
	tokens             map[string]*datastore.JoinToken
}

// Store cache constants
const (
	loadPageSize = 10000

	// TxEotTtl defines the lifespan of the end of transaction markers in seconds
	TxEotTTL = 60

	// Heartbeat interval for inter-server timing events
	storeCacheHeartbeatInterval = time.Second

	// Write response delay to reduce likelihood of cross server cache incoherency
	storeCacheUpdateDelay = time.Millisecond * 200
)

// NewCache returns an initialized cache object.
func NewCache(cfg *Configuration, clock clock.Clock, logger hclog.Logger) Cache {
	return Cache{
		clock: clock,
		log:   logger,

		bundles: map[string]*common.Bundle{},
		entries: map[string]*common.RegistrationEntry{},
		nodes:   map[string]*common.AttestedNode{},
		tokens:  map[string]*datastore.JoinToken{},

		bundleCacheEnabled: !cfg.DisableBundleCache,
		entryCacheEnabled:  !cfg.DisableEntryCache,
		nodeCacheEnabled:   !cfg.DisableNodeCache,
		tokenCacheEnabled:  !cfg.DisableTokenCache,
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

	if s.cache.bundleCacheEnabled {
		rev, err = s.loadBundles(rev)
		if err != nil {
			return err
		}
		go s.watchBundles(rev)
	}

	if s.cache.entryCacheEnabled {
		rev, err = s.loadEntries(rev)
		if err != nil {
			return err
		}
		go s.watchEntries(rev)
	}

	if s.cache.nodeCacheEnabled {
		_, err = s.loadNodes(rev)
		if err != nil {
			return err
		}
		go s.watchNodes(rev)
	}

	if s.cache.tokenCacheEnabled {
		_, err = s.loadTokens(rev)
		if err != nil {
			return err
		}
		go s.watchTokens(rev)
	}

	// Start server heartbeat
	// TODO

	return nil
}

// loadBundles performs the initial cache load for bundles
func (s *Shim) loadBundles(revision int64) (int64, error) {
	s.cache.bundleMu.Lock()
	defer s.cache.bundleMu.Unlock()

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
			// Requested store version was honored - something wrong with etcd
			return 0, fmt.Errorf("Revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(br.Bundles)
		token = br.Pagination.Token
		s.log.Info(fmt.Sprintf("load bundles count: %d, token: %s, rev: %d", count, token, rev))
		if token == "" || count == 0 {
			break
		}

		for _, b := range br.Bundles {
			s.cache.bundles[b.TrustDomainId] = b
		}
	}

	s.cache.bundleStoreRevision = rev

	return rev, nil
}

// loadEntries performs the initial cache load for registration entries
func (s *Shim) loadEntries(revision int64) (int64, error) {
	s.cache.entryMu.Lock()
	defer s.cache.entryMu.Unlock()

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
			// Requested store version was honored - something wrong with etcd
			return 0, fmt.Errorf("Revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(er.Entries)
		token = er.Pagination.Token
		s.log.Info(fmt.Sprintf("load entries count: %d, token: %s, rev: %d", count, token, rev))
		if token == "" || count == 0 {
			break
		}

		for _, e := range er.Entries {
			s.cache.entries[e.EntryId] = e
		}
	}

	s.cache.entryStoreRevision = rev

	return rev, nil
}

// loadNodes performs the initial cache load for attested nodes.
func (s *Shim) loadNodes(revision int64) (int64, error) {
	s.cache.nodeMu.Lock()
	defer s.cache.nodeMu.Unlock()

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
			// Requested store version was honored - something wrong with etcd
			return 0, fmt.Errorf("Revision returned (%d) does not match requested (%d)", r, rev)
		}

		count := len(nr.Nodes)
		token = nr.Pagination.Token
		s.log.Info(fmt.Sprintf("load nodes count: %d, token: %s, rev: %d", count, token, rev))
		if token == "" || count == 0 {
			break
		}

		for _, n := range nr.Nodes {
			s.cache.nodes[n.SpiffeId] = n
		}
	}

	s.cache.nodeStoreRevision = rev

	return rev, nil
}

// loadTokens performs the initial cache load for join tokens.
func (s *Shim) loadTokens(revision int64) (rev int64, err error) {
	s.cache.tokenMu.Lock()
	defer s.cache.tokenMu.Unlock()

	tr := &datastore.ListJoinTokensResponse{}
	rev = revision
	token := ""
	for {
		tr, rev, err = s.listJoinTokens(context.TODO(), rev, &datastore.ListJoinTokensRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		count := len(tr.JoinTokens)
		token = tr.Pagination.Token
		s.log.Info(fmt.Sprintf("load tokens count: %d, token: %s, rev: %d", count, token, rev))
		if token == "" || count == 0 {
			break
		}

		for _, t := range tr.JoinTokens {
			s.cache.tokens[t.Token] = t
		}
	}

	s.cache.tokenStoreRevision = rev

	return
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
	// Using bundleKeyID instead of bundlePrefix will also return all index updates
	bChan := s.Etcd.Watch(context.Background(), bundlePrefix, opts...)
	s.log.Info("Watching bundle updates")

	for w := range bChan {
		if w.Err() != nil {
			s.log.Error(fmt.Sprintf("bChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.log.Info("No bundle updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.log.Info(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			bundle := &common.Bundle{}
			err := proto.Unmarshal(e.Kv.Value, bundle)
			if err != nil {
				s.log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			s.cache.bundleMu.Lock()
			s.cache.bundles[bundle.TrustDomainId] = bundle
			s.cache.bundleMu.Unlock()
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
	// Using entryKeyID instead of entryPrefix will also return all index updates
	eChan := s.Etcd.Watch(context.Background(), entryPrefix, opts...)
	s.log.Info("Watching entry updates")

	for w := range eChan {
		if w.Err() != nil {
			s.log.Error(fmt.Sprintf("eChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.log.Info("No entry updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.log.Info(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			entry := &common.RegistrationEntry{}
			err := proto.Unmarshal(e.Kv.Value, entry)
			if err != nil {
				s.log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			s.cache.entryMu.Lock()
			s.cache.entries[entry.EntryId] = entry
			s.cache.entryMu.Unlock()
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
	// Using nodeKeyID instead of nodePrefix will also return all index updates
	nChan := s.Etcd.Watch(context.Background(), nodePrefix, opts...)
	s.log.Info("Watching node updates")

	for w := range nChan {
		if w.Err() != nil {
			s.log.Error(fmt.Sprintf("nChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.log.Info("No node updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.log.Info(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			node := &common.AttestedNode{}
			err := proto.Unmarshal(e.Kv.Value, node)
			if err != nil {
				s.log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			s.cache.nodeMu.Lock()
			s.cache.nodes[node.SpiffeId] = node
			s.cache.nodeMu.Unlock()
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
	// Using tokenKeyID instead of tokenPrefix will also return all index updates
	tChan := s.Etcd.Watch(context.Background(), tokenPrefix, opts...)
	s.log.Info("Watching join token updates")

	for w := range tChan {
		if w.Err() != nil {
			s.log.Error(fmt.Sprintf("tChan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			s.log.Info("No join token updates for 10 minutes")
			continue
		}

		for _, e := range w.Events {
			s.log.Info(fmt.Sprintf("%s %s at version %d at revision %d",
				e.Type, e.Kv.Key, e.Kv.Version, e.Kv.ModRevision))

			token := &datastore.JoinToken{}
			err := proto.Unmarshal(e.Kv.Value, token)
			if err != nil {
				s.log.Error(fmt.Sprintf("%v on %v", err, e.Kv.Value))
			}

			s.cache.tokenMu.Lock()
			s.cache.tokens[token.Token] = token
			s.cache.tokenMu.Unlock()
		}
	}

	// TODO restart at last successfully updated store revision

	return nil
}
