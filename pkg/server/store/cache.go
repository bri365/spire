package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/hashicorp/go-hclog"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
)

type bundleCacheEntry struct {
	bundle *common.Bundle
	mu     sync.Mutex
	ver    int64
}

type nodeCacheEntry struct {
	node *common.AttestedNode
	mu   sync.Mutex
	ver  int64
}

type registrationCacheEntry struct {
	entry *common.RegistrationEntry
	mu    sync.Mutex
	ver   int64
}

type tokenCacheEntry struct {
	bundle *datastore.JoinToken
	mu     sync.Mutex
	ver    int64
}

// Cache represents the in-memory cache for Store object groups
type Cache struct {
	datastore.DataStore

	clock clock.Clock
	log   hclog.Logger

	// Bundle and Registration Entry acess is protected together for federation
	brCacheEnabled  bool
	brStoreRevision int64
	bundleRegMu     sync.Mutex
	bundles         map[string]*bundleCacheEntry
	registrations   map[string]*registrationCacheEntry

	nCacheEnabled  bool
	nStoreRevision int64
	nodeMu         sync.Mutex
	nodes          map[string]*nodeCacheEntry

	tCacheEnabled  bool
	tStoreRevision int64
	tokenMu        sync.Mutex
	tokens         map[string]*tokenCacheEntry
}

// Store cache constants
const (
	loadPageSize = 10000

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

		bundles:       map[string]*bundleCacheEntry{},
		registrations: map[string]*registrationCacheEntry{},
		nodes:         map[string]*nodeCacheEntry{},
		tokens:        map[string]*tokenCacheEntry{},

		brCacheEnabled: !cfg.DisableBundleRegCache,
		nCacheEnabled:  !cfg.DisableNodeCache,
		tCacheEnabled:  !cfg.DisableTokenCache,
	}
}

// Initialize loads cache data and starts the watcher tasks.
// Bulk loading is performed on the same store revision.
// After the initial bulk load, watcher routines are started to
// continually update the cache as store updates are procesed.
// NOTE: for faster startup, we could start a go routine to load
// and validate the cache data.
func (s *Shim) Initialize() error {
	// Start with the latest revision (rev = 0)
	var rev int64
	var err error

	if s.cache.brCacheEnabled {
		rev, err = s.loadBundlesAndRegistrations(rev)
		if err != nil {
			return err
		}
		go s.watchBundlesAndRegistrations()
	}

	if s.cache.nCacheEnabled {
		_, err = s.loadNodes(rev)
		if err != nil {
			return err
		}
		go s.watchNodes()
	}

	if s.cache.tCacheEnabled {
		_, err = s.loadTokens(rev)
		if err != nil {
			return err
		}
		go s.watchTokens()
	}

	return nil
}

// loadBundlesAndRegistrations performs the initial cache load for
// bundles and registration entries under the same lock.
func (s *Shim) loadBundlesAndRegistrations(revision int64) (rev int64, err error) {
	s.cache.bundleRegMu.Lock()
	defer s.cache.bundleRegMu.Unlock()

	br := &datastore.ListBundlesResponse{}
	rev = revision
	token := ""
	for {
		br, rev, err = s.listBundles(context.TODO(), rev, &datastore.ListBundlesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		count := len(br.Bundles)
		token = br.Pagination.Token
		s.log.Info(fmt.Sprintf("load bundles count: %d, token: %s, rev: %d", count, token, rev))
		if token == "" || count == 0 {
			break
		}

		for _, b := range br.Bundles {
			s.cache.bundles[b.TrustDomainId] = &bundleCacheEntry{bundle: b}
		}
	}

	er := &datastore.ListRegistrationEntriesResponse{}
	token = ""
	for {
		er, rev, err = s.listRegistrationEntries(context.TODO(), rev, &datastore.ListRegistrationEntriesRequest{
			Pagination: &datastore.Pagination{Token: token, PageSize: loadPageSize}})
		if err != nil {
			return 0, err
		}

		count := len(er.Entries)
		token = er.Pagination.Token
		s.log.Info(fmt.Sprintf("load entries count: %d, token: %s, rev: %d", count, token, rev))
		if token == "" || count == 0 {
			break
		}

		for _, e := range er.Entries {
			s.cache.registrations[e.EntryId] = &registrationCacheEntry{entry: e}
		}
	}

	s.cache.brStoreRevision = rev

	return rev, nil
}

// loadNodes performs the initial cache load for attested nodes.
func (s *Shim) loadNodes(rev int64) (int64, error) {
	s.cache.nodeMu.Lock()
	defer s.cache.nodeMu.Unlock()
	//
	return rev, nil
}

// loadTokens performs the initial cache load for join tokens.
func (s *Shim) loadTokens(rev int64) (int64, error) {
	s.cache.tokenMu.Lock()
	defer s.cache.tokenMu.Unlock()
	//
	return rev, nil
}

// watchBundlesAndRegistrations receives a stream of updates (deletes or puts)
// for bundles and registration entries, starting at the given store revision.
func (s *Shim) watchBundlesAndRegistrations() error {
	return nil
}

// watchNodes receives a stream of updates (deletes or puts)
// for attested nodes, starting at the given store revision.
func (s *Shim) watchNodes() error {
	return nil
}

// watchTokens receives a stream of updates (deletes or puts)
// for join tokens, starting at the given store revision.
func (s *Shim) watchTokens() error {
	return nil
}
