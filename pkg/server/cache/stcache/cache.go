package stcache

import (
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"golang.org/x/net/context"
)

const (
	storeCacheExpiry = time.Second
)

type useCache struct{}

// WithCache makes the store cache available to the context
func WithCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, useCache{}, struct{}{})
}

type bundleCacheEntry struct {
	mu   sync.Mutex
	resp *datastore.FetchBundleResponse
}

type entryCacheEntry struct {
	mu   sync.Mutex
	resp *datastore.FetchRegistrationEntryResponse
}

type nodeCacheEntry struct {
	mu   sync.Mutex
	resp *datastore.FetchAttestedNodeResponse
}

type tokenCacheEntry struct {
	mu   sync.Mutex
	resp *datastore.FetchJoinTokenResponse
}

// StoreCache represents a cache for datastore objects
type StoreCache struct {
	datastore.DataStore
	store.Store
	clock clock.Clock

	beMu    sync.Mutex
	bundles map[string]*bundleCacheEntry
	entries map[string]*entryCacheEntry

	nMu   sync.Mutex
	nodes map[string]*nodeCacheEntry

	tMu    sync.Mutex
	tokens map[string]*tokenCacheEntry
}

// New returns an initialized StoreCache
func New(ds datastore.DataStore, st store.Store, clock clock.Clock) *StoreCache {
	return &StoreCache{
		DataStore: ds,
		Store:     st,
		clock:     clock,
		bundles:   make(map[string]*bundleCacheEntry),
		entries:   make(map[string]*entryCacheEntry),
		nodes:     make(map[string]*nodeCacheEntry),
		tokens:    make(map[string]*tokenCacheEntry),
	}
}

func (sc *StoreCache) FetchBundle(ctx context.Context,
	req *datastore.FetchBundleRequest) (*datastore.FetchBundleResponse, error) {

	sc.beMu.Lock()
	defer sc.beMu.Unlock()
	cacheEntry, ok := sc.bundles[req.TrustDomainId]
	if !ok {
		return &datastore.FetchBundleResponse{}, nil
	}

	return cacheEntry.resp, nil
}
