package etcd

// The cache is implemented as a single map of KeyValue objects organized by key (object ID).
// Index arrays of sorted keys are maintained for each object type - bundles, entries, and nodes.
//
// Reads are expected to meaningfully outweight writes, so cache segments are
// protected with a RWMutex to allow parallel read operations.
//
// The cache is loaded from backing store at startup and maintained through asynchronous
// watch functions that stream backend write events (create, delete, update).
//
// For etcd, the watch feature is builtin. For other KV stores and SQL databases,
// the watch feature may be implemented in an event table, and polling the backend
// store revision or another lightweight change indicator.

import (
	"context"
	"fmt"
	"sort"

	"github.com/spiffe/spire/pkg/server/plugin/store"
	ss "github.com/spiffe/spire/pkg/server/store"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/roguesoftware/etcd/mvcc/mvccpb"
)

const (
	// Number of cache entries to allocate on creation
	initialCacheSize = 1024

	// Number of records to load at a time during cache initialization
	loadPageSize = 10000
)

// cacheIndex consists of an ordered list and count of cached keys (object IDs).
type cacheIndex struct {
	Count int
	Keys  []string
}

// initializeCache loads cache data, sorted key indexes, and starts the watcher tasks.
// Bulk loading is performed at the same store revision for consistency.
// After the initial bulk load, watcher routines are started to
// continually update the cache as store updates are processed.
func (st *Plugin) initializeCache() error {
	// rev is a unique store revision and serves as both the heartbeat ID for this server
	// as well as the initial cache revision
	rev, err := st.StartHeartbeatService()
	if err != nil {
		return err
	}

	if st.cacheEnabled {
		err = st.loadItems(ss.BundleKeyID, st.bundleIndex, rev)
		if err != nil {
			return err
		}

		err = st.loadItems(ss.EntryKeyID, st.entryIndex, rev)
		if err != nil {
			return err
		}

		err = st.loadItems(ss.NodeKeyID, st.nodeIndex, rev)
		if err != nil {
			return err
		}

		// Start watchers at the store revision immediately following the bulk load
		go st.watchChanges(ss.BundlePrefix, st.bundleIndex, rev+1)

		go st.watchChanges(ss.EntryPrefix, st.entryIndex, rev+1)

		go st.watchChanges(ss.NodePrefix, st.nodeIndex, rev+1)

		st.mu.Lock()
		st.storeRevision = rev
		st.cacheInitialized = true
		st.log.Info("cache initialized", "count", len(st.cache))
		st.mu.Unlock()
	}

	// TODO handle exit conditions from the five previous routines

	return nil
}

// ResetCache returns the cache to the uninitialized state.
// Used by tests to employ different configurations
func (st *Plugin) resetCache() {
	st.etcd.Close()
	st.mu.Lock()
	st.cacheInitialized = false
	st.cache = make(map[string]*store.KeyValue, initialCacheSize)
	st.bundleIndex = &cacheIndex{Keys: []string{}}
	st.entryIndex = &cacheIndex{Keys: []string{}}
	st.nodeIndex = &cacheIndex{Keys: []string{}}
	st.mu.Unlock()
}

// loadItems performs the initial cache bulk load for the requested item type
func (st *Plugin) loadItems(key string, ci *cacheIndex, rev int64) error {
	// Cache is being initialized so it is safe to hold the lock the whole time
	st.mu.Lock()
	defer st.mu.Unlock()

	// Define a range to include all primary objects of the requested type (no indexes)
	start := fmt.Sprintf("%s%s", key, ss.Delim)
	end := fmt.Sprintf("%s%s", key, ss.Delend)
	for {
		res, err := st.Get(context.TODO(),
			&store.GetRequest{Key: start, End: end, Limit: loadPageSize, Revision: rev})
		if err != nil {
			return err
		}

		if res.Revision != rev {
			// Requested store revision was not honored
			return fmt.Errorf("Loaded revision (%d) does not match requested (%d)", res.Revision, rev)
		}

		lastKey := ""
		for _, kv := range res.Kvs {
			st.cache[kv.Key] = kv
			ci.Keys = append(ci.Keys, kv.Key)
			lastKey = kv.Key
		}

		st.log.Info("Cache load", "count", len(res.Kvs), "last", lastKey, "rev", rev)

		if len(res.Kvs) < loadPageSize {
			break
		}

		// Next batch starts one bit greater than the last returned key
		lastChar := lastKey[len(lastKey)-1] + 1
		start = fmt.Sprintf("%s%c", lastKey[:len(lastKey)-1], lastChar)
	}

	// Ensure index is sorted for range lookups
	sort.Strings(ci.Keys)
	ci.Count = len(ci.Keys)

	return nil
}

// watchChanges receives a stream of updates (deletes or puts) from etcd.
// These changes are applied to cached items, keeping server caches consistent,
// typically within 10's of milliseconds.
// Note: this is called as a goroutine and only returns after an error
func (st *Plugin) watchChanges(key string, ci *cacheIndex, rev int64) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}

	wc := st.etcd.Watch(context.TODO(), key, opts...)
	st.log.Info("Watching store updates", "key", key, "rev", rev)

	for w := range wc {
		if w.Err() != nil {
			st.log.Error("Watch: channel error", "err", w.Err())
			break
		}

		if w.IsProgressNotify() {
			st.log.Info("Watch: no updates for 10 minutes", "key", key)
			continue
		}

		// Hold lock for all updates to give other routines the most recent data.
		st.mu.Lock()

		for _, e := range w.Events {
			// fmt.Printf("Watch: %s %s\n", e.Type, e.Kv.Key)
			k := string(e.Kv.Key)
			if e.Type == mvccpb.DELETE {
				delete(st.cache, k)
				st.removeIndexKey(ci, k)
				st.storeRevision = e.Kv.ModRevision
			} else if e.Type == mvccpb.PUT {
				st.cache[string(e.Kv.Key)] = &store.KeyValue{
					Key:            k,
					Value:          e.Kv.Value,
					CreateRevision: e.Kv.CreateRevision,
					ModRevision:    e.Kv.ModRevision,
					Version:        e.Kv.Version,
				}
				st.insertIndexKey(ci, k)
				st.storeRevision = e.Kv.ModRevision
			} else {
				st.log.Error("Watch: unknown event", "type", e.Type, "kv", e.Kv)
			}
		}

		st.mu.Unlock()
	}

	// TODO restart at last successfully updated store revision

	return nil
}

// insertIndexKey inserts the given key into the given index if it does not already exist.
// This function must be called with st.mu.Lock() already held.
func (st *Plugin) insertIndexKey(ci *cacheIndex, key string) {
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
// This function must be called with st.mu.Lock() already held.
func (st *Plugin) removeIndexKey(ci *cacheIndex, key string) {
	i := sort.SearchStrings(ci.Keys, key)
	if i < len(ci.Keys) && ci.Keys[i] == key {
		ci.Keys = append(ci.Keys[:i], ci.Keys[i+1:]...)
		ci.Count = len(ci.Keys)
		return
	}
	// Not found
	return
}

// findIndexKeyEnd returns a slice of keys from the given cache index bounded by key and end.
// This function must be called with st.mu.RLock() already held.
func (st *Plugin) findIndexKeyEnd(key, end string) []string {
	var ci *cacheIndex
	if ss.IsBundleKey(key) {
		ci = st.bundleIndex
	} else if ss.IsEntryKey(key) {
		ci = st.entryIndex
	} else if ss.IsNodeKey(key) {
		ci = st.nodeIndex
	} else {
		st.log.Error("Unknown key", "key", key)
		return []string{}
	}

	// Locate the index offsets for the beginning and end of the range
	start := sort.SearchStrings(ci.Keys, key)
	finish := sort.SearchStrings(ci.Keys, end)

	if start > finish {
		st.log.Error("Invalid range", "key", key, "end", end)
		return []string{}
	}

	if start == ci.Count {
		// not found
		return []string{}
	}

	return ci.Keys[start:finish]
}

func (st *Plugin) printCache(s string) {
	keys := ""
	for k := range st.cache {
		keys = fmt.Sprintf("%s  %s", keys, k)
	}
	fmt.Printf("%s cache: %s %v %v %v\n", s, keys, st.bundleIndex, st.entryIndex, st.nodeIndex)
}
