package etcd

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
// The cached elements are stored in unordered maps by object ID.
// Index arrays of ID strings are maintained for bundles, entries, and nodes

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
	// The number of cache entries to allocate on creation
	initialCacheSize = 1024

	// The number of records to load at a time during cache initialization
	loadPageSize = 10000
)

// cacheIndex maintains an ordered list and count of specific cache items.
type cacheIndex struct {
	Count int
	Keys  []string
}

// initialize loads cache data and starts the watcher tasks.
// Bulk loading is performed on the same store revision for consistency.
// After the initial bulk load, watcher routines are started to
// continually update the cache as store updates are processed.
func (st *Plugin) initialize() error {
	// rev is both the heartbeat ID for this server as well as the initial cache store revision
	rev, err := st.StartHeartbeatService()
	if err != nil {
		return err
	}

	if st.cacheEnabled {
		err = st.loadItems(ss.BundleKeyID, rev, st.bundleIndex)
		if err != nil {
			return err
		}

		err = st.loadItems(ss.EntryKeyID, rev, st.entryIndex)
		if err != nil {
			return err
		}

		err = st.loadItems(ss.NodeKeyID, rev, st.nodeIndex)
		if err != nil {
			return err
		}

		go st.watchChanges(ss.BundlePrefix, rev+1, st.bundleIndex)

		go st.watchChanges(ss.EntryPrefix, rev+1, st.entryIndex)

		go st.watchChanges(ss.NodePrefix, rev+1, st.nodeIndex)

		st.mu.RLock()
		st.storeRevision = rev
		st.cacheInitialized = true
		st.printCache("init")
		st.mu.RUnlock()

		st.log.Info("cache initialized")
	}

	// TODO handle exit conditions from the five previous routines

	return nil
}

func (st *Plugin) printCache(s string) {
	keys := ""
	for k := range st.cache {
		keys = fmt.Sprintf("%s  %s", keys, k)
	}
	fmt.Printf("%s cache: %s %v %v %v\n", s, keys, st.bundleIndex, st.entryIndex, st.nodeIndex)
}

// loadItems performs the initial cache load for the requested item type
func (st *Plugin) loadItems(key string, rev int64, ci *cacheIndex) error {
	// The cache is being initialized so it is safe to hold the lock for the whole time
	st.mu.Lock()
	defer st.mu.Unlock()

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

		if len(res.Kvs) == 0 {
			break
		}

		lastKey := ""
		for _, kv := range res.Kvs {
			st.cache[kv.Key] = kv
			ci.Keys = append(ci.Keys, kv.Key)
			lastKey = kv.Key
		}

		// Get next batch starting one bit greater than the last returned key
		lastChar := lastKey[len(lastKey)-1] + 1
		start = fmt.Sprintf("%s%c", lastKey[:len(lastKey)-1], lastChar)

		st.log.Info(fmt.Sprintf("Loaded %d items, last: %s, next: %s, rev: %d", len(res.Kvs), lastKey, start, rev))
	}

	// Ensure index is sorted for range lookups
	sort.Strings(ci.Keys)
	ci.Count = len(ci.Keys)

	return nil
}

// watchChanges receives a stream of updates (deletes or puts)
// for cached items, beginning with changes in the given store revision.
// Note: this is called as a goroutine and only returns on an error
func (st *Plugin) watchChanges(key string, rev int64, ci *cacheIndex) error {
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}

	wc := st.etcd.Watch(context.TODO(), key, opts...)
	st.log.Info(fmt.Sprintf("Watching store updates for %s from %d", key, rev))

	for w := range wc {
		if w.Err() != nil {
			st.log.Error(fmt.Sprintf("chan error %v", w.Err()))
			break
		}

		if w.IsProgressNotify() {
			st.log.Info(fmt.Sprintf("No store updates for %s in 10 minutes", key))
			continue
		}

		// Hold the lock for all updates to give other routines the most recent data.
		st.mu.Lock()

		for _, e := range w.Events {
			fmt.Printf("Watch: %s %s\n", e.Type, e.Kv.Key)
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
				st.log.Error(fmt.Sprintf("Unknown watch event %v", e))
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
	fmt.Printf("iIK %s %v\n", key, ci.Keys)
	i := sort.SearchStrings(ci.Keys, key)
	if i < len(ci.Keys) && ci.Keys[i] == key {
		fmt.Printf("iIK %s already exists\n", key)
		return
	}
	ci.Keys = append(ci.Keys, "")
	copy(ci.Keys[i+1:], ci.Keys[i:])
	ci.Keys[i] = key
	ci.Count = len(ci.Keys)
	st.printCache("ik")
	return
}

// removeIndexKey deletes the given key from the given index if it exists.
// This function must be called with st.mu.Lock() already held.
func (st *Plugin) removeIndexKey(ci *cacheIndex, key string) {
	fmt.Printf("rIK %s %v\n", key, ci.Keys)
	i := sort.SearchStrings(ci.Keys, key)
	if i < len(ci.Keys) && ci.Keys[i] == key {
		ci.Keys = append(ci.Keys[:i], ci.Keys[i+1:]...)
		ci.Count = len(ci.Keys)
		st.printCache("rk")
		return
	}
	fmt.Printf("rIK %s not found\n", key)
	return
}
