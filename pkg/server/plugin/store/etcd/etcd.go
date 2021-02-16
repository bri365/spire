// Package etcd implements KV backend Store
package etcd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl"

	"github.com/spiffe/spire/pkg/common/catalog"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	ss "github.com/spiffe/spire/pkg/server/store"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	pb "github.com/spiffe/spire/proto/spire/server/store"
	"github.com/zeebo/errs"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/roguesoftware/etcd/mvcc/mvccpb"
	"github.com/roguesoftware/etcd/pkg/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

const (
	// PluginName is the name of this store plugin implementation.
	PluginName = "etcd"

	// Default heartbeat interval in seconds
	heartbeatDefaultInterval = 5

	// Lifespan of heartbeat records in seconds before automatic deletion
	heartbeatTTL = 1

	// The number of cache entries to allocate on creation
	initialCacheSize = 1024

	// The number of records to load at a time during cache initialization
	loadPageSize = 10000
)

var (
	pluginInfo = spi.GetPluginInfoResponse{
		Description: "",
		DateCreated: "",
		Version:     "",
		Author:      "",
		Company:     "",
	}

	etcdError = errs.Class("store-etcd")

	// These default values may be overridden by configuration
	dialTimeout = 5 * time.Second

	enableEotMarkers   = false
	writeResponseDelay = 0
)

// Plugin is a store plugin implemented with etcd
type Plugin struct {
	store.UnsafeStoreServer

	cache map[string]*store.KeyValue
	index []string

	clock clock.Clock
	etcd  *clientv3.Client
	log   hclog.Logger
	mu    sync.RWMutex

	heartbeatInterval time.Duration
	cacheEnabled      bool
	cacheInitialized  bool
	storeRevision     int64
}

// New creates a new etcd plugin struct
func New() *Plugin {
	return &Plugin{
		cache:             make(map[string]*store.KeyValue, initialCacheSize),
		index:             make([]string, initialCacheSize),
		clock:             clock.New(),
		cacheEnabled:      true,
		heartbeatInterval: time.Duration(heartbeatDefaultInterval) * time.Second,
	}
}

// BuiltIn designates this plugin as part of the standard product
func BuiltIn() catalog.Plugin {
	return builtin(New())
}

func builtin(p *Plugin) catalog.Plugin {
	return catalog.MakePlugin(PluginName, store.PluginServer(p))
}

// Configuration for the datastore
// Pointers allow distinction between "unset" and "zero" values
type configuration struct {
	Endpoints          []string `hcl:"endpoints" json:"endpoints"`
	RootCAPath         string   `hcl:"root_ca_path" json:"root_ca_path"`
	ClientCertPath     string   `hcl:"client_cert_path" json:"client_cert_path"`
	ClientKeyPath      string   `hcl:"client_key_path" json:"client_key_path"`
	DialTimeout        *int     `hcl:"dial_timeout" json:"dial_timeout"`
	DisableCache       *bool    `hcl:"disable_cache" json:"disable_cache"`
	HeartbeatInterval  *int     `hcl:"heartbeat_interval" json:"heartbeat_interval"`
	WriteResponseDelay *int     `hcl:"write_response_delay" json:"write_response_delay"`
}

// GetPluginInfo returns etcd plugin information
func (*Plugin) GetPluginInfo(context.Context, *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error) {
	return &pluginInfo, nil
}

// SetLogger sets the plugin logger
func (st *Plugin) SetLogger(logger hclog.Logger) {
	st.log = logger
	st.log.Info("Logger configured")
}

func (cfg *configuration) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return errors.New("At least one endpoint must be set")
	}

	if cfg.ClientCertPath == "" || cfg.ClientKeyPath == "" || cfg.RootCAPath == "" {
		return errors.New("client_cert_path, client_key_path, and root_ca_path must be set")
	}

	return nil
}

// Configure parses HCL config payload into config struct and opens etcd connection
func (st *Plugin) Configure(ctx context.Context, req *spi.ConfigureRequest) (*spi.ConfigureResponse, error) {
	st.log.Info("Configuring etcd store")

	cfg := &configuration{}
	if err := hcl.Decode(cfg, req.Configuration); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if cfg.DialTimeout != nil {
		dialTimeout = time.Duration(*cfg.DialTimeout) * time.Second
	}

	if cfg.DisableCache != nil {
		st.cacheEnabled = !*cfg.DisableCache
	}

	if cfg.HeartbeatInterval != nil {
		st.heartbeatInterval = time.Duration(*cfg.HeartbeatInterval) * time.Second
	}

	if cfg.WriteResponseDelay != nil {
		writeResponseDelay = *cfg.WriteResponseDelay
	}

	// TODO set proper logging for etcd client
	// NOTE: etcd client is noisy
	// clientv3.SetLogger(grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout))
	clientv3.SetLogger(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, ioutil.Discard))

	if err := st.openConnection(cfg, false); err != nil {
		return nil, err
	}

	return &spi.ConfigureResponse{}, nil
}

// TODO explore opening multiple client connections for improved performance
func (st *Plugin) openConnection(cfg *configuration, isReadOnly bool) error {
	tlsInfo := transport.TLSInfo{
		KeyFile:        cfg.ClientKeyPath,
		CertFile:       cfg.ClientCertPath,
		TrustedCAFile:  cfg.RootCAPath,
		ClientCertAuth: true,
	}

	tls, err := tlsInfo.ClientConfig()
	if err != nil {
		return err
	}

	st.etcd, err = clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: dialTimeout,
		TLS:         tls,
	})
	if err != nil {
		return err
	}

	st.log.Info("Connected to etcd",
		"endpoints", cfg.Endpoints,
		"heartbeat interval", st.heartbeatInterval,
	)

	err = st.initialize()
	return nil
}

func (st *Plugin) close() {
	// TODO stop heartbeat routines?
	if st.etcd != nil {
		// TODO close watchers first?
		st.etcd.Close()
	}
}

// initialize loads cache data and starts the watcher tasks.
// Bulk loading is performed on the same store revision for consistency.
// After the initial bulk load, watcher routines are started to
// continually update the cache as store updates are processed.
func (st *Plugin) initialize() error {
	// Start with rev == 0 to retrieve from the latest store revision
	var rev int64
	var err error

	st.StartHeartbeatService()
	if err != nil {
		return err
	}

	if st.cacheEnabled {
		rev, err = st.loadItems(ss.BundleKeyID, 0)
		if err != nil {
			return err
		}

		_, err = st.loadItems(ss.EntryKeyID, rev)
		if err != nil {
			return err
		}

		_, err = st.loadItems(ss.NodeKeyID, rev)
		if err != nil {
			return err
		}

		go st.watchChanges(ss.BundlePrefix, rev+1)

		go st.watchChanges(ss.EntryPrefix, rev+1)

		go st.watchChanges(ss.NodePrefix, rev+1)

		st.cacheInitialized = true
		st.log.Info("cache initialized")
	}

	// TODO handle exit conditions from the five previous routines

	return nil
}

// loadItems performs the initial cache load for the requested item type
func (st *Plugin) loadItems(key string, rev int64) (int64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()

	start := fmt.Sprintf("%s%s", key, ss.Delim)
	end := fmt.Sprintf("%s%s", key, ss.Delend)
	for {
		res, err := st.Get(context.TODO(),
			&store.GetRequest{Key: start, End: end, Limit: loadPageSize, Revision: rev})
		if err != nil {
			return 0, err
		}

		if rev == 0 {
			// First read returns the current store revision if requested revision is 0
			rev = res.Revision
		} else if res.Revision != rev {
			// Requested store revision was not honored - something wrong with etcd?
			return 0, fmt.Errorf("Loaded revision (%d) does not match requested (%d)", res.Revision, rev)
		}

		if len(res.Kvs) == 0 {
			break
		}

		lastKey := ""
		for _, kv := range res.Kvs {
			st.cache[kv.Key] = kv
			lastKey = kv.Key
		}

		// Get next batch starting one bit greater than the last returned key
		lastChar := lastKey[len(lastKey)-1] + 1
		start = fmt.Sprintf("%s%c", lastKey[:len(lastKey)-1], lastChar)

		st.log.Info(fmt.Sprintf("Loaded %d items, last: %s, next: %s, rev: %d", len(res.Kvs), lastKey, start, rev))
	}

	st.storeRevision = rev

	return rev, nil
}

// watchChanges receives a stream of updates (deletes or puts)
// for cached items, beginning with changes in the given store revision.
// Note: this is called as a goroutine and only returns on an error
func (st *Plugin) watchChanges(key string, rev int64) error {
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
			if e.Type == mvccpb.DELETE {
				delete(st.cache, string(e.Kv.Key))
				st.storeRevision = e.Kv.ModRevision
			} else if e.Type == mvccpb.PUT {
				st.cache[string(e.Kv.Key)] = &store.KeyValue{
					Key:            string(e.Kv.Key),
					Value:          e.Kv.Value,
					CreateRevision: e.Kv.CreateRevision,
					ModRevision:    e.Kv.ModRevision,
					Version:        e.Kv.Version,
				}
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

// StartHeartbeatService initializes inter- and intra-server heartbeat monitoring.
// Heartbeats are short-lived records PUT in etcd by servers and responded to by
// other servers. Heartbeat keys include originator and responder IDs and the value
// is a timestamp from the originator. In this way, servers can track the latency
// from database write to async watch update across servers and time.
// TODO use heartbeat data to modulate write response time to ensure (improve) inter-server cache coherency.
func (st *Plugin) StartHeartbeatService() error {
	if st.heartbeatInterval == 0 {
		st.log.Warn("Heartbeat disabled")
		return nil
	}

	// Secure a unique store revision with an empty heartbeat
	ctx := context.TODO()
	rev, err := st.sendHB(ctx, "", "", 1)
	if err != nil {
		return fmt.Errorf("Error getting heartbeat ID %v", err)
	}

	st.log.Info(fmt.Sprintf("Starting heartbeat with id %d", rev))
	go st.heartbeatReply(context.TODO(), rev+1)
	go st.heartbeatSend(rev + 1)

	// Handle returns from above

	return nil
}

// Send periodic heartbeat messages
func (st *Plugin) heartbeatSend(rev int64) {
	id := fmt.Sprintf("%d", rev)
	// Loop every interval forever
	ticker := st.clock.Ticker(st.heartbeatInterval)
	for t := range ticker.C {
		st.log.Info(fmt.Sprintf("Sending heartbeat %q at %d", id, t.UnixNano()))
		fmt.Printf("Sending heartbeat %q at %d\n", id, t.UnixNano())
		st.sendHB(context.TODO(), id, "", t.UnixNano())
	}
}

// Reply to heartbeat messages from other servers.
func (st *Plugin) heartbeatReply(ctx context.Context, rev int64) {
	// Watch heartbeat records created after we initialized
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithProgressNotify(),
		clientv3.WithRev(rev),
	}
	hc := st.etcd.Watch(context.Background(), ss.HeartbeatPrefix, opts...)

	id := fmt.Sprintf("%d", rev)
	for w := range hc {
		if w.Err() != nil {
			st.log.Error(fmt.Sprintf("Heartbeat channel error %v", w.Err()))
			return
		}

		if w.IsProgressNotify() {
			st.log.Error("No heartbeats for 10 minutes")
		}

		for _, e := range w.Events {
			if e.Type == 1 {
				// Ignore delete operations
				continue
			}
			originator, responder, ts := st.parseHB(e)
			delta := float64(st.clock.Now().UnixNano()-ts) / 1000000.0
			if originator == id {
				if responder == "" {
					st.log.Info(fmt.Sprintf("self heartbeat in %.2fms", delta))
					fmt.Printf("self heartbeat in %.2fms\n", delta)
				} else {
					st.log.Info(fmt.Sprintf("reply heartbeat from %s in %.2fms", responder, delta))
				}
			} else if originator != "" && responder == "" {
				// reply to foreign heartbeat
				st.log.Debug(fmt.Sprintf("reply to %s", originator))
				_, err := st.sendHB(ctx, originator, id, ts)
				if err != nil {
					st.log.Error(fmt.Sprintf("Error sending heartbeat reply to %s %v", originator, err))
				}
			}
		}
	}
}

// Send a heartbeat and return the store revision.
// Heartbeats are formatted as "H|<originator>|<responder>"
func (st *Plugin) sendHB(ctx context.Context, orig, resp string, ts int64) (int64, error) {
	lease, err := st.etcd.Grant(ctx, heartbeatTTL)
	if err != nil {
		st.log.Error("Failed to acquire heartbeat lease")
		return 0, err
	}

	key := fmt.Sprintf("%s%s%s%s", ss.HeartbeatPrefix, orig, ss.Delim, resp)
	value := fmt.Sprintf("%d", ts)
	res, err := st.etcd.Put(ctx, key, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return 0, err
	}

	return res.Header.Revision, nil
}

// Parse a heartbeat and return the originator and responder ID strings and the timestamp
func (st *Plugin) parseHB(hb *clientv3.Event) (string, string, int64) {
	ts, err := strconv.ParseInt(string(hb.Kv.Value), 10, 64)
	if err != nil {
		st.log.Error(fmt.Sprintf("Invalid heartbeat payload %q %v", string(hb.Kv.Value), hb))
		return "", "", 0
	}

	items := strings.Split(string(hb.Kv.Key), ss.Delim)
	if len(items) == 2 {
		return items[1], "", ts
	}

	if len(items) == 3 {
		return items[1], items[2], ts
	}

	st.log.Error(fmt.Sprintf("Invalid heartbeat %q", string(hb.Kv.Key)))

	return "", "", 0
}

// Get retrieves one or more items by key range.
func (st *Plugin) Get(ctx context.Context, req *store.GetRequest) (*store.GetResponse, error) {
	if st.cacheInitialized {
		if req.End == "" {
			// Single key
			if _, ok := st.cache[req.Key]; ok {
				st.mu.RLock()
				resp := &store.GetResponse{
					Revision: st.storeRevision,
					Kvs:      []*store.KeyValue{st.cache[req.Key]},
					More:     false,
					Total:    1,
				}
				st.mu.RUnlock()
				return resp, nil
			}
		} else {
			// Range of keys
		}
	}

	opts := []clientv3.OpOption{}
	if req.End != "" {
		opts = append(opts, clientv3.WithRange(req.End))
		opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	}

	if req.Limit != 0 {
		opts = append(opts, clientv3.WithLimit(req.Limit))
	}

	res, err := st.etcd.Get(ctx, req.Key, opts...)
	if err != nil {
		return nil, err
	}

	kvs := []*store.KeyValue{}
	for _, kv := range res.Kvs {
		kvs = append(kvs, &store.KeyValue{
			Key:            string(kv.Key),
			Value:          kv.Value,
			CreateRevision: kv.CreateRevision,
			ModRevision:    kv.ModRevision,
			Version:        kv.Version,
		})
	}

	resp := &store.GetResponse{
		Revision: res.Header.Revision,
		Kvs:      kvs,
		More:     res.More,
		Total:    res.Count,
	}

	return resp, nil
}

// Set adds, deletes, or updates one or more items in a single transaction.
// If a single comparison is requested, it should be listed first for the most precise error code.
func (st *Plugin) Set(ctx context.Context, req *store.SetRequest) (*store.SetResponse, error) {
	if len(req.Elements) == 0 {
		return nil, status.Error(codes.InvalidArgument, "store-etcd: one or more set elements required")
	}

	// Comparisons to perform before committing kv writes
	cmps := []clientv3.Cmp{}

	// Operations to execute if all comparisons are true
	ops := []clientv3.Op{}

	// Collect transaction operations for logging
	res := ""
	for _, element := range req.Elements {
		for _, kv := range element.Kvs {
			// Add requested comparison, if requested; default is no comparison
			if kv.Compare == pb.Compare_NOT_PRESENT {
				// Create new key
				cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), "=", 0))
				res = fmt.Sprintf("%s C %s @ 0", res, kv.Key)
			} else if kv.Compare == pb.Compare_PRESENT {
				// Update existing key
				cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), ">", 0))
				res = fmt.Sprintf("%s C %s > 0", res, kv.Key)
			} else if kv.Compare == pb.Compare_EQUALS {
				// Update with transactional integrity from a previous read operation
				cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), "=", kv.Version))
				res = fmt.Sprintf("%s C %s @ %d", res, kv.Key, kv.Version)
			}

			// Add the operation (Operation_COMPARE has no operation)
			if element.Operation == store.Operation_DELETE {
				opts := []clientv3.OpOption{}
				if kv.End != "" {
					opts = append(opts, clientv3.WithRange(kv.End))
					// TODO invalidate range of cache entries?
				} else {
					// TODO invalidate single cache entry?
				}
				ops = append(ops, clientv3.OpDelete(kv.Key, opts...))
				res = fmt.Sprintf("%s DEL %s %s", res, kv.Key, kv.End)
			} else if element.Operation == store.Operation_PUT {
				// TODO invalidate single cache entry?
				ops = append(ops, clientv3.OpPut(kv.Key, string(kv.Value)))
				res = fmt.Sprintf("%s PUT %s %v", res, kv.Key, kv.Value)
			}
		}
	}

	// Send the transaction to the etcd cluster
	t, err := st.etcd.Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()
	if err != nil {
		// return nil, err
		return nil, status.Error(codes.Unknown, fmt.Sprintf("%v %s", err, res))
	}
	if !t.Succeeded {
		return nil, status.Error(codes.Aborted, "store-etcd: missing or incorrect version")
		// return nil, status.Error(codes.Aborted, fmt.Sprintf("%s %v", res, t.Responses))
	}

	// Write delay for cluster sync time
	// Most, if not all, items should tolerate eventual consistency
	// Watch update latency is monitored through periodic heartbeats
	// and the delay value can be adjusted if stricter consistency is desired
	if writeResponseDelay > 0 {
		time.Sleep(time.Duration(writeResponseDelay) * time.Millisecond)
	}

	resp := &store.SetResponse{Revision: t.Header.Revision}

	// TODO consider returning responses for the operations

	return resp, nil
}
