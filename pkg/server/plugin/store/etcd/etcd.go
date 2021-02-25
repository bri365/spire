// Package etcd implements KV backend store.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl"

	"github.com/spiffe/spire/pkg/common/catalog"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	ss "github.com/spiffe/spire/pkg/server/store"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	pb "github.com/spiffe/spire/proto/spire/server/store"
	"github.com/zeebo/errs"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/roguesoftware/etcd/pkg/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

const (
	// PluginName is the name of this store plugin implementation.
	PluginName = "etcd"
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

	// Default values may be overridden by configuration
	dialTimeout = 5 * time.Second

	enableEotMarkers   = false
	writeResponseDelay = 0
)

// Plugin is a store plugin implemented with etcd.
type Plugin struct {
	store.UnsafeStoreServer

	Cfg  *ss.Configuration
	Etcd *clientv3.Client
	mu   sync.Mutex
	log  hclog.Logger
}

// New creates a new etcd plugin.
func New() *Plugin {
	return &Plugin{}
}

// BuiltIn designates this plugin as part of the core product.
func BuiltIn() catalog.Plugin {
	return builtin(New())
}

func builtin(p *Plugin) catalog.Plugin {
	return catalog.MakePlugin(PluginName, store.PluginServer(p))
}

// Pointers allow distinction between "unset" and "zero" values
type configuration struct {
	Endpoints      []string `hcl:"endpoints" json:"endpoints"`
	RootCAPath     string   `hcl:"root_ca_path" json:"root_ca_path"`
	ClientCertPath string   `hcl:"client_cert_path" json:"client_cert_path"`
	ClientKeyPath  string   `hcl:"client_key_path" json:"client_key_path"`
	DialTimeout    *int     `hcl:"dial_timeout" json:"dial_timeout"`

	EnableBundleCache           *bool `hcl:"enable_bundle_cache" json:"enable_bundle_cache"`
	EnableBundleCacheInvalidate bool  `hcl:"enable_bundle_cache_invalidate" json:"enable_bundle_cache_invalidate"`
	EnableBundleCacheUpdate     bool  `hcl:"enable_bundle_cache_update" json:"enable_bundle_cache_update"`

	EnableEntryCache           *bool `hcl:"enable_entry_cache" json:"enable_entry_cache"`
	EnableEntryCacheInvalidate bool  `hcl:"enable_entry_cache_invalidate" json:"enable_entry_cache_invalidate"`
	EnableEntryCacheUpdate     bool  `hcl:"enable_entry_cache_update" json:"enable_entry_cache_update"`

	EnableNodeCache           *bool `hcl:"enable_node_cache" json:"enable_node_cache"`
	EnableNodeCacheFetch      *bool `hcl:"enable_node_cache_fetch" json:"enable_node_cache_fetch"`
	EnableNodeCacheInvalidate bool  `hcl:"enable_node_cache_invalidate" json:"enable_node_cache_invalidate"`
	EnableNodeCacheUpdate     bool  `hcl:"enable_node_cache_update" json:"enable_node_cache_update"`

	EnableTokenCache           *bool `hcl:"enable_token_cache" json:"enable_token_cache"`
	EnableTokenCacheInvalidate bool  `hcl:"enable_token_cache_invalidate" json:"enable_token_cache_invalidate"`
	EnableTokenCacheUpdate     bool  `hcl:"enable_token_cache_update" json:"enable_token_cache_update"`

	EnableEotMarkers   *bool `hcl:"enable_eot_markers" json:"enable_eot_markers"`
	HeartbeatInterval  *int  `hcl:"heartbeat_interval" json:"heartbeat_interval"`
	WriteResponseDelay *int  `hcl:"write_response_delay" json:"write_response_delay"`

	// Undocumented flags
	LogOps bool `hcl:"log_ops" json:"log_ops"`
}

// GetPluginInfo returns etcd plugin information.
func (*Plugin) GetPluginInfo(context.Context, *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error) {
	return &pluginInfo, nil
}

// SetLogger sets the plugin logger.
func (st *Plugin) SetLogger(logger hclog.Logger) {
	st.log = logger
}

// Validate checks the given plugin configuration.
func (cfg *configuration) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return errors.New("At least one endpoint must be set")
	}

	if cfg.ClientCertPath == "" || cfg.ClientKeyPath == "" || cfg.RootCAPath == "" {
		return errors.New("client_cert_path, client_key_path, and root_ca_path must be set")
	}

	return nil
}

// Configure parses HCL config into config struct, opens etcd connection, and initializes the cache.
func (st *Plugin) Configure(ctx context.Context, req *spi.ConfigureRequest) (*spi.ConfigureResponse, error) {
	cfg := &configuration{}
	if err := hcl.Decode(cfg, req.Configuration); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Set etcd level variables
	if cfg.DialTimeout != nil {
		dialTimeout = time.Duration(*cfg.DialTimeout) * time.Second
	}

	if cfg.EnableEotMarkers != nil {
		enableEotMarkers = *cfg.EnableEotMarkers
	}

	if cfg.WriteResponseDelay != nil {
		writeResponseDelay = *cfg.WriteResponseDelay
	}

	// Set Store level configuration
	st.Cfg = &ss.Configuration{
		EnableBundleCache:           true,
		EnableBundleCacheInvalidate: cfg.EnableBundleCacheInvalidate,
		EnableBundleCacheUpdate:     cfg.EnableBundleCacheUpdate,

		EnableEntryCache:           true,
		EnableEntryCacheInvalidate: cfg.EnableEntryCacheInvalidate,
		EnableEntryCacheUpdate:     cfg.EnableEntryCacheUpdate,

		EnableNodeCache:           true,
		EnableNodeCacheFetch:      true,
		EnableNodeCacheInvalidate: cfg.EnableNodeCacheInvalidate,
		EnableNodeCacheUpdate:     cfg.EnableNodeCacheUpdate,

		EnableTokenCache:           true,
		EnableTokenCacheInvalidate: cfg.EnableTokenCacheInvalidate,
		EnableTokenCacheUpdate:     cfg.EnableTokenCacheUpdate,

		HeartbeatInterval: ss.HeartbeatDefaultInterval,
	}

	if cfg.HeartbeatInterval != nil {
		st.Cfg.HeartbeatInterval = *cfg.HeartbeatInterval
	}

	// Store configuration settings
	if cfg.EnableBundleCache != nil {
		st.Cfg.EnableBundleCache = *cfg.EnableBundleCache
	}

	if cfg.EnableEntryCache != nil {
		st.Cfg.EnableEntryCache = *cfg.EnableEntryCache
	}

	if cfg.EnableNodeCache != nil {
		st.Cfg.EnableNodeCache = *cfg.EnableNodeCache
	}

	if cfg.EnableNodeCacheFetch != nil {
		st.Cfg.EnableNodeCacheFetch = *cfg.EnableNodeCacheFetch
	}

	if cfg.EnableTokenCache != nil {
		st.Cfg.EnableTokenCache = *cfg.EnableTokenCache
	}

	// TODO set proper logging for etcd client
	// NOTE: etcd client is noisy
	clientv3.SetLogger(grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout))
	// clientv3.SetLogger(grpclog.NewLoggerV2(ioutil.Discard, ioutil.Discard, ioutil.Discard))

	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.openConnection(cfg, false); err != nil {
		return nil, err
	}

	st.log.Info("Configured etcd store", "endpoints", cfg.Endpoints)

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

	st.log.Debug("Open etcd connection", "timeout", dialTimeout)
	st.Etcd, err = clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: dialTimeout,
		TLS:         tls,
	})
	if err != nil {
		return err
	}

	return nil
}

func (st *Plugin) close() {
	// TODO stop heartbeat routines?
	if st.Etcd != nil {
		// TODO close watchers first?
		st.Etcd.Close()
	}
}

// Get retrieves one or more items by key range.
func (st *Plugin) Get(ctx context.Context, req *store.GetRequest) (*store.GetResponse, error) {
	opts := []clientv3.OpOption{}
	if req.End != "" {
		opts = append(opts, clientv3.WithRange(req.End))
		opts = append(opts, clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	}

	if req.Limit != 0 {
		opts = append(opts, clientv3.WithLimit(req.Limit))
	}

	res, err := st.Etcd.Get(ctx, req.Key, opts...)
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
				}
				ops = append(ops, clientv3.OpDelete(kv.Key, opts...))
				res = fmt.Sprintf("%s DEL %s %s", res, kv.Key, kv.End)
			} else if element.Operation == store.Operation_PUT {
				ops = append(ops, clientv3.OpPut(kv.Key, string(kv.Value)))
				res = fmt.Sprintf("%s PUT %s %v", res, kv.Key, kv.Value)
			}
		}
	}

	// Send the transaction to the etcd cluster
	t, err := st.Etcd.Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()
	if err != nil {
		// return nil, err
		return nil, status.Error(codes.Unknown, fmt.Sprintf("%v %s", err, res))
	}
	if !t.Succeeded {
		// return nil, status.Error(codes.Aborted, "store-etcd: missing or incorrect version")
		return nil, status.Error(codes.Aborted, fmt.Sprintf("%s %v", res, t.Responses))
	}

	// Send End of Transaction marker if requested
	if enableEotMarkers {
		eot := fmt.Sprintf("%s%d", ss.TxPrefix, t.Header.Revision)
		lease, err := st.Etcd.Grant(ctx, ss.TxEotTTL)
		if err != nil {
			st.log.Error("Failed to acquire lease")
			return nil, err
		}
		_, err = st.Etcd.Put(ctx, eot, "", clientv3.WithLease(lease.ID))
		if err != nil {
			st.log.Error("Failed to write end of transaction marker")
			return nil, err
		}
	}

	// Configurable write response delay to allow cluster nodes to sync changes
	// Most, if not all, items should tolerate eventual consistency (typically 10's of milliseconds)
	// Update latency is monitored through periodic heartbeats and the delay
	// value can be adjusted if stricter consistency is desired

	// TODO/NOTE can certain writes be subjected to two phase commit in cases where we want servers to
	// acknowledge changes have been updated in cache. For example, in an agent (node) SVID rotation
	// attack scenario, an attacker could try to rotate the agent SVID immediately after the authentic
	// agent, hitting the cache to authenticate, and assuming the identity of the agent with a second newer.
	// This scenario may also be prevented by having the rotation query the backend store before updating the
	// SVID regardless of authentication. Additionally, a "stale rotation" attempt will fail the transaction
	// consistency check when the update write is attempted on the backend.
	if writeResponseDelay > 0 {
		time.Sleep(time.Duration(writeResponseDelay) * time.Millisecond)
	}

	resp := &store.SetResponse{Revision: t.Header.Revision}

	// TODO consider returning responses for the operations

	return resp, nil
}

// Watch returns a stream of object write operations.
// TODO figure out how to implement gRPC streaming with the current catalog bypass
func (st *Plugin) Watch(ctx context.Context, req *store.WatchRequest) (*store.WatchResponse, error) {
	return nil, nil
}
