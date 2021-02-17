// Package etcd implements KV backend store.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/andres-erbsen/clock"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl"

	"github.com/spiffe/spire/pkg/common/catalog"
	"github.com/spiffe/spire/pkg/server/plugin/store"
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

	// These default values may be overridden by configuration
	dialTimeout = 5 * time.Second

	enableEotMarkers   = false
	writeResponseDelay = 0
)

// Plugin is a store plugin implemented with etcd
type Plugin struct {
	store.UnsafeStoreServer

	cache map[string]*store.KeyValue

	bundleIndex *cacheIndex
	entryIndex  *cacheIndex
	nodeIndex   *cacheIndex

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
		bundleIndex:       &cacheIndex{Keys: []string{}},
		entryIndex:        &cacheIndex{Keys: []string{}},
		nodeIndex:         &cacheIndex{Keys: []string{}},
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
	fmt.Println("Logger configured")
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
				fmt.Printf("Get (cached): %s\n", req.Key)
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

	fmt.Printf("Get: %s %s %d\n", req.Key, req.End, len(res.Kvs))
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
			fmt.Printf("Set: %s %s %s\n", element.Operation, kv.Key, kv.End)
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
		fmt.Printf("Set: tx failed\n")
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
