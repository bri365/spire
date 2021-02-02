// Package etcd implements KV backend Store
package etcd

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl"

	"github.com/spiffe/spire/pkg/common/catalog"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	"github.com/zeebo/errs"

	"github.com/roguesoftware/etcd/clientv3"
	"github.com/roguesoftware/etcd/pkg/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

// Plugin constant values
const (
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

	dialTimeout    = 5 * time.Second
	requestTimeout = 10 * time.Second
)

// Plugin is a Store plugin implemented via etcd
type Plugin struct {
	store.UnsafeStoreServer

	c   *clientv3.Client
	mu  sync.Mutex
	log hclog.Logger
}

// New creates a new etcd plugin struct
func New() *Plugin {
	return &Plugin{}
}

// BuiltIn designates this as a native plugins
func BuiltIn() catalog.Plugin {
	return builtin(New())
}

func builtin(p *Plugin) catalog.Plugin {
	return catalog.MakePlugin(PluginName, store.PluginServer(p))
}

// Configuration for the datastore
// Pointers allow distinction between "unset" and "zero" values
type configuration struct {
	Endpoints      []string `hcl:"endpoints" json:"endpoints"`
	RootCAPath     string   `hcl:"root_ca_path" json:"root_ca_path"`
	ClientCertPath string   `hcl:"client_cert_path" json:"client_cert_path"`
	ClientKeyPath  string   `hcl:"client_key_path" json:"client_key_path"`
	DialTimeout    *int     `hcl:"dial_timeout" json:"dial_timeout"`
	RequestTimeout *int     `hcl:"request_timeout" json:"request_timeout"`

	// Undocumented flags
	LogOps bool `hcl:"log_ops" json:"log_ops"`
}

// GetPluginInfo returns etcd plugin information
func (*Plugin) GetPluginInfo(context.Context, *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error) {
	return &pluginInfo, nil
}

// SetLogger sets the plugin logger
func (st *Plugin) SetLogger(logger hclog.Logger) {
	st.log = logger
}

func (cfg *configuration) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return errors.New("At least one endpoint must be set")
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

	if cfg.RequestTimeout != nil {
		requestTimeout = time.Duration(*cfg.RequestTimeout) * time.Second
	}

	// TODO set proper logging
	clientv3.SetLogger(grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout))

	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.openConnection(cfg, false); err != nil {
		return nil, err
	}

	return &spi.ConfigureResponse{}, nil
}

// Create adds one or more new items in a single transaction.
func (st *Plugin) Create(ctx context.Context, req *store.PutRequest) (*store.PutResponse, error) {
	// Comparisons to verify keys do not already exist (version is 0)
	cmps := []clientv3.Cmp{}

	// Operations to execute if all comparisons are true
	ops := []clientv3.Op{}

	for _, kv := range req.Kvs {
		cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), "=", 0))
		ops = append(ops, clientv3.OpPut(kv.Key, string(kv.Value)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	t, err := st.c.Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()
	cancel()
	if err != nil {
		return nil, err
	}
	if !t.Succeeded {
		return nil, status.Error(codes.AlreadyExists, "store-etcd: record already exists")
	}

	return &store.PutResponse{Revision: t.Header.Revision}, nil
}

// Delete removes one or more item(s) as a single transaction.
// A range and/or an array of keys (KV entries) is required.
// Any key/value may contain a Version, which will be used to validate that key before deletion.
// If any version check fails, the entire transaction will be aborted.
func (st *Plugin) Delete(ctx context.Context, req *store.DeleteRequest) (*store.DeleteResponse, error) {
	if req.Range == nil && len(req.Kvs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "store-etcd: kv and/or range required")
	}

	// Comparisons to verify keys exist (version > 0) or are requested version
	cmps := []clientv3.Cmp{}

	// Operations to execute
	ops := []clientv3.Op{}

	// Add range delete to transaction
	if req.Range != nil {
		if req.Range.Key == "" || req.Range.End == "" {
			return nil, status.Error(codes.InvalidArgument, "store-etcd: range key and end required")
		}

		opts := []clientv3.OpOption{clientv3.WithRange(req.Range.End)}
		ops = append(ops, clientv3.OpDelete(req.Range.Key, opts...))
	}

	// Add each KV delete, with optional checks, to transaction
	for _, kv := range req.Kvs {
		// Version of -1 for no compare, 0 to ensure key exists, > 0 to confirm current version
		if kv.Version == 0 {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), ">", 0))
		} else if kv.Version > 0 {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), "=", kv.Version))
		}

		ops = append(ops, clientv3.OpDelete(kv.Key))
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	t, err := st.c.Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()
	cancel()
	if err != nil {
		return nil, err
	}
	if !t.Succeeded {
		return nil, status.Error(codes.Aborted, "store-etcd: missing or incorrect version")
	}

	return &store.DeleteResponse{Revision: t.Header.Revision}, nil
}

func (st *Plugin) close() {
	if st.c != nil {
		st.c.Close()
	}
}

// Get retrieves one or more item(s) by key range.
func (st *Plugin) Get(ctx context.Context, req *store.GetRequest) (*store.GetResponse, error) {
	opts := []clientv3.OpOption{}
	if req.End != "" {
		opts = append(opts, clientv3.WithRange(req.End))
	}

	if req.Limit != 0 {
		opts = append(opts, clientv3.WithLimit(req.Limit))
	}

	res, err := st.c.Get(ctx, req.Key, opts...)
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
	gr := &store.GetResponse{
		Revision: res.Header.Revision,
		Kvs:      kvs,
		More:     res.More,
		Total:    res.Count,
	}

	return gr, nil
}

// Transaction performs the given operations as a single transaction.
func (st *Plugin) Transaction(ctx context.Context, req *store.TransactionRequest) (*store.TransactionResponse, error) {
	// TODO implement
	return &store.TransactionResponse{}, nil
}

// Update modifies one or more existing item(s) in a single transaction.
func (st *Plugin) Update(ctx context.Context, req *store.PutRequest) (*store.PutResponse, error) {
	// Comparisons to verify keys already exist (version > 0)
	cmps := []clientv3.Cmp{}
	// Operations to execute if all comparisons are true
	ops := []clientv3.Op{}

	for _, kv := range req.Kvs {
		// ensure items exists or is correct version (for read-modify-write operations)
		if kv.Version == 0 {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), ">", 0))
		} else {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(kv.Key), "=", kv.Version))
		}
		ops = append(ops, clientv3.OpPut(kv.Key, string(kv.Value)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	t, err := st.c.Txn(ctx).
		If(cmps...).
		Then(ops...).
		Commit()
	cancel()
	if err != nil {
		return nil, err
	}
	if !t.Succeeded {
		// TODO distinguish between not found and tranaction collision (wrong version)
		return nil, status.Error(codes.NotFound, "store-etcd: record not found")
	}

	return &store.PutResponse{Revision: t.Header.Revision}, nil
}

func (st *Plugin) openConnection(cfg *configuration, isReadOnly bool) error {
	if cfg.ClientCertPath == "" || cfg.ClientKeyPath == "" || cfg.RootCAPath == "" {
		return errors.New("client_cert_path, client_key_path, and root_ca_path must be set")
	}

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

	st.c, err = clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		DialTimeout: dialTimeout,
		TLS:         tls,
	})
	if err != nil {
		return err
	}

	// err = st.sanity()
	// if err != nil {
	// 	return err
	// }

	return nil
}
