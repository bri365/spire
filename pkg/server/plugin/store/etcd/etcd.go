// Package etcd implements KV backend Store
package etcd

import (
	"context"
	"errors"
	"sync"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl"

	"github.com/spiffe/spire/pkg/common/catalog"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	"github.com/zeebo/errs"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
	"google.golang.org/grpc/grpclog"
)

const (
	// PluginName is the name of this Store plugin implementation
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
)

// Plugin is a Store plugin implemented via etcd
type Plugin struct {
	store.UnsafeStoreServer

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
	config := &configuration{}
	if err := hcl.Decode(config, req.Configuration); err != nil {
		return nil, err
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	if err := st.openConnection(config, false); err != nil {
		return nil, err
	}

	return &spi.ConfigureResponse{}, nil
}

func (st *Plugin) openConnection(config *configuration, isReadOnly bool) error {

	tlsInfo := transport.TLSInfo{
		KeyFile:        "../tf-etcd-vsphere/certs/client-key.pem",
		CertFile:       "../tf-etcd-vsphere/certs/client.pem",
		TrustedCAFile:  "../tf-etcd-vsphere/certs/ca.pem",
		ClientCertAuth: true,
	}

	clientv3.SetLogger(grpclog.NewLoggerV2(os.Stderr, os.Stderr, os.Stderr))

	tls, err := tlsInfo.ClientConfig()
	if err != nil {
		log.Fatal(err)
	}

	kvc, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		TLS:         tls,
	})
	if err != nil {
		log.Fatal(err)
	}

	// make sure to close the client
	// defer kvc.Close()

	return nil
}

// Create adds one or more new item(s) to etcd
func (st *Plugin) Create(context.Context, *store.PutRequest) (*store.PutResponse, error) {
	return nil, nil
}

// Delete removes one or more item(s) from etcd
func (st *Plugin) Delete(context.Context, *store.DeleteRequest) (*store.DeleteResponse, error) {
	return nil, nil
}

// Get retrieves one or more item(s) from etcd
func (st *Plugin) Get(context.Context, *store.GetRequest) (*store.GetResponse, error) {
	return nil, nil
}

// Update modifies one or more existing etcd item(s)
func (st *Plugin) Update(context.Context, *store.PutRequest) (*store.PutResponse, error) {
	return nil, nil
}
