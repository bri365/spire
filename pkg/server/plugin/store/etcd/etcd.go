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
)

const (
	// PluginName for this plugin
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

// Plugin is a DataStore plugin implemented via a SQL database
type Plugin struct {
	store.UnsafeStoreServer

	mu  sync.Mutex
	log hclog.Logger
}

// New creates a new etcd plugin struct
func New() *Plugin {
	return &Plugin{}
}

// BuiltIn for catalog plugins
func BuiltIn() catalog.Plugin {
	return builtin(New())
}

func builtin(p *Plugin) catalog.Plugin {
	return catalog.MakePlugin(PluginName, store.PluginServer(p))
}

// Configuration for the datastore
// Pointer values are used to distinguish between "unset" and "zero" values
type configuration struct {
	DatabaseType       string  `hcl:"database_type" json:"database_type"`
	ConnectionString   string  `hcl:"connection_string" json:"connection_string"`
	RoConnectionString string  `hcl:"ro_connection_string" json:"ro_connection_string"`
	RootCAPath         string  `hcl:"root_ca_path" json:"root_ca_path"`
	ClientCertPath     string  `hcl:"client_cert_path" json:"client_cert_path"`
	ClientKeyPath      string  `hcl:"client_key_path" json:"client_key_path"`
	ConnMaxLifetime    *string `hcl:"conn_max_lifetime" json:"conn_max_lifetime"`
	MaxOpenConns       *int    `hcl:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns       *int    `hcl:"max_idle_conns" json:"max_idle_conns"`
	DisableMigration   bool    `hcl:"disable_migration" json:"disable_migration"`

	// Undocumented flags
	LogSQL bool `hcl:"log_sql" json:"log_sql"`
}

// GetPluginInfo returns the etcd plugin
func (*Plugin) GetPluginInfo(context.Context, *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error) {
	return &pluginInfo, nil
}

// SetLogger sets the plugin logger
func (st *Plugin) SetLogger(logger hclog.Logger) {
	st.log = logger
}

func (cfg *configuration) Validate() error {
	if cfg.DatabaseType == "" {
		return errors.New("database_type must be set")
	}

	if cfg.ConnectionString == "" {
		return errors.New("connection_string must be set")
	}

	return nil
}

// Configure parses HCL config payload into config struct, and opens new DB based on the result
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

	if config.RoConnectionString == "" {
		return &spi.ConfigureResponse{}, nil
	}

	if err := st.openConnection(config, true); err != nil {
		return nil, err
	}

	return &spi.ConfigureResponse{}, nil
}

func (st *Plugin) openConnection(config *configuration, isReadOnly bool) error {
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
