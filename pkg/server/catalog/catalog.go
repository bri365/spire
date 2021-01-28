package catalog

import (
	"context"
	"errors"
	"fmt"

	"github.com/andres-erbsen/clock"
	"github.com/sirupsen/logrus"
	"github.com/spiffe/spire/pkg/common/catalog"
	common_log "github.com/spiffe/spire/pkg/common/log"
	common_services "github.com/spiffe/spire/pkg/common/plugin/hostservices"
	"github.com/spiffe/spire/pkg/common/telemetry"
	datastore_telemetry "github.com/spiffe/spire/pkg/common/telemetry/server/datastore"
	keymanager_telemetry "github.com/spiffe/spire/pkg/common/telemetry/server/keymanager"
	"github.com/spiffe/spire/pkg/server/cache/dscache"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	ds_sql "github.com/spiffe/spire/pkg/server/plugin/datastore/sql"
	"github.com/spiffe/spire/pkg/server/plugin/hostservices"
	"github.com/spiffe/spire/pkg/server/plugin/keymanager"
	km_disk "github.com/spiffe/spire/pkg/server/plugin/keymanager/disk"
	km_memory "github.com/spiffe/spire/pkg/server/plugin/keymanager/memory"
	"github.com/spiffe/spire/pkg/server/plugin/nodeattestor"
	na_aws_iid "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/aws"
	na_azure_msi "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/azure"
	na_gcp_iit "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/gcp"
	na_join_token "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/jointoken"
	na_k8s_psat "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/k8s/psat"
	na_k8s_sat "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/k8s/sat"
	na_sshpop "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/sshpop"
	na_x509pop "github.com/spiffe/spire/pkg/server/plugin/nodeattestor/x509pop"
	"github.com/spiffe/spire/pkg/server/plugin/noderesolver"
	nr_aws_iid "github.com/spiffe/spire/pkg/server/plugin/noderesolver/aws"
	nr_azure_msi "github.com/spiffe/spire/pkg/server/plugin/noderesolver/azure"
	nr_noop "github.com/spiffe/spire/pkg/server/plugin/noderesolver/noop"
	"github.com/spiffe/spire/pkg/server/plugin/notifier"
	no_gcs_bundle "github.com/spiffe/spire/pkg/server/plugin/notifier/gcsbundle"
	no_k8sbundle "github.com/spiffe/spire/pkg/server/plugin/notifier/k8sbundle"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	st_etcd "github.com/spiffe/spire/pkg/server/plugin/store/etcd"
	"github.com/spiffe/spire/pkg/server/plugin/upstreamauthority"
	up_awspca "github.com/spiffe/spire/pkg/server/plugin/upstreamauthority/awspca"
	up_awssecret "github.com/spiffe/spire/pkg/server/plugin/upstreamauthority/awssecret"
	up_disk "github.com/spiffe/spire/pkg/server/plugin/upstreamauthority/disk"
	up_spire "github.com/spiffe/spire/pkg/server/plugin/upstreamauthority/spire"
	up_vault "github.com/spiffe/spire/pkg/server/plugin/upstreamauthority/vault"
	ss "github.com/spiffe/spire/pkg/server/store"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
)

var (
	builtIns = []catalog.Plugin{
		// DataStores
		ds_sql.BuiltIn(),
		// Stores
		st_etcd.BuiltIn(),
		// NodeAttestors
		na_aws_iid.BuiltIn(),
		na_gcp_iit.BuiltIn(),
		na_x509pop.BuiltIn(),
		na_sshpop.BuiltIn(),
		na_azure_msi.BuiltIn(),
		na_k8s_sat.BuiltIn(),
		na_k8s_psat.BuiltIn(),
		na_join_token.BuiltIn(),
		// NodeResolvers
		nr_noop.BuiltIn(),
		nr_aws_iid.BuiltIn(),
		nr_azure_msi.BuiltIn(),
		// UpstreamAuthorities
		up_awspca.BuiltIn(),
		up_awssecret.BuiltIn(),
		up_spire.BuiltIn(),
		up_disk.BuiltIn(),
		up_vault.BuiltIn(),
		// KeyManagers
		km_disk.BuiltIn(),
		km_memory.BuiltIn(),
		// Notifiers
		no_k8sbundle.BuiltIn(),
		no_gcs_bundle.BuiltIn(),
	}
)

type Catalog interface {
	GetStore() store.Store
	GetDataStore() datastore.DataStore
	GetNodeAttestorNamed(name string) (nodeattestor.NodeAttestor, bool)
	GetNodeResolverNamed(name string) (noderesolver.NodeResolver, bool)
	GetKeyManager() keymanager.KeyManager
	GetNotifiers() []Notifier
	GetUpstreamAuthority() (*UpstreamAuthority, bool)
}

type GlobalConfig = catalog.GlobalConfig
type HCLPluginConfig = catalog.HCLPluginConfig
type HCLPluginConfigMap = catalog.HCLPluginConfigMap

func KnownPlugins() []catalog.PluginClient {
	return []catalog.PluginClient{
		nodeattestor.PluginClient,
		noderesolver.PluginClient,
		upstreamauthority.PluginClient,
		keymanager.PluginClient,
		notifier.PluginClient,
	}
}

func KnownServices() []catalog.ServiceClient {
	return []catalog.ServiceClient{}
}

func BuiltIns() []catalog.Plugin {
	return append([]catalog.Plugin(nil), builtIns...)
}

type Store struct {
	catalog.PluginInfo
	store.Store
}

type DataStore struct {
	catalog.PluginInfo
	datastore.DataStore
}

type Notifier struct {
	catalog.PluginInfo
	notifier.Notifier
}

type UpstreamAuthority struct {
	catalog.PluginInfo
	upstreamauthority.UpstreamAuthority
}

type Plugins struct {
	// DataStore and Store are not filled directly by the catalog plugins
	DataStore DataStore `catalog:"-"`
	Store     Store     `catalog:"-"`

	NodeAttestors     map[string]nodeattestor.NodeAttestor
	NodeResolvers     map[string]noderesolver.NodeResolver
	UpstreamAuthority *UpstreamAuthority
	KeyManager        keymanager.KeyManager
	Notifiers         []Notifier
}

var _ Catalog = (*Plugins)(nil)

func (p *Plugins) GetStore() store.Store {
	return p.Store
}

func (p *Plugins) GetDataStore() datastore.DataStore {
	return p.DataStore
}

func (p *Plugins) GetNodeAttestorNamed(name string) (nodeattestor.NodeAttestor, bool) {
	n, ok := p.NodeAttestors[name]
	return n, ok
}

func (p *Plugins) GetNodeResolverNamed(name string) (noderesolver.NodeResolver, bool) {
	n, ok := p.NodeResolvers[name]
	return n, ok
}

func (p *Plugins) GetKeyManager() keymanager.KeyManager {
	return p.KeyManager
}

func (p *Plugins) GetNotifiers() []Notifier {
	return p.Notifiers
}

func (p *Plugins) GetUpstreamAuthority() (*UpstreamAuthority, bool) {
	return p.UpstreamAuthority, p.UpstreamAuthority != nil
}

type Config struct {
	Log          logrus.FieldLogger
	GlobalConfig *GlobalConfig
	PluginConfig HCLPluginConfigMap

	Metrics          telemetry.Metrics
	IdentityProvider hostservices.IdentityProviderServer
	AgentStore       hostservices.AgentStoreServer
	MetricsService   common_services.MetricsServiceServer
}

type Repository struct {
	Catalog
	catalog.Closer
}

func Load(ctx context.Context, config Config) (*Repository, error) {
	// Strip out the Store plugin configuration and load the etcd plugin
	// directly. This allows us to bypass gRPC and get rid of response limits.
	storeConfig := config.PluginConfig[store.Type]
	delete(config.PluginConfig, store.Type)
	st, err := loadEtcdStore(ctx, config.Log, storeConfig)
	if err != nil {
		return nil, err
	}

	// Strip out the Datastore plugin configuration and load the SQL plugin
	// directly. This allows us to bypass gRPC and get rid of response limits.
	dataStoreConfig := config.PluginConfig[datastore.Type]
	delete(config.PluginConfig, datastore.Type)
	ds, err := loadSQLDataStore(ctx, config.Log, dataStoreConfig)
	if err != nil {
		return nil, err
	}

	pluginConfigs, err := catalog.PluginConfigsFromHCL(config.PluginConfig)
	if err != nil {
		return nil, err
	}

	p := new(Plugins)
	closer, err := catalog.Fill(ctx, catalog.Config{
		Log:           config.Log,
		GlobalConfig:  config.GlobalConfig,
		PluginConfig:  pluginConfigs,
		KnownPlugins:  KnownPlugins(),
		KnownServices: KnownServices(),
		BuiltIns:      BuiltIns(),
		HostServices: []catalog.HostServiceServer{
			hostservices.IdentityProviderHostServiceServer(config.IdentityProvider),
			hostservices.AgentStoreHostServiceServer(config.AgentStore),
			common_services.MetricsServiceHostServiceServer(config.MetricsService),
		},
	}, p)
	if err != nil {
		return nil, err
	}

	ss_logger := common_log.NewHCLogAdapter(config.Log, telemetry.PluginBuiltIn).Named("shim")
	p.DataStore.DataStore = ss.New(ds, st, ss_logger)
	p.DataStore.DataStore = datastore_telemetry.WithMetrics(ds, config.Metrics)
	p.DataStore.DataStore = dscache.New(p.DataStore.DataStore, clock.New())
	p.KeyManager = keymanager_telemetry.WithMetrics(p.KeyManager, config.Metrics)

	return &Repository{
		Catalog: p,
		Closer:  closer,
	}, nil
}

func loadSQLDataStore(ctx context.Context, log logrus.FieldLogger, datastoreConfig map[string]catalog.HCLPluginConfig) (*ds_sql.Plugin, error) {
	switch {
	case len(datastoreConfig) == 0:
		return nil, errors.New("expecting a DataStore plugin")
	case len(datastoreConfig) > 1:
		return nil, errors.New("only one DataStore plugin is allowed")
	}

	sqlHCLConfig, ok := datastoreConfig[ds_sql.PluginName]
	if !ok {
		return nil, fmt.Errorf("pluggability for the DataStore is deprecated; only the built-in %q plugin is supported", ds_sql.PluginName)
	}

	sqlConfig, err := catalog.PluginConfigFromHCL(datastore.Type, ds_sql.PluginName, sqlHCLConfig)
	if err != nil {
		return nil, err
	}

	// Is the plugin external?
	if sqlConfig.Path != "" {
		return nil, fmt.Errorf("pluggability for the DataStore is deprecated; only the built-in %q plugin is supported", ds_sql.PluginName)
	}

	ds := ds_sql.New()
	ds.SetLogger(common_log.NewHCLogAdapter(log, telemetry.PluginBuiltIn).Named(sqlConfig.Name))
	if _, err := ds.Configure(ctx, &spi.ConfigureRequest{
		Configuration: sqlConfig.Data,
	}); err != nil {
		return nil, err
	}
	return ds, nil
}

func loadEtcdStore(ctx context.Context, log logrus.FieldLogger, storeConfig map[string]catalog.HCLPluginConfig) (*st_etcd.Plugin, error) {
	switch {
	case len(storeConfig) == 0:
		return nil, errors.New("expecting a Store plugin")
	case len(storeConfig) > 1:
		return nil, errors.New("only one Store plugin is allowed")
	}

	etcdHCLConfig, ok := storeConfig[st_etcd.PluginName]
	if !ok {
		return nil, fmt.Errorf("pluggability for the Store is incomplete; only the built-in %q plugin is supported", st_etcd.PluginName)
	}

	etcdConfig, err := catalog.PluginConfigFromHCL(store.Type, st_etcd.PluginName, etcdHCLConfig)
	if err != nil {
		return nil, err
	}

	// Is the plugin external?
	if etcdConfig.Path != "" {
		return nil, fmt.Errorf("pluggability for the Store is incomplete; only the built-in %q plugin is supported", st_etcd.PluginName)
	}

	st := st_etcd.New()
	st.SetLogger(common_log.NewHCLogAdapter(log, telemetry.PluginBuiltIn).Named(etcdConfig.Name))
	if _, err := st.Configure(ctx, &spi.ConfigureRequest{
		Configuration: etcdConfig.Data,
	}); err != nil {
		return nil, err
	}
	return st, nil
}
