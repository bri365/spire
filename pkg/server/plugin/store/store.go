// Provides interfaces and adapters for the Store service
//
// Generated code. Do not modify by hand.
package store

import (
	"context"

	"github.com/spiffe/spire/pkg/common/catalog"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	"github.com/spiffe/spire/proto/spire/server/store"
	"google.golang.org/grpc"
)

type Compare = store.Compare                                   //nolint: golint
type GetRequest = store.GetRequest                             //nolint: golint
type GetResponse = store.GetResponse                           //nolint: golint
type KeyValue = store.KeyValue                                 //nolint: golint
type Operation = store.Operation                               //nolint: golint
type SetRequest = store.SetRequest                             //nolint: golint
type SetRequestElement = store.SetRequestElement               //nolint: golint
type SetResponse = store.SetResponse                           //nolint: golint
type StoreClient = store.StoreClient                           //nolint: golint
type StoreServer = store.StoreServer                           //nolint: golint
type UnimplementedStoreServer = store.UnimplementedStoreServer //nolint: golint
type UnsafeStoreServer = store.UnsafeStoreServer               //nolint: golint
type WatchRequest = store.WatchRequest                         //nolint: golint
type WatchResponse = store.WatchResponse                       //nolint: golint

const (
	Type                = "Store"
	Compare_EQUALS      = store.Compare_EQUALS      //nolint: golint
	Compare_NONE        = store.Compare_NONE        //nolint: golint
	Compare_NOT_PRESENT = store.Compare_NOT_PRESENT //nolint: golint
	Compare_PRESENT     = store.Compare_PRESENT     //nolint: golint
	Operation_COMPARE   = store.Operation_COMPARE   //nolint: golint
	Operation_DELETE    = store.Operation_DELETE    //nolint: golint
	Operation_NOOP      = store.Operation_NOOP      //nolint: golint
	Operation_PUT       = store.Operation_PUT       //nolint: golint
)

// Store is the client interface for the service type Store interface.
type Store interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Set(context.Context, *SetRequest) (*SetResponse, error)
}

// Plugin is the client interface for the service with the plugin related methods used by the catalog to initialize the plugin.
type Plugin interface {
	Configure(context.Context, *spi.ConfigureRequest) (*spi.ConfigureResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	GetPluginInfo(context.Context, *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error)
	Set(context.Context, *SetRequest) (*SetResponse, error)
}

// PluginServer returns a catalog PluginServer implementation for the Store plugin.
func PluginServer(server StoreServer) catalog.PluginServer {
	return &pluginServer{
		server: server,
	}
}

type pluginServer struct {
	server StoreServer
}

func (s pluginServer) PluginType() string {
	return Type
}

func (s pluginServer) PluginClient() catalog.PluginClient {
	return PluginClient
}

func (s pluginServer) RegisterPluginServer(server *grpc.Server) interface{} {
	store.RegisterStoreServer(server, s.server)
	return s.server
}

// PluginClient is a catalog PluginClient implementation for the Store plugin.
var PluginClient catalog.PluginClient = pluginClient{}

type pluginClient struct{}

func (pluginClient) PluginType() string {
	return Type
}

func (pluginClient) NewPluginClient(conn grpc.ClientConnInterface) interface{} {
	return AdaptPluginClient(store.NewStoreClient(conn))
}

func AdaptPluginClient(client StoreClient) Store {
	return pluginClientAdapter{client: client}
}

type pluginClientAdapter struct {
	client StoreClient
}

func (a pluginClientAdapter) Configure(ctx context.Context, in *spi.ConfigureRequest) (*spi.ConfigureResponse, error) {
	return a.client.Configure(ctx, in)
}

func (a pluginClientAdapter) Get(ctx context.Context, in *GetRequest) (*GetResponse, error) {
	return a.client.Get(ctx, in)
}

func (a pluginClientAdapter) GetPluginInfo(ctx context.Context, in *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error) {
	return a.client.GetPluginInfo(ctx, in)
}

func (a pluginClientAdapter) Set(ctx context.Context, in *SetRequest) (*SetResponse, error) {
	return a.client.Set(ctx, in)
}
