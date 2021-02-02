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
type DeleteRequest = store.DeleteRequest                       //nolint: golint
type DeleteResponse = store.DeleteResponse                     //nolint: golint
type GetRequest = store.GetRequest                             //nolint: golint
type GetResponse = store.GetResponse                           //nolint: golint
type KeyValue = store.KeyValue                                 //nolint: golint
type Operation = store.Operation                               //nolint: golint
type PutRequest = store.PutRequest                             //nolint: golint
type PutResponse = store.PutResponse                           //nolint: golint
type Range = store.Range                                       //nolint: golint
type StoreClient = store.StoreClient                           //nolint: golint
type StoreServer = store.StoreServer                           //nolint: golint
type TransactionElement = store.TransactionElement             //nolint: golint
type TransactionRequest = store.TransactionRequest             //nolint: golint
type TransactionResponse = store.TransactionResponse           //nolint: golint
type UnimplementedStoreServer = store.UnimplementedStoreServer //nolint: golint
type UnsafeStoreServer = store.UnsafeStoreServer               //nolint: golint

const (
	Type                = "Store"
	Compare_EQUALS      = store.Compare_EQUALS      //nolint: golint
	Compare_NONE        = store.Compare_NONE        //nolint: golint
	Compare_NOT_PRESENT = store.Compare_NOT_PRESENT //nolint: golint
	Compare_PRESENT     = store.Compare_PRESENT     //nolint: golint
	Operation_COMPARE   = store.Operation_COMPARE   //nolint: golint
	Operation_DELETE    = store.Operation_DELETE    //nolint: golint
	Operation_PUT       = store.Operation_PUT       //nolint: golint
)

// Store is the client interface for the service type Store interface.
type Store interface {
	Create(context.Context, *PutRequest) (*PutResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Transaction(context.Context, *TransactionRequest) (*TransactionResponse, error)
	Update(context.Context, *PutRequest) (*PutResponse, error)
}

// Plugin is the client interface for the service with the plugin related methods used by the catalog to initialize the plugin.
type Plugin interface {
	Configure(context.Context, *spi.ConfigureRequest) (*spi.ConfigureResponse, error)
	Create(context.Context, *PutRequest) (*PutResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	GetPluginInfo(context.Context, *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error)
	Transaction(context.Context, *TransactionRequest) (*TransactionResponse, error)
	Update(context.Context, *PutRequest) (*PutResponse, error)
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

func (a pluginClientAdapter) Create(ctx context.Context, in *PutRequest) (*PutResponse, error) {
	return a.client.Create(ctx, in)
}

func (a pluginClientAdapter) Delete(ctx context.Context, in *DeleteRequest) (*DeleteResponse, error) {
	return a.client.Delete(ctx, in)
}

func (a pluginClientAdapter) Get(ctx context.Context, in *GetRequest) (*GetResponse, error) {
	return a.client.Get(ctx, in)
}

func (a pluginClientAdapter) GetPluginInfo(ctx context.Context, in *spi.GetPluginInfoRequest) (*spi.GetPluginInfoResponse, error) {
	return a.client.GetPluginInfo(ctx, in)
}

func (a pluginClientAdapter) Transaction(ctx context.Context, in *TransactionRequest) (*TransactionResponse, error) {
	return a.client.Transaction(ctx, in)
}

func (a pluginClientAdapter) Update(ctx context.Context, in *PutRequest) (*PutResponse, error) {
	return a.client.Update(ctx, in)
}
