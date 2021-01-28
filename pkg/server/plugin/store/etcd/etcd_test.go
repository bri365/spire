package etcd

import (
	"context"
	"crypto/x509"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/common/bundleutil"
	common_log "github.com/spiffe/spire/pkg/common/log"
	"github.com/spiffe/spire/pkg/common/telemetry"

	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	ss "github.com/spiffe/spire/pkg/server/store"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	"github.com/spiffe/spire/test/clock"
	"github.com/spiffe/spire/test/spiretest"
	testutil "github.com/spiffe/spire/test/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	_ttl                   = time.Hour
	_expiredNotAfterString = "2018-01-10T01:34:00+00:00"
	_validNotAfterString   = "2018-01-10T01:36:00+00:00"
	_middleTimeString      = "2018-01-10T01:35:00+00:00"
	_alreadyExistsErrMsg   = "store-etcd: record already exists"
	_notFoundErrMsg        = "store-etcd: record not found"
)

var (
	ctx = context.Background()

	// The following are set by the linker during integration tests to
	// run these unit tests against various KV backends.
	TestEndpoints = []string{"192.168.50.181:2379", "192.168.50.182:2379", "192.168.50.183:2379"}
	TestCA        string
	TestCert      string
	TestKey       string
)

type PluginSuite struct {
	spiretest.Suite

	cert   *x509.Certificate
	cacert *x509.Certificate

	dir        string
	nextID     int
	st         store.Plugin
	shim       ss.Shim
	etcdPlugin *Plugin
}

func TestPlugin(t *testing.T) {
	spiretest.Run(t, new(PluginSuite))
}

func (s *PluginSuite) SetupSuite() {
	clk := clock.NewMock(s.T())

	expiredNotAfterTime, err := time.Parse(time.RFC3339, _expiredNotAfterString)
	s.Require().NoError(err)
	validNotAfterTime, err := time.Parse(time.RFC3339, _validNotAfterString)
	s.Require().NoError(err)

	caTemplate, err := testutil.NewCATemplate(clk, spiffeid.RequireTrustDomainFromString("foo"))
	s.Require().NoError(err)

	caTemplate.NotAfter = expiredNotAfterTime
	caTemplate.NotBefore = expiredNotAfterTime.Add(-_ttl)

	cacert, cakey, err := testutil.SelfSign(caTemplate)
	s.Require().NoError(err)

	svidTemplate, err := testutil.NewSVIDTemplate(clk, "spiffe://foo/id1")
	s.Require().NoError(err)

	svidTemplate.NotAfter = validNotAfterTime
	svidTemplate.NotBefore = validNotAfterTime.Add(-_ttl)

	cert, _, err := testutil.Sign(svidTemplate, cacert, cakey)
	s.Require().NoError(err)

	s.cacert = cacert
	s.cert = cert
}

func (s *PluginSuite) SetupTest() {
	log, _ := common_log.NewLogger()
	ssLogger := common_log.NewHCLogAdapter(log, telemetry.PluginBuiltIn).Named("shim")
	s.st = s.newPlugin()
	shim := ss.New(nil, s.st, ssLogger)
	s.shim = *shim

	// delete all keys from the store
	res, err := s.st.Get(context.Background(), &store.GetRequest{
		Key: "A",
		End: "z",
	})
	if err != nil {
		s.T().Log(err)
	}

	if len(res.Kvs) > 0 {
		s.T().Logf("Deleting %d key(s) from store", len(res.Kvs))
		ranges := []*store.Range{}
		for _, kv := range res.Kvs {
			ranges = append(ranges, &store.Range{Key: kv.Key})
		}

		_, err = s.st.Delete(context.Background(), &store.DeleteRequest{
			Ranges: ranges,
		})
		if err != nil {
			s.T().Log(err)
		}
	}
}

func (s *PluginSuite) TearDownTest() {
	s.etcdPlugin.close()
}

func (s *PluginSuite) newPlugin() store.Plugin {
	var st store.Plugin

	p := New()
	s.etcdPlugin = p
	s.LoadPlugin(builtin(p), &st)

	// s.T().Logf("Endpoints: %v", TestEndpoints)

	_, err := st.Configure(context.Background(), &spi.ConfigureRequest{
		Configuration: `
		endpoints = ["192.168.50.181:2379","192.168.50.182:2379","192.168.50.183:2379"]
		root_ca_path = "/Users/brian/Dev/scytale/performance-tests/etcd/tf-etcd-vsphere/certs/ca.pem"
		client_cert_path = "/Users/brian/Dev/scytale/performance-tests/etcd/tf-etcd-vsphere/certs/client.pem"
		client_key_path = "/Users/brian/Dev/scytale/performance-tests/etcd/tf-etcd-vsphere/certs/client-key.pem"
		`,
	})
	s.Require().NoError(err)

	return st
}

func (s *PluginSuite) TestBundleCRUD() {
	bundle := bundleutil.BundleProtoFromRootCA("spiffe://foo", s.cert)

	// fetch non-existent
	fresp, err := s.shim.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: "spiffe://foo"})
	s.Require().NoError(err)
	s.Require().NotNil(fresp)
	s.Require().Nil(fresp.Bundle)

	// delete non-existent
	_, err = s.shim.DeleteBundle(ctx, &datastore.DeleteBundleRequest{TrustDomainId: "spiffe://foo"})
	s.Equal(codes.NotFound, status.Code(err))
	s.RequireGRPCStatus(err, codes.NotFound, _notFoundErrMsg)

	// create
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle,
	})
	s.Require().NoError(err)

	// create again (constraint violation)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle,
	})
	s.Equal(codes.AlreadyExists, status.Code(err))
	s.RequireGRPCStatus(err, codes.AlreadyExists, _alreadyExistsErrMsg)

}