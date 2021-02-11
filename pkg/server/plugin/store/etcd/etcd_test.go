package etcd

import (
	"context"
	"crypto/x509"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	common_log "github.com/spiffe/spire/pkg/common/log"
	"github.com/spiffe/spire/pkg/common/telemetry"
	"github.com/stretchr/testify/require"

	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	ss "github.com/spiffe/spire/pkg/server/store"
	"github.com/spiffe/spire/proto/spire/common"
	spi "github.com/spiffe/spire/proto/spire/common/plugin"
	"github.com/spiffe/spire/test/clock"
	"github.com/spiffe/spire/test/spiretest"
	testutil "github.com/spiffe/spire/test/util"
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
	shim       *ss.Shim
	etcdPlugin *Plugin
}

type ListRegistrationReq struct {
	name               string
	pagination         *datastore.Pagination
	selectors          []*common.Selector
	expectedList       []*common.RegistrationEntry
	expectedPagination *datastore.Pagination
	err                string
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
	var err error
	s.T().Log("SetupTest")
	log, _ := common_log.NewLogger()
	ssLogger := common_log.NewHCLogAdapter(log, telemetry.PluginBuiltIn).Named("shim")
	s.st = s.newPlugin()
	cfg := &ss.Configuration{}
	s.shim, err = ss.New(nil, s.st, ssLogger, cfg, s.etcdPlugin.Etcd)
	if err != nil {
		s.T().Log(err)
	}

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
		kvs := []*store.KeyValue{{Key: "A", End: "z"}}
		_, err = s.st.Set(context.Background(), &store.SetRequest{
			Elements: []*store.SetRequestElement{{Operation: store.Operation_DELETE, Kvs: kvs}},
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
		heartbeat_interval = 0
		write_response_delay = 0
		`,
	})
	s.Require().NoError(err)

	return st
}

func (s *PluginSuite) TestGetPluginInfo() {
	resp, err := s.st.GetPluginInfo(ctx, &spi.GetPluginInfoRequest{})
	s.Require().NoError(err)
	s.Require().NotNil(resp)
}

func (s *PluginSuite) TestRace() {
	next := int64(0)
	exp := time.Now().Add(time.Hour).Unix()

	testutil.RaceTest(s.T(), func(t *testing.T) {
		node := &common.AttestedNode{
			SpiffeId:            fmt.Sprintf("foo%d", atomic.AddInt64(&next, 1)),
			AttestationDataType: "aws-tag",
			CertSerialNumber:    "badcafe",
			CertNotAfter:        exp,
		}

		_, err := s.shim.CreateAttestedNode(ctx, &datastore.CreateAttestedNodeRequest{Node: node})
		require.NoError(t, err)
		_, err = s.shim.FetchAttestedNode(ctx, &datastore.FetchAttestedNodeRequest{SpiffeId: node.SpiffeId})
		require.NoError(t, err)
	})
}
