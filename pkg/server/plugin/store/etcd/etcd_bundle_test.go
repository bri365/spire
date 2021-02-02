package etcd

import (
	"crypto/x509"
	"fmt"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/common/bundleutil"

	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (s *PluginSuite) TestBundleCRUD() {
	bundle := bundleutil.BundleProtoFromRootCA("spiffe://foo", s.cert)

	// fetch non-existent
	fresp, err := s.shim.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: "spiffe://foo"})
	s.Require().NoError(err)
	s.Require().NotNil(fresp)
	s.Require().Nil(fresp.Bundle)

	// update non-existent
	_, err = s.shim.UpdateBundle(ctx, &datastore.UpdateBundleRequest{Bundle: bundle})
	s.RequireGRPCStatus(err, codes.NotFound, _notFoundErrMsg)

	// delete non-existent
	_, err = s.shim.DeleteBundle(ctx, &datastore.DeleteBundleRequest{TrustDomainId: "spiffe://foo"})
	s.Equal(codes.NotFound, status.Code(err))
	s.RequireGRPCStatus(err, codes.NotFound, "store-etcd: record not found")

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

	// fetch
	fresp, err = s.shim.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: "spiffe://foo"})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle, fresp.Bundle)

	// fetch (with denormalized id)
	fresp, err = s.shim.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: "spiffe://fOO"})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle, fresp.Bundle)

	// list
	lresp, err := s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	s.Equal(1, len(lresp.Bundles))
	s.AssertProtoEqual(bundle, lresp.Bundles[0])

	bundle2 := bundleutil.BundleProtoFromRootCA(bundle.TrustDomainId, s.cacert)
	appendedBundle := bundleutil.BundleProtoFromRootCAs(bundle.TrustDomainId,
		[]*x509.Certificate{s.cert, s.cacert})

	// append
	aresp, err := s.shim.AppendBundle(ctx, &datastore.AppendBundleRequest{
		Bundle: bundle2,
	})
	s.Require().NoError(err)
	s.Require().NotNil(aresp.Bundle)
	s.AssertProtoEqual(appendedBundle, aresp.Bundle)

	// append identical
	aresp, err = s.shim.AppendBundle(ctx, &datastore.AppendBundleRequest{
		Bundle: bundle2,
	})
	s.Require().NoError(err)
	s.Require().NotNil(aresp.Bundle)
	s.AssertProtoEqual(appendedBundle, aresp.Bundle)

	// append on a new bundle
	bundle3 := bundleutil.BundleProtoFromRootCA("spiffe://bar", s.cacert)
	anresp, err := s.shim.AppendBundle(ctx, &datastore.AppendBundleRequest{
		Bundle: bundle3,
	})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle3, anresp.Bundle)

	// update with mask: RootCas
	uresp, err := s.shim.UpdateBundle(ctx, &datastore.UpdateBundleRequest{
		Bundle: bundle,
		InputMask: &common.BundleMask{
			RootCas: true,
		},
	})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle, uresp.Bundle)

	lresp, err = s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	assertBundlesEqual(s.T(), []*common.Bundle{bundle, bundle3}, lresp.Bundles)

	// update with mask: RefreshHint
	bundle.RefreshHint = 60
	uresp, err = s.shim.UpdateBundle(ctx, &datastore.UpdateBundleRequest{
		Bundle: bundle,
		InputMask: &common.BundleMask{
			RefreshHint: true,
		},
	})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle, uresp.Bundle)

	lresp, err = s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	assertBundlesEqual(s.T(), []*common.Bundle{bundle, bundle3}, lresp.Bundles)

	// update with mask: JwtSingingKeys
	bundle.JwtSigningKeys = []*common.PublicKey{{Kid: "jwt-key-1"}}
	uresp, err = s.shim.UpdateBundle(ctx, &datastore.UpdateBundleRequest{
		Bundle: bundle,
		InputMask: &common.BundleMask{
			JwtSigningKeys: true,
		},
	})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle, uresp.Bundle)

	lresp, err = s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	assertBundlesEqual(s.T(), []*common.Bundle{bundle, bundle3}, lresp.Bundles)

	// update without mask
	uresp, err = s.shim.UpdateBundle(ctx, &datastore.UpdateBundleRequest{
		Bundle: bundle2,
	})
	s.Require().NoError(err)
	s.AssertProtoEqual(bundle2, uresp.Bundle)

	lresp, err = s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	assertBundlesEqual(s.T(), []*common.Bundle{bundle2, bundle3}, lresp.Bundles)

	// delete
	_, err = s.shim.DeleteBundle(ctx, &datastore.DeleteBundleRequest{
		TrustDomainId: bundle.TrustDomainId,
	})
	s.Require().NoError(err)
	//s.AssertProtoEqual(bundle2, dresp.Bundle)

	lresp, err = s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	s.Equal(1, len(lresp.Bundles))
	s.AssertProtoEqual(bundle3, lresp.Bundles[0])

	// delete (with denormalized id)
	_, err = s.shim.DeleteBundle(ctx, &datastore.DeleteBundleRequest{
		TrustDomainId: "spiffe://bAR",
	})
	s.Require().NoError(err)
	//s.AssertProtoEqual(bundle3, dresp.Bundle)

	lresp, err = s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{})
	s.Require().NoError(err)
	s.Empty(lresp.Bundles)

}

func (s *PluginSuite) TestListBundlesWithPagination() {
	bundle1 := bundleutil.BundleProtoFromRootCA("spiffe://bar", s.cert)
	_, err := s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle1,
	})
	s.Require().NoError(err)

	bundle2 := bundleutil.BundleProtoFromRootCA("spiffe://baz", s.cert)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle2,
	})
	s.Require().NoError(err)

	bundle3 := bundleutil.BundleProtoFromRootCA("spiffe://example.org", s.cert)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle3,
	})
	s.Require().NoError(err)

	bundle4 := bundleutil.BundleProtoFromRootCA("spiffe://foo", s.cacert)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle4,
	})
	s.Require().NoError(err)

	tests := []struct {
		name               string
		pagination         *datastore.Pagination
		byExpiresBefore    *wrapperspb.Int64Value
		expectedList       []*common.Bundle
		expectedPagination *datastore.Pagination
		expectedErr        string
	}{
		{
			name:         "no pagination",
			expectedList: []*common.Bundle{bundle1, bundle2, bundle3, bundle4},
		},
		{
			name: "page size bigger than items",
			pagination: &datastore.Pagination{
				PageSize: 5,
			},
			expectedList: []*common.Bundle{bundle1, bundle2, bundle3, bundle4},
			expectedPagination: &datastore.Pagination{
				Token:    "",
				PageSize: 5,
			},
		},
		{
			name: "pagination page size is zero",
			pagination: &datastore.Pagination{
				PageSize: 0,
			},
			expectedErr: "rpc error: code = InvalidArgument desc = cannot paginate with pagesize = 0",
		},
		{
			name: "bundles first page",
			pagination: &datastore.Pagination{
				Token:    "",
				PageSize: 2,
			},
			expectedList: []*common.Bundle{bundle1, bundle2},
			expectedPagination: &datastore.Pagination{
				Token:    fmt.Sprintf("B|%s", bundle2.TrustDomainId),
				PageSize: 2,
			},
		},
		{
			name: "bundles second page",
			pagination: &datastore.Pagination{
				Token:    fmt.Sprintf("B|%s", bundle2.TrustDomainId),
				PageSize: 2,
			},
			expectedList: []*common.Bundle{bundle3, bundle4},
			expectedPagination: &datastore.Pagination{
				Token:    fmt.Sprintf("B|%s", bundle4.TrustDomainId),
				PageSize: 2,
			},
		},
		{
			name:         "bundles third page",
			expectedList: []*common.Bundle{},
			pagination: &datastore.Pagination{
				Token:    fmt.Sprintf("B|%s", bundle4.TrustDomainId),
				PageSize: 2,
			},
			expectedPagination: &datastore.Pagination{
				Token:    "",
				PageSize: 2,
			},
		},
		{
			name:         "invalid token",
			expectedList: []*common.Bundle{},
			expectedErr:  "rpc error: code = InvalidArgument desc = could not parse token 'invalid token'",
			pagination: &datastore.Pagination{
				Token:    "invalid token",
				PageSize: 2,
			},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
			},
		},
	}
	for _, test := range tests {
		test := test
		s.T().Run(test.name, func(t *testing.T) {
			resp, err := s.shim.ListBundles(ctx, &datastore.ListBundlesRequest{
				Pagination: test.pagination,
			})
			if test.expectedErr != "" {
				require.EqualError(t, err, test.expectedErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)

			expectedResponse := &datastore.ListBundlesResponse{
				Bundles:    test.expectedList,
				Pagination: test.expectedPagination,
			}
			//s.T().Logf("response: %v", resp)
			//s.T().Logf("expected: %v", expectedResponse)
			spiretest.RequireProtoEqual(t, expectedResponse, resp)
		})
	}
}

func (s *PluginSuite) TestCountBundles() {
	// Count empty bundles
	resp, err := s.shim.CountBundles(ctx, &datastore.CountBundlesRequest{})
	s.Require().NoError(err)
	spiretest.RequireProtoEqual(s.T(), &datastore.CountBundlesResponse{Bundles: 0}, resp)

	// Create bundles
	bundle1 := bundleutil.BundleProtoFromRootCA("spiffe://example.org", s.cert)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle1,
	})
	s.Require().NoError(err)

	bundle2 := bundleutil.BundleProtoFromRootCA("spiffe://foo", s.cacert)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle2,
	})
	s.Require().NoError(err)

	bundle3 := bundleutil.BundleProtoFromRootCA("spiffe://bar", s.cert)
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundle3,
	})
	s.Require().NoError(err)

	// Count all
	resp, err = s.shim.CountBundles(ctx, &datastore.CountBundlesRequest{})
	s.Require().NoError(err)
	spiretest.RequireProtoEqual(s.T(), &datastore.CountBundlesResponse{Bundles: 3}, resp)
}

func (s *PluginSuite) TestSetBundle() {
	// create a couple of bundles for tests. the contents don't really matter
	// as long as they are for the same trust domain but have different contents.
	bundle := bundleutil.BundleProtoFromRootCA("spiffe://foo", s.cert)
	bundle2 := bundleutil.BundleProtoFromRootCA("spiffe://foo", s.cacert)

	// ensure the bundle does not exist (it shouldn't)
	s.Require().Nil(s.fetchBundle("spiffe://foo"))

	// set the bundle and make sure it is created
	_, err := s.shim.SetBundle(ctx, &datastore.SetBundleRequest{
		Bundle: bundle,
	})
	s.Require().NoError(err)
	s.RequireProtoEqual(bundle, s.fetchBundle("spiffe://foo"))

	// set the bundle and make sure it is updated
	_, err = s.shim.SetBundle(ctx, &datastore.SetBundleRequest{
		Bundle: bundle2,
	})
	s.Require().NoError(err)
	s.RequireProtoEqual(bundle2, s.fetchBundle("spiffe://foo"))
}

func (s *PluginSuite) TestBundlePrune() {
	// Setup
	// Create new bundle with two cert (one valid and one expired)
	bundle := bundleutil.BundleProtoFromRootCAs("spiffe://foo", []*x509.Certificate{s.cert, s.cacert})

	// Add two JWT signing keys (one valid and one expired)
	expiredKeyTime, err := time.Parse(time.RFC3339, _expiredNotAfterString)
	s.Require().NoError(err)

	nonExpiredKeyTime, err := time.Parse(time.RFC3339, _validNotAfterString)
	s.Require().NoError(err)

	// middleTime is a point between the two certs expiration time
	middleTime, err := time.Parse(time.RFC3339, _middleTimeString)
	s.Require().NoError(err)

	bundle.JwtSigningKeys = []*common.PublicKey{
		{NotAfter: expiredKeyTime.Unix()},
		{NotAfter: nonExpiredKeyTime.Unix()},
	}

	// Store bundle in datastore
	_, err = s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{Bundle: bundle})
	s.Require().NoError(err)

	// Prune
	// prune non existent bundle should not return error, no bundle to prune
	expiration := time.Now().Unix()
	presp, err := s.shim.PruneBundle(ctx, &datastore.PruneBundleRequest{
		TrustDomainId: "spiffe://notexistent",
		ExpiresBefore: expiration,
	})
	s.NoError(err)
	s.AssertProtoEqual(presp, &datastore.PruneBundleResponse{})

	// prune fails if internal prune bundle fails. For instance, if all certs are expired
	expiration = time.Now().Unix()
	presp, err = s.shim.PruneBundle(ctx, &datastore.PruneBundleRequest{
		TrustDomainId: bundle.TrustDomainId,
		ExpiresBefore: expiration,
	})
	s.AssertGRPCStatus(err, codes.Unknown, "prune failed: would prune all certificates")
	s.Nil(presp)

	// prune should remove expired certs
	presp, err = s.shim.PruneBundle(ctx, &datastore.PruneBundleRequest{
		TrustDomainId: bundle.TrustDomainId,
		ExpiresBefore: middleTime.Unix(),
	})
	s.NoError(err)
	s.NotNil(presp)
	s.True(presp.BundleChanged)

	// Fetch and verify pruned bundle is the expected
	expectedPrunedBundle := bundleutil.BundleProtoFromRootCAs("spiffe://foo", []*x509.Certificate{s.cert})
	expectedPrunedBundle.JwtSigningKeys = []*common.PublicKey{{NotAfter: nonExpiredKeyTime.Unix()}}
	fresp, err := s.shim.FetchBundle(ctx, &datastore.FetchBundleRequest{TrustDomainId: "spiffe://foo"})
	s.Require().NoError(err)
	s.AssertProtoEqual(expectedPrunedBundle, fresp.Bundle)
}

func (s *PluginSuite) fetchBundle(trustDomainID string) *common.Bundle {
	resp, err := s.shim.FetchBundle(ctx, &datastore.FetchBundleRequest{
		TrustDomainId: trustDomainID,
	})
	s.Require().NoError(err)
	return resp.Bundle
}

func (s *PluginSuite) createBundle(trustDomainID string) {
	_, err := s.shim.CreateBundle(ctx, &datastore.CreateBundleRequest{
		Bundle: bundleutil.BundleProtoFromRootCA(trustDomainID, s.cert),
	})
	s.Require().NoError(err)
}

// assertBundlesEqual asserts that the two bundle lists are equal independent
// of ordering.
func assertBundlesEqual(t *testing.T, expected, actual []*common.Bundle) {
	if !assert.Equal(t, len(expected), len(actual)) {
		return
	}

	es := map[string]*common.Bundle{}
	as := map[string]*common.Bundle{}

	for _, e := range expected {
		es[e.TrustDomainId] = e
	}

	for _, a := range actual {
		as[a.TrustDomainId] = a
	}

	for id, a := range as {
		e, ok := es[id]
		if assert.True(t, ok, "bundle %q was unexpected", id) {
			spiretest.AssertProtoEqual(t, e, a)
			delete(es, id)
		}
	}

	for id := range es {
		assert.Failf(t, "bundle %q was expected but not found", id)
	}
}
