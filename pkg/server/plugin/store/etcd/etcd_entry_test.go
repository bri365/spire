package etcd

import (
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
)

func (s *PluginSuite) TestCountRegistrationEntries() {
	// Count empty registration entries
	resp, err := s.shim.CountRegistrationEntries(ctx, &datastore.CountRegistrationEntriesRequest{})
	s.Require().NoError(err)
	spiretest.RequireProtoEqual(s.T(), &datastore.CountRegistrationEntriesResponse{Entries: 0}, resp)

	// Create attested nodes
	entry := &common.RegistrationEntry{
		ParentId:  "spiffe://example.org/agent",
		SpiffeId:  "spiffe://example.org/foo",
		Selectors: []*common.Selector{{Type: "a", Value: "1"}},
	}
	_, err = s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: entry})
	s.Require().NoError(err)

	entry2 := &common.RegistrationEntry{
		ParentId:  "spiffe://example.org/agent",
		SpiffeId:  "spiffe://example.org/bar",
		Selectors: []*common.Selector{{Type: "a", Value: "2"}},
	}
	_, err = s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: entry2})
	s.Require().NoError(err)

	// Count all
	resp, err = s.shim.CountRegistrationEntries(ctx, &datastore.CountRegistrationEntriesRequest{})
	s.Require().NoError(err)
	spiretest.RequireProtoEqual(s.T(), &datastore.CountRegistrationEntriesResponse{Entries: 2}, resp)
}
