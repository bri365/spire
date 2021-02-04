package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/spiffe/spire/pkg/common/util"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/test/spiretest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

func (s *PluginSuite) TestCreateRegistrationEntry() {
	var validRegistrationEntries []*common.RegistrationEntry
	s.getTestDataFromJSONFile(filepath.Join("testdata", "valid_registration_entries.json"), &validRegistrationEntries)

	for _, validRegistrationEntry := range validRegistrationEntries {
		resp, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: validRegistrationEntry})
		s.Require().NoError(err)
		s.NotNil(resp)
		s.Require().NotNil(resp.Entry)
		s.NotEmpty(resp.Entry.EntryId)
		resp.Entry.EntryId = ""
		s.RequireProtoEqual(resp.Entry, validRegistrationEntry)
	}
}

func (s *PluginSuite) TestCreateInvalidRegistrationEntry() {
	var invalidRegistrationEntries []*common.RegistrationEntry
	s.getTestDataFromJSONFile(filepath.Join("testdata", "invalid_registration_entries.json"), &invalidRegistrationEntries)

	for _, invalidRegistrationEntry := range invalidRegistrationEntries {
		createRegistrationEntryResponse, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: invalidRegistrationEntry})
		s.Require().Error(err)
		s.Require().Nil(createRegistrationEntryResponse)
	}

	// TODO: Check that no entries have been created
}

func (s *PluginSuite) TestFetchRegistrationEntry() {
	registeredEntry := &common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId: "SpiffeId",
		ParentId: "ParentId",
		Ttl:      1,
		DnsNames: []string{
			"abcd.efg",
			"somehost",
		},
	}

	createRegistrationEntryResponse, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: registeredEntry})
	s.Require().NoError(err)
	s.Require().NotNil(createRegistrationEntryResponse)
	createdEntry := createRegistrationEntryResponse.Entry

	fetchRegistrationEntryResponse, err := s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: createdEntry.EntryId})
	s.Require().NoError(err)
	s.Require().NotNil(fetchRegistrationEntryResponse)
	s.RequireProtoEqual(createdEntry, fetchRegistrationEntryResponse.Entry)
}

func (s *PluginSuite) TestPruneRegistrationEntries() {
	now := time.Now().Unix()
	registeredEntry := &common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId:    "SpiffeId",
		ParentId:    "ParentId",
		Ttl:         1,
		EntryExpiry: now,
	}

	createRegistrationEntryResponse, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: registeredEntry})
	s.Require().NoError(err)
	s.Require().NotNil(createRegistrationEntryResponse)
	createdEntry := createRegistrationEntryResponse.Entry

	// Ensure we don't prune valid entries, wind clock back 10s
	_, err = s.shim.PruneRegistrationEntries(ctx, &datastore.PruneRegistrationEntriesRequest{
		ExpiresBefore: now - 10,
	})
	s.Require().NoError(err)

	fetchRegistrationEntryResponse, err := s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: createdEntry.EntryId})
	s.Require().NoError(err)
	s.Require().NotNil(fetchRegistrationEntryResponse)
	s.RequireProtoEqual(createdEntry, fetchRegistrationEntryResponse.Entry)
	// s.Equal(createdEntry, fetchRegistrationEntryResponse.Entry)

	// Ensure we don't prune on the exact ExpiresBefore
	_, err = s.shim.PruneRegistrationEntries(ctx, &datastore.PruneRegistrationEntriesRequest{
		ExpiresBefore: now,
	})
	s.Require().NoError(err)

	fetchRegistrationEntryResponse, err = s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: createdEntry.EntryId})
	s.Require().NoError(err)
	s.Require().NotNil(fetchRegistrationEntryResponse)
	s.RequireProtoEqual(createdEntry, fetchRegistrationEntryResponse.Entry)
	// s.Equal(createdEntry, fetchRegistrationEntryResponse.Entry)

	// Ensure we prune old entries
	_, err = s.shim.PruneRegistrationEntries(ctx, &datastore.PruneRegistrationEntriesRequest{
		ExpiresBefore: now + 10,
	})
	s.Require().NoError(err)

	fetchRegistrationEntryResponse, err = s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: createdEntry.EntryId})
	s.Require().NoError(err)
	s.Nil(fetchRegistrationEntryResponse.Entry)
}

func (s *PluginSuite) TestFetchInexistentRegistrationEntry() {
	fetchRegistrationEntryResponse, err := s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: "INEXISTENT"})
	s.Require().NoError(err)
	s.Require().Nil(fetchRegistrationEntryResponse.Entry)
}

func (s *PluginSuite) TestListRegistrationEntries() {
	entry1 := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId: "spiffe://example.org/foo",
		ParentId: "spiffe://example.org/bar",
		Ttl:      1,
		Admin:    true,
	})

	entry2 := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type3", Value: "Value3"},
			{Type: "Type4", Value: "Value4"},
			{Type: "Type5", Value: "Value5"},
		},
		SpiffeId:   "spiffe://example.org/baz",
		ParentId:   "spiffe://example.org/bat",
		Ttl:        2,
		Downstream: true,
	})

	resp, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{})
	s.Require().NoError(err)
	s.Require().NotNil(resp)

	expectedResponse := &datastore.ListRegistrationEntriesResponse{
		Entries: []*common.RegistrationEntry{entry2, entry1},
	}
	util.SortRegistrationEntries(expectedResponse.Entries)
	util.SortRegistrationEntries(resp.Entries)
	s.RequireProtoEqual(expectedResponse, resp)
}

func (s *PluginSuite) TestListRegistrationEntriesWithPagination() {
	entry1 := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId: "spiffe://example.org/foo",
		ParentId: "spiffe://example.org/bar",
		Ttl:      1,
	})

	entry2 := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type3", Value: "Value3"},
			{Type: "Type4", Value: "Value4"},
			{Type: "Type5", Value: "Value5"},
		},
		SpiffeId: "spiffe://example.org/baz",
		ParentId: "spiffe://example.org/bat",
		Ttl:      2,
	})

	entry3 := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId: "spiffe://example.org/tez",
		ParentId: "spiffe://example.org/taz",
		Ttl:      2,
	})

	selectors := []*common.Selector{
		{Type: "Type1", Value: "Value1"},
		{Type: "Type2", Value: "Value2"},
		{Type: "Type3", Value: "Value3"},
	}

	ids := []string{"1000", entry1.EntryId, entry2.EntryId, entry3.EntryId}

	tests := []ListRegistrationReq{
		{
			name: "pagination_without_token",
			pagination: &datastore.Pagination{
				PageSize: 2,
			},
			expectedList: []*common.RegistrationEntry{entry2, entry1},
			expectedPagination: &datastore.Pagination{
				Token:    "2",
				PageSize: 2,
			},
		},
		{
			name: "pagination_not_null_but_page_size_is_zero",
			pagination: &datastore.Pagination{
				Token:    "0",
				PageSize: 0,
			},
			err: "rpc error: code = InvalidArgument desc = cannot paginate with pagesize = 0",
		},
		{
			name: "get_all_entries_first_page",
			pagination: &datastore.Pagination{
				Token:    "0",
				PageSize: 2,
			},
			expectedList: []*common.RegistrationEntry{entry2, entry1},
			expectedPagination: &datastore.Pagination{
				Token:    "2",
				PageSize: 2,
			},
		},
		{
			name: "get_all_entries_second_page",
			pagination: &datastore.Pagination{
				Token:    "2",
				PageSize: 2,
			},
			expectedList: []*common.RegistrationEntry{entry3},
			expectedPagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 2,
			},
		},
		{
			name: "get_all_entries_third_page_no_results",
			pagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 2,
			},
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
			},
		},
		{
			name: "get_entries_by_selector_get_only_page_first_page",
			pagination: &datastore.Pagination{
				Token:    "0",
				PageSize: 2,
			},
			selectors:    selectors,
			expectedList: []*common.RegistrationEntry{entry1, entry3},
			expectedPagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 2,
			},
		},
		{
			name: "get_entries_by_selector_get_only_page_second_page_no_results",
			pagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 2,
			},
			selectors: selectors,
			expectedPagination: &datastore.Pagination{
				PageSize: 2,
			},
		},
		{
			name: "get_entries_by_selector_first_page",
			pagination: &datastore.Pagination{
				Token:    "0",
				PageSize: 1,
			},
			selectors:    selectors,
			expectedList: []*common.RegistrationEntry{entry1},
			expectedPagination: &datastore.Pagination{
				Token:    "1",
				PageSize: 1,
			},
		},
		{
			name: "get_entries_by_selector_second_page",
			pagination: &datastore.Pagination{
				Token:    "1",
				PageSize: 1,
			},
			selectors:    selectors,
			expectedList: []*common.RegistrationEntry{entry3},
			expectedPagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 1,
			},
		},
		{
			name: "get_entries_by_selector_third_page_no_results",
			pagination: &datastore.Pagination{
				Token:    "3",
				PageSize: 1,
			},
			selectors: selectors,
			expectedPagination: &datastore.Pagination{
				PageSize: 1,
			},
		},
	}

	s.listRegistrationEntries(tests, ids)

	// with invalid token
	resp, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{
		Pagination: &datastore.Pagination{
			Token:    "invalid int",
			PageSize: 10,
		},
	})
	s.Require().Nil(resp)
	s.Require().Error(err, "could not parse token 'invalid int'")
}

func (s *PluginSuite) listRegistrationEntries(tests []ListRegistrationReq, ids []string) {
	for _, test := range tests {
		test := test
		s.T().Run(test.name, func(t *testing.T) {
			var bySelectors *datastore.BySelectors
			if test.selectors != nil {
				bySelectors = &datastore.BySelectors{
					Selectors: test.selectors,
					Match:     datastore.BySelectors_MATCH_EXACT,
				}
			}
			if test.pagination != nil && test.pagination.Token != "" {
				i, _ := strconv.ParseInt(test.pagination.Token, 10, 0)
				test.pagination.Token = fmt.Sprintf("E|%s", ids[i])
			}
			resp, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{
				BySelectors: bySelectors,
				Pagination:  test.pagination,
			})
			if test.err != "" {
				require.EqualError(t, err, test.err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			if test.expectedPagination != nil && test.expectedPagination.Token != "" {
				i, _ := strconv.ParseInt(test.expectedPagination.Token, 10, 0)
				test.expectedPagination.Token = fmt.Sprintf("E|%s", ids[i])
			}

			expectedResponse := &datastore.ListRegistrationEntriesResponse{
				Entries:    test.expectedList,
				Pagination: test.expectedPagination,
			}
			util.SortRegistrationEntries(expectedResponse.Entries)
			util.SortRegistrationEntries(resp.Entries)
			s.T().Log(fmt.Sprintf("expected: %v", expectedResponse.Entries))
			s.T().Log(fmt.Sprintf("received: %v", resp.Entries))
			spiretest.RequireProtoEqual(t, expectedResponse, resp)
		})
	}
}

func (s *PluginSuite) TestListRegistrationEntriesAgainstMultipleCriteria() {
	s.createBundle("spiffe://federates1.org")
	s.createBundle("spiffe://federates2.org")
	s.createBundle("spiffe://federates3.org")
	s.createBundle("spiffe://federates4.org")

	entry := s.createRegistrationEntry(&common.RegistrationEntry{
		ParentId: "spiffe://example.org/P1",
		SpiffeId: "spiffe://example.org/S1",
		Selectors: []*common.Selector{
			{Type: "T1", Value: "V1"},
		},
		FederatesWith: []string{
			"spiffe://federates1.org",
		},
	})

	// shares a parent ID
	s.createRegistrationEntry(&common.RegistrationEntry{
		ParentId: "spiffe://example.org/P1",
		SpiffeId: "spiffe://example.org/S2",
		Selectors: []*common.Selector{
			{Type: "T2", Value: "V2"},
		},
		FederatesWith: []string{
			"spiffe://federates2.org",
		},
	})

	// shares a spiffe ID
	s.createRegistrationEntry(&common.RegistrationEntry{
		ParentId: "spiffe://example.org/P3",
		SpiffeId: "spiffe://example.org/S1",
		Selectors: []*common.Selector{
			{Type: "T3", Value: "V3"},
		},
		FederatesWith: []string{
			"spiffe://federates3.org",
		},
	})

	// shares selectors
	s.createRegistrationEntry(&common.RegistrationEntry{
		ParentId: "spiffe://example.org/P4",
		SpiffeId: "spiffe://example.org/S4",
		Selectors: []*common.Selector{
			{Type: "T1", Value: "V1"},
		},
		FederatesWith: []string{
			"spiffe://federates4.org",
		},
	})

	// shares federates with
	s.createRegistrationEntry(&common.RegistrationEntry{
		ParentId: "spiffe://example.org/P5",
		SpiffeId: "spiffe://example.org/S5",
		Selectors: []*common.Selector{
			{Type: "T5", Value: "V5"},
		},
		FederatesWith: []string{
			"spiffe://federates1.org",
		},
	})

	resp, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{
		ByParentId: &wrapperspb.StringValue{
			Value: "spiffe://example.org/P1",
		},
		BySpiffeId: &wrapperspb.StringValue{
			Value: "spiffe://example.org/S1",
		},
		BySelectors: &datastore.BySelectors{
			Selectors: []*common.Selector{
				{Type: "T1", Value: "V1"},
			},
			Match: datastore.BySelectors_MATCH_EXACT,
		},
		ByFederatesWith: &datastore.ByFederatesWith{
			TrustDomains: []string{
				"spiffe://federates1.org",
			},
			Match: datastore.ByFederatesWith_MATCH_EXACT,
		},
	})

	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.RequireProtoListEqual([]*common.RegistrationEntry{entry}, resp.Entries)
}

func (s *PluginSuite) TestUpdateRegistrationEntry() {
	entry := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId: "spiffe://example.org/foo",
		ParentId: "spiffe://example.org/bar",
		Ttl:      1,
	})

	entry.Ttl = 2
	entry.Admin = true
	entry.Downstream = true

	updateRegistrationEntryResponse, err := s.shim.UpdateRegistrationEntry(ctx, &datastore.UpdateRegistrationEntryRequest{
		Entry: entry,
	})
	s.Require().NoError(err)
	s.Require().NotNil(updateRegistrationEntryResponse)

	fetchRegistrationEntryResponse, err := s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: entry.EntryId})
	s.Require().NoError(err)
	s.Require().NotNil(fetchRegistrationEntryResponse)
	s.Require().NotNil(fetchRegistrationEntryResponse.Entry)
	s.RequireProtoEqual(updateRegistrationEntryResponse.Entry, fetchRegistrationEntryResponse.Entry)

	entry.EntryId = "badid"
	_, err = s.shim.UpdateRegistrationEntry(ctx, &datastore.UpdateRegistrationEntryRequest{
		Entry: entry,
	})
	s.RequireGRPCStatus(err, codes.NotFound, _notFoundErrMsg)
}

func (s *PluginSuite) TestUpdateRegistrationEntryWithMask() {
	// There are 9 fields in a registration entry. Of these, 3 have some validation in the SQL
	// layer. In this test, we update each of the 9 fields and make sure update works, and also check
	// with the mask value false to make sure nothing changes. For the 3 fields that have validation
	// we try with good data, bad data, and with or without a mask (so 4 cases each.)

	// Note that most of the input validation is done in the API layer and has more extensive tests there.
	oldEntry := &common.RegistrationEntry{
		ParentId:      "spiffe://example.org/oldParentId",
		SpiffeId:      "spiffe://example.org/oldSpiffeId",
		Ttl:           1000,
		Selectors:     []*common.Selector{{Type: "Type1", Value: "Value1"}},
		FederatesWith: []string{"spiffe://dom1.org"},
		Admin:         false,
		EntryExpiry:   1000,
		DnsNames:      []string{"dns1"},
		Downstream:    false,
	}
	newEntry := &common.RegistrationEntry{
		ParentId:      "spiffe://example.org/newParentId",
		SpiffeId:      "spiffe://example.org/newSpiffeId",
		Ttl:           1001,
		Selectors:     []*common.Selector{{Type: "Type2", Value: "Value2"}},
		FederatesWith: []string{"spiffe://dom2.org"},
		Admin:         true,
		EntryExpiry:   1001,
		DnsNames:      []string{"dns2"},
		Downstream:    true,
	}
	badEntry := &common.RegistrationEntry{
		ParentId:      "not a good parent id",
		SpiffeId:      "",
		Ttl:           -1000,
		Selectors:     []*common.Selector{},
		FederatesWith: []string{"invalid federated bundle"},
		Admin:         false,
		EntryExpiry:   -2000,
		DnsNames:      []string{"this is a bad domain name "},
		Downstream:    false,
	}
	// Needed for the FederatesWith field to work
	s.createBundle("spiffe://dom1.org")
	s.createBundle("spiffe://dom2.org")
	for _, testcase := range []struct {
		name   string
		mask   *common.RegistrationEntryMask
		update func(*common.RegistrationEntry)
		result func(*common.RegistrationEntry)
		rev    int64
		err    error
	}{ /// SPIFFE ID FIELD -- this field is validated so we check with good and bad data
		{name: "Update Spiffe ID, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{SpiffeId: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.SpiffeId = newEntry.SpiffeId },
			result: func(e *common.RegistrationEntry) { e.SpiffeId = newEntry.SpiffeId }},
		{name: "Update Spiffe ID, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{SpiffeId: false},
			update: func(e *common.RegistrationEntry) { e.SpiffeId = newEntry.SpiffeId },
			result: func(e *common.RegistrationEntry) {}},
		{name: "Update Spiffe ID, Bad Data, Mask True",
			mask:   &common.RegistrationEntryMask{SpiffeId: true},
			update: func(e *common.RegistrationEntry) { e.SpiffeId = badEntry.SpiffeId },
			err:    errors.New("invalid registration entry: missing SPIFFE ID")},
		{name: "Update Spiffe ID, Bad Data, Mask False",
			mask:   &common.RegistrationEntryMask{SpiffeId: false},
			update: func(e *common.RegistrationEntry) { e.SpiffeId = badEntry.SpiffeId },
			result: func(e *common.RegistrationEntry) {}},
		/// PARENT ID FIELD -- This field isn't validated so we just check with good data
		{name: "Update Parent ID, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{ParentId: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.ParentId = newEntry.ParentId },
			result: func(e *common.RegistrationEntry) { e.ParentId = newEntry.ParentId }},
		{name: "Update Parent ID, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{ParentId: false},
			update: func(e *common.RegistrationEntry) { e.ParentId = newEntry.ParentId },
			result: func(e *common.RegistrationEntry) {}},
		/// TTL FIELD -- This field is validated so we check with good and bad data
		{name: "Update TTL, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{Ttl: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.Ttl = newEntry.Ttl },
			result: func(e *common.RegistrationEntry) { e.Ttl = newEntry.Ttl }},
		{name: "Update TTL, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{Ttl: false},
			update: func(e *common.RegistrationEntry) { e.Ttl = badEntry.Ttl },
			result: func(e *common.RegistrationEntry) {}},
		{name: "Update TTL, Bad Data, Mask True",
			mask:   &common.RegistrationEntryMask{Ttl: true},
			update: func(e *common.RegistrationEntry) { e.Ttl = badEntry.Ttl },
			err:    errors.New("invalid registration entry: TTL is not set")},
		{name: "Update TTL, Bad Data, Mask False",
			mask:   &common.RegistrationEntryMask{Ttl: false},
			update: func(e *common.RegistrationEntry) { e.Ttl = badEntry.Ttl },
			result: func(e *common.RegistrationEntry) {}},
		/// SELECTORS FIELD -- This field is validated so we check with good and bad data
		{name: "Update Selectors, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{Selectors: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.Selectors = newEntry.Selectors },
			result: func(e *common.RegistrationEntry) { e.Selectors = newEntry.Selectors }},
		{name: "Update Selectors, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{Selectors: false},
			update: func(e *common.RegistrationEntry) { e.Selectors = badEntry.Selectors },
			result: func(e *common.RegistrationEntry) {}},
		{name: "Update Selectors, Bad Data, Mask True",
			mask:   &common.RegistrationEntryMask{Selectors: false},
			update: func(e *common.RegistrationEntry) { e.Selectors = badEntry.Selectors },
			err:    errors.New("invalid registration entry: missing selector list")},
		{name: "Update Selectors, Bad Data, Mask False",
			mask:   &common.RegistrationEntryMask{Selectors: false},
			update: func(e *common.RegistrationEntry) { e.Selectors = badEntry.Selectors },
			result: func(e *common.RegistrationEntry) {}},
		/// FEDERATESWITH FIELD -- This field isn't validated so we just check with good data
		{name: "Update FederatesWith, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{FederatesWith: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.FederatesWith = newEntry.FederatesWith },
			result: func(e *common.RegistrationEntry) { e.FederatesWith = newEntry.FederatesWith }},
		{name: "Update FederatesWith Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{FederatesWith: false},
			update: func(e *common.RegistrationEntry) { e.FederatesWith = newEntry.FederatesWith },
			result: func(e *common.RegistrationEntry) {}},
		/// ADMIN FIELD -- This field isn't validated so we just check with good data
		{name: "Update Admin, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{Admin: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.Admin = newEntry.Admin },
			result: func(e *common.RegistrationEntry) { e.Admin = newEntry.Admin }},
		{name: "Update Admin, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{Admin: false},
			update: func(e *common.RegistrationEntry) { e.Admin = newEntry.Admin },
			result: func(e *common.RegistrationEntry) {}},
		/// ENTRYEXPIRY FIELD -- This field isn't validated so we just check with good data
		{name: "Update EntryExpiry, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{EntryExpiry: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.EntryExpiry = newEntry.EntryExpiry },
			result: func(e *common.RegistrationEntry) { e.EntryExpiry = newEntry.EntryExpiry }},
		{name: "Update EntryExpiry, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{EntryExpiry: false},
			update: func(e *common.RegistrationEntry) { e.EntryExpiry = newEntry.EntryExpiry },
			result: func(e *common.RegistrationEntry) {}},
		/// DNSNAMES FIELD -- This field isn't validated so we just check with good data
		{name: "Update DnsNames, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{DnsNames: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.DnsNames = newEntry.DnsNames },
			result: func(e *common.RegistrationEntry) { e.DnsNames = newEntry.DnsNames }},
		{name: "Update DnsNames, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{DnsNames: false},
			update: func(e *common.RegistrationEntry) { e.DnsNames = newEntry.DnsNames },
			result: func(e *common.RegistrationEntry) {}},
		/// DOWNSTREAM FIELD -- This field isn't validated so we just check with good data
		{name: "Update DnsNames, Good Data, Mask True",
			mask:   &common.RegistrationEntryMask{Downstream: true},
			rev:    1,
			update: func(e *common.RegistrationEntry) { e.Downstream = newEntry.Downstream },
			result: func(e *common.RegistrationEntry) { e.Downstream = newEntry.Downstream }},
		{name: "Update DnsNames, Good Data, Mask False",
			mask:   &common.RegistrationEntryMask{Downstream: false},
			update: func(e *common.RegistrationEntry) { e.Downstream = newEntry.Downstream },
			result: func(e *common.RegistrationEntry) {}},
		// This should update all fields
		{name: "Test With Nil Mask",
			mask:   nil,
			rev:    1,
			update: func(e *common.RegistrationEntry) { proto.Merge(e, oldEntry) },
			result: func(e *common.RegistrationEntry) {}},
	} {
		tt := testcase
		s.Run(tt.name, func() {
			s.T().Log(tt.name)
			s.T().Log(fmt.Sprintf("oldEntry ID %s", oldEntry.EntryId))
			s.deleteRegistrationEntry(oldEntry)
			oldEntry.EntryId = ""
			oldEntry.RevisionNumber = 0
			entry := s.createRegistrationEntry(oldEntry)
			id := entry.EntryId

			updateEntry := &common.RegistrationEntry{}
			tt.update(updateEntry)
			updateEntry.EntryId = id
			updateRegistrationEntryResponse, err := s.shim.UpdateRegistrationEntry(ctx, &datastore.UpdateRegistrationEntryRequest{
				Entry: updateEntry,
				Mask:  tt.mask,
			})

			if tt.err != nil {
				s.Require().Error(tt.err)
				return
			}

			s.Require().NoError(err)
			s.Require().NotNil(updateRegistrationEntryResponse)
			expectedResult := proto.Clone(oldEntry).(*common.RegistrationEntry)
			tt.result(expectedResult)
			expectedResult.EntryId = id
			// RevisionNumber only increments if something actually changed
			expectedResult.RevisionNumber = tt.rev
			s.RequireProtoEqual(expectedResult, updateRegistrationEntryResponse.Entry)

			// Fetch and check the results match expectations
			fetchRegistrationEntryResponse, err := s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{EntryId: id})
			s.Require().NoError(err)
			s.Require().NotNil(fetchRegistrationEntryResponse)
			s.Require().NotNil(fetchRegistrationEntryResponse.Entry)

			s.RequireProtoEqual(expectedResult, fetchRegistrationEntryResponse.Entry)
		})
	}
}

func (s *PluginSuite) TestDeleteRegistrationEntry() {
	// delete non-existing
	_, err := s.shim.DeleteRegistrationEntry(ctx, &datastore.DeleteRegistrationEntryRequest{EntryId: "badid"})
	s.RequireGRPCStatus(err, codes.NotFound, _notFoundErrMsg)

	entry1 := s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
			{Type: "Type2", Value: "Value2"},
			{Type: "Type3", Value: "Value3"},
		},
		SpiffeId: "spiffe://example.org/foo",
		ParentId: "spiffe://example.org/bar",
		Ttl:      1,
	})

	s.createRegistrationEntry(&common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type3", Value: "Value3"},
			{Type: "Type4", Value: "Value4"},
			{Type: "Type5", Value: "Value5"},
		},
		SpiffeId: "spiffe://example.org/baz",
		ParentId: "spiffe://example.org/bat",
		Ttl:      2,
	})

	// We have two registration entries
	entriesResp, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{})
	s.Require().NoError(err)
	s.Require().Len(entriesResp.Entries, 2)

	// Make sure we deleted the right one
	delRes, err := s.shim.DeleteRegistrationEntry(ctx, &datastore.DeleteRegistrationEntryRequest{EntryId: entry1.EntryId})
	s.Require().NoError(err)
	s.RequireProtoEqual(entry1, delRes.Entry)

	// Make sure we have now only one registration entry
	entriesResp, err = s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{})
	s.Require().NoError(err)
	s.Require().Len(entriesResp.Entries, 1)

	// Delete again must fails with Not Found
	delRes, err = s.shim.DeleteRegistrationEntry(ctx, &datastore.DeleteRegistrationEntryRequest{EntryId: entry1.EntryId})
	s.Require().EqualError(err, "rpc error: code = NotFound desc = store-etcd: record not found")
	s.Require().Nil(delRes)
}

func (s *PluginSuite) TestListParentIDEntries() {
	allEntries := make([]*common.RegistrationEntry, 0)
	s.getTestDataFromJSONFile(filepath.Join("testdata", "entries.json"), &allEntries)
	tests := []struct {
		name                string
		registrationEntries []*common.RegistrationEntry
		parentID            string
		expectedList        []*common.RegistrationEntry
	}{
		{

			name:                "test_parentID_found",
			registrationEntries: allEntries,
			parentID:            "spiffe://parent",
			expectedList:        allEntries[:2],
		},
		{
			name:                "test_parentID_notfound",
			registrationEntries: allEntries,
			parentID:            "spiffe://imnoparent",
			expectedList:        nil,
		},
	}
	for _, test := range tests {
		test := test
		s.T().Run(test.name, func(t *testing.T) {
			s.cleanStore()
			for _, entry := range test.registrationEntries {
				entry.EntryId = ""
				r, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: entry})
				require.NoError(t, err)
				require.NotNil(t, r)
				require.NotNil(t, r.Entry)
				entry.EntryId = r.Entry.EntryId
			}
			result, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{
				ByParentId: &wrapperspb.StringValue{
					Value: test.parentID,
				},
			})
			require.NoError(t, err)
			spiretest.RequireProtoListEqual(t, test.expectedList, result.Entries)
		})
	}
}

func (s *PluginSuite) TestListSelectorEntries() {
	allEntries := make([]*common.RegistrationEntry, 0)
	s.getTestDataFromJSONFile(filepath.Join("testdata", "entries.json"), &allEntries)
	tests := []struct {
		name                string
		registrationEntries []*common.RegistrationEntry
		selectors           []*common.Selector
		expectedList        []*common.RegistrationEntry
	}{
		{
			name:                "entries_by_selector_found",
			registrationEntries: allEntries,
			selectors: []*common.Selector{
				{Type: "a", Value: "1"},
				{Type: "b", Value: "2"},
				{Type: "c", Value: "3"},
			},
			expectedList: []*common.RegistrationEntry{allEntries[0]},
		},
		{
			name:                "entries_by_selector_not_found",
			registrationEntries: allEntries,
			selectors: []*common.Selector{
				{Type: "e", Value: "0"},
			},
			expectedList: nil,
		},
	}
	for _, test := range tests {
		test := test
		s.T().Run(test.name, func(t *testing.T) {
			s.cleanStore()
			for _, entry := range test.registrationEntries {
				entry.EntryId = ""
				r, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: entry})
				require.NoError(t, err)
				require.NotNil(t, r)
				require.NotNil(t, r.Entry)
				entry.EntryId = r.Entry.EntryId
			}
			result, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{
				BySelectors: &datastore.BySelectors{
					Selectors: test.selectors,
					Match:     datastore.BySelectors_MATCH_EXACT,
				},
			})
			require.NoError(t, err)
			spiretest.RequireProtoListEqual(t, test.expectedList, result.Entries)
		})
	}
}

// func (s *PluginSuite) TestListEntriesBySelectorSubset() {
// 	allEntries := make([]*common.RegistrationEntry, 0)
// 	s.getTestDataFromJSONFile(filepath.Join("testdata", "entries.json"), &allEntries)
// 	tests := []struct {
// 		name                string
// 		registrationEntries []*common.RegistrationEntry
// 		selectors           []*common.Selector
// 		expectedList        []*common.RegistrationEntry
// 	}{
// 		{
// 			name:                "test1",
// 			registrationEntries: allEntries,
// 			selectors: []*common.Selector{
// 				{Type: "a", Value: "1"},
// 				{Type: "b", Value: "2"},
// 				{Type: "c", Value: "3"},
// 			},
// 			expectedList: []*common.RegistrationEntry{
// 				allEntries[0],
// 				allEntries[1],
// 				allEntries[2],
// 			},
// 		},
// 		{
// 			name:                "test2",
// 			registrationEntries: allEntries,
// 			selectors: []*common.Selector{
// 				{Type: "d", Value: "4"},
// 			},
// 			expectedList: nil,
// 		},
// 	}
// 	for _, test := range tests {
// 		test := test
// 		s.T().Run(test.name, func(t *testing.T) {
// 			s.cleanStore()
// 			for _, entry := range test.registrationEntries {
// 				entry.EntryId = ""
// 				r, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{Entry: entry})
// 				require.NoError(t, err)
// 				require.NotNil(t, r)
// 				require.NotNil(t, r.Entry)
// 				entry.EntryId = r.Entry.EntryId
// 			}
// 			result, err := s.shim.ListRegistrationEntries(ctx, &datastore.ListRegistrationEntriesRequest{
// 				BySelectors: &datastore.BySelectors{
// 					Selectors: test.selectors,
// 					Match:     datastore.BySelectors_MATCH_SUBSET,
// 				},
// 			})
// 			require.NoError(t, err)
// 			util.SortRegistrationEntries(test.expectedList)
// 			util.SortRegistrationEntries(result.Entries)
// 			s.RequireProtoListEqual(test.expectedList, result.Entries)
// 		})
// 	}
// }

func (s *PluginSuite) TestRegistrationEntriesFederatesWithAgainstMissingBundle() {
	// cannot federate with a trust bundle that does not exist
	_, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{
		Entry: makeFederatedRegistrationEntry(),
	})
	s.RequireErrorContains(err, `unable to find federated bundle`)
}

func (s *PluginSuite) TestRegistrationEntriesFederatesWithSuccess() {
	// create two bundles but only federate with one. having a second bundle
	// has the side effect of asserting that only the code only associates
	// the entry with the exact bundle referenced during creation.
	s.createBundle("spiffe://otherdomain.org")
	s.createBundle("spiffe://otherdomain2.org")

	expected := s.createRegistrationEntry(makeFederatedRegistrationEntry())
	// fetch the entry and make sure the federated trust ids come back
	actual := s.fetchRegistrationEntry(expected.EntryId)
	s.RequireProtoEqual(expected, actual)
}

// func (s *PluginSuite) TestDeleteBundleRestrictedByRegistrationEntries() {
// 	// create the bundle and associated entry
// 	s.createBundle("spiffe://otherdomain.org")
// 	s.createRegistrationEntry(makeFederatedRegistrationEntry())

// 	// delete the bundle in RESTRICTED mode
// 	_, err := s.shim.DeleteBundle(context.Background(), &datastore.DeleteBundleRequest{
// 		TrustDomainId: "spiffe://otherdomain.org",
// 	})
// 	s.RequireErrorContains(err, "datastore-sql: cannot delete bundle; federated with 1 registration entries")
// }

// func (s *PluginSuite) TestDeleteBundleDeleteRegistrationEntries() {
// 	// create an unrelated registration entry to make sure the delete
// 	// operation only deletes associated registration entries.
// 	unrelated := s.createRegistrationEntry(&common.RegistrationEntry{
// 		SpiffeId:  "spiffe://example.org/foo",
// 		Selectors: []*common.Selector{{Type: "TYPE", Value: "VALUE"}},
// 	})

// 	// create the bundle and associated entry
// 	s.createBundle("spiffe://otherdomain.org")
// 	entry := s.createRegistrationEntry(makeFederatedRegistrationEntry())

// 	// delete the bundle in DELETE mode
// 	_, err := s.shim.DeleteBundle(context.Background(), &datastore.DeleteBundleRequest{
// 		TrustDomainId: "spiffe://otherdomain.org",
// 		Mode:          datastore.DeleteBundleRequest_DELETE,
// 	})
// 	s.Require().NoError(err)

// 	// verify that the registeration entry has been deleted
// 	resp, err := s.shim.FetchRegistrationEntry(context.Background(), &datastore.FetchRegistrationEntryRequest{
// 		EntryId: entry.EntryId,
// 	})
// 	s.Require().NoError(err)
// 	s.Require().Nil(resp.Entry)

// 	// make sure the unrelated entry still exists
// 	s.fetchRegistrationEntry(unrelated.EntryId)
// }

// func (s *PluginSuite) TestDeleteBundleDissociateRegistrationEntries() {
// 	// create the bundle and associated entry
// 	s.createBundle("spiffe://otherdomain.org")
// 	entry := s.createRegistrationEntry(makeFederatedRegistrationEntry())

// 	// delete the bundle in DISSOCIATE mode
// 	_, err := s.shim.DeleteBundle(context.Background(), &datastore.DeleteBundleRequest{
// 		TrustDomainId: "spiffe://otherdomain.org",
// 		Mode:          datastore.DeleteBundleRequest_DISSOCIATE,
// 	})
// 	s.Require().NoError(err)

// 	// make sure the entry still exists, albeit without an associated bundle
// 	entry = s.fetchRegistrationEntry(entry.EntryId)
// 	s.Require().Empty(entry.FederatesWith)
// }

// ////////////////////////////////////////////////////////////////////////
// //
// // Support functions
// //
// ////////////////////////////////////////////////////////////////////////
func (s *PluginSuite) getTestDataFromJSONFile(filePath string, jsonValue interface{}) {
	entriesJSON, err := ioutil.ReadFile(filePath)
	s.Require().NoError(err)

	err = json.Unmarshal(entriesJSON, &jsonValue)
	s.Require().NoError(err)
}

func (s *PluginSuite) createRegistrationEntry(entry *common.RegistrationEntry) *common.RegistrationEntry {
	resp, err := s.shim.CreateRegistrationEntry(ctx, &datastore.CreateRegistrationEntryRequest{
		Entry: entry,
	})
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.Require().NotNil(resp.Entry)
	return resp.Entry
}

func (s *PluginSuite) cleanStore() {
	// delete all keys from the store
	res, err := s.st.Get(ctx, &store.GetRequest{
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

	return
}

func (s *PluginSuite) deleteRegistrationEntry(entry *common.RegistrationEntry) {
	s.shim.DeleteRegistrationEntry(ctx, &datastore.DeleteRegistrationEntryRequest{
		EntryId: entry.EntryId,
	})
	return
}

func (s *PluginSuite) fetchRegistrationEntry(entryID string) *common.RegistrationEntry {
	resp, err := s.shim.FetchRegistrationEntry(ctx, &datastore.FetchRegistrationEntryRequest{
		EntryId: entryID,
	})
	s.Require().NoError(err)
	s.Require().NotNil(resp)
	s.Require().NotNil(resp.Entry)
	return resp.Entry
}

func makeFederatedRegistrationEntry() *common.RegistrationEntry {
	return &common.RegistrationEntry{
		Selectors: []*common.Selector{
			{Type: "Type1", Value: "Value1"},
		},
		SpiffeId:      "spiffe://example.org/foo",
		FederatesWith: []string{"spiffe://otherdomain.org"},
	}
}
