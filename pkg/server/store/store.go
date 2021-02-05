// Package store implements a datastore shim with the proposed new store interface.
//
// Items are stored with a key string and the marshalled protobuf data as the value
// Keys are formatted as <item key><delim><unique identifier>
// e.g. "B|spiffie://example.com" for a bundle
// or "E|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a" for a registration entry
//
// Indices are stored with a Key string and no Value. Index keys are formatted as
// <I><item key><delim><index field identifier>[...<delim><field value>]<delim><unique item identifier>
// e.g. "IN|EXP|1611907252|spiffie://example.com/clusterA/nodeN" for an attested node expiry
//
package store

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
)

// Shim defines a datastore shim to allow the new Store API
// to coexist with the existing datastore API during prototyping.
type Shim struct {
	datastore.DataStore
	store.Store

	log hclog.Logger
}

// Key creation constants for items and indices
const (
	// Key constructs - changing these will require migrating existing stored data

	// NOTE: prefer a printable character not part of a conforming URI for delimiter
	delim = "|"

	// Object identifiers
	// NOTE: these could be an enum if readability is not valued for debugability
	indexKeyID = "I"

	bundleKeyID = "B"
	entryKeyID  = "E"
	nodeKeyID   = "N"
	selKeyID    = "S"
	tokenKeyID  = "T"

	// Index field identifiers
	// NOTE: these could theoretically be reflected from protobufs but that
	// feels like overkill and would be less readable for debugging.
	ADT = "ADT" // AttestationDataType
	CNA = "CNA" // CertNotAfter
	BAN = "BAN" // Banned (CertSerialNumber empty or not)
	EXP = "EXP" // Expiry
	FED = "FED" // Federation
	PID = "PID" // ParentId
	SID = "SID" // SpiffeId
	TVI = "TVI" // Type-Value-ID
)

var (
	storeLoaded = false

	// NOTE: this is one bit greater than delimiter - it is used in range end to get
	// all key values after the delimiter in the range key.
	delend = string(delim[0] + 1)

	// Key creation and comparison values
	bundlePrefix  = fmt.Sprintf("%s%s", bundleKeyID, delim)
	entryPrefix   = fmt.Sprintf("%s%s", entryKeyID, delim)
	nodePrefix    = fmt.Sprintf("%s%s", nodeKeyID, delim)
	nodeExpPrefix = fmt.Sprintf("%s%s%s%s", indexKeyID, nodePrefix, EXP, delim)
	nodeExpAll    = fmt.Sprintf("%s%s%s%s", indexKeyID, nodePrefix, EXP, delend)
	selPrefix     = fmt.Sprintf("%s%s", selKeyID, delim)
	allBundles    = fmt.Sprintf("%s%s", bundleKeyID, delend)
	allEntries    = fmt.Sprintf("%s%s", entryKeyID, delend)
	allNodes      = fmt.Sprintf("%s%s", nodeKeyID, delend)
	allSelectors  = fmt.Sprintf("%s%s", selKeyID, delend)
)

// New returns an initialized storage shim.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger) *Shim {
	return &Shim{DataStore: ds, Store: st, log: logger}
}

// removeString removes the given string from an array, if present
func removeString(a []string, s string) []string {
	for i, v := range a {
		if v == s {
			a[i] = a[len(a)-1]
			return a[:len(a)-1]
		}
	}
	return a
}
