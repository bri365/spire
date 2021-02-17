// Package store implements a new datastore layer with the proposed new store interface.
//
// Items are stored with a key string and the marshalled protobuf data as the value
// Keys are formatted as <item key><delim><unique identifier>
// e.g. "B|spiffie://example.com" for a bundle
// or "E|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a" for a registration entry
//
// Indices are stored with a Key string and no Value. Index keys are formatted as
// <item key><I><delim><index field identifier>[...<delim><field value>]<delim><unique item identifier>
// e.g. "NI|EXP|1611907252|spiffie://example.com/clusterA/nodeN" for an attested node expiry
//
package store

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
)

// Shim defines a temporary datastore shim to allow the new Store API
// to coexist with the existing datastore API during prototyping.
type Shim struct {
	datastore.DataStore
	store.Store

	log hclog.Logger
}

// Key creation constants for items and indices
// NOTE: changing any of these constants will require migrating store data
const (
	// NOTE: prefer a printable character not part of a conforming URI for debugability
	Delim = "|"

	// Object identifiers
	indexKeyID = "I"

	BundleKeyID = "B"
	EntryKeyID  = "E"
	heartbeatID = "H"
	NodeKeyID   = "N"
	selKeyID    = "S"
	tokenKeyID  = "T"
	txKeyID     = "X"

	// Index field identifiers
	ADT = "ADT" // AttestationDataType
	CNA = "CNA" // CertNotAfter
	BAN = "BAN" // Banned
	EXP = "EXP" // Expiry
	FED = "FED" // Federation
	PID = "PID" // ParentId
	SID = "SID" // SpiffeId
	TVI = "TVI" // Type-Value-ID
)

// Key creation values
var (
	// NOTE: this is one bit greater than the delimiter - it is used to end
	// a range to get all key values for a given prefix.
	Delend = string(Delim[0] + 1)

	// Key creation and comparison values
	BundlePrefix    = fmt.Sprintf("%s%s", BundleKeyID, Delim)
	EntryPrefix     = fmt.Sprintf("%s%s", EntryKeyID, Delim)
	HeartbeatPrefix = fmt.Sprintf("%s%s", heartbeatID, Delim)
	NodePrefix      = fmt.Sprintf("%s%s", NodeKeyID, Delim)
	tokenPrefix     = fmt.Sprintf("%s%s", tokenKeyID, Delim)
	TxPrefix        = fmt.Sprintf("%s%s", txKeyID, Delim)

	AllBundles   = fmt.Sprintf("%s%s", BundleKeyID, Delend)
	AllEntries   = fmt.Sprintf("%s%s", EntryKeyID, Delend)
	AllNodes     = fmt.Sprintf("%s%s", NodeKeyID, Delend)
	allSelectors = fmt.Sprintf("%s%s", selKeyID, Delend)
	allTokens    = fmt.Sprintf("%s%s", tokenKeyID, Delend)

	bundleIndex = fmt.Sprintf("%s%s%s", BundleKeyID, indexKeyID, Delim)
	entryIndex  = fmt.Sprintf("%s%s%s", EntryKeyID, indexKeyID, Delim)
	nodeIndex   = fmt.Sprintf("%s%s%s", NodeKeyID, indexKeyID, Delim)
	tokenIndex  = fmt.Sprintf("%s%s%s", tokenKeyID, indexKeyID, Delim)

	nodeExpPrefix = fmt.Sprintf("%s%s%s", nodeIndex, EXP, Delim)
	nodeExpAll    = fmt.Sprintf("%s%s%s", nodeIndex, EXP, Delend)
)

// New returns an initialized store.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger) *Shim {
	return &Shim{DataStore: ds, Store: st, log: logger}
}
