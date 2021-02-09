// Package store implements a new datastore layer with the proposed new store interface.
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

	"github.com/andres-erbsen/clock"
	"github.com/hashicorp/go-hclog"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
)

// Shim defines a temporary datastore shim to allow the new Store API
// to coexist with the existing datastore API during prototyping.
type Shim struct {
	datastore.DataStore
	store.Store

	cache Cache
	cfg   *Configuration
	log   hclog.Logger
}

// Configuration represents store wide config, supplied by the plugin
type Configuration struct {
	DisableBundleRegCache bool
	DisableNodeCache      bool
	DisableTokenCache     bool
	HeartbeatInterval     int
	WriteResponseDelay    int
}

// Key creation constants for items and indices
// NOTE: changing any of these constants will require migrating store data
const (
	// NOTE: prefer a printable character not part of a conforming URI
	delim = "|"

	// Object identifiers
	// NOTE: these could be an enum if readability is not important for debugability
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
	BAN = "BAN" // Banned
	EXP = "EXP" // Expiry
	FED = "FED" // Federation
	PID = "PID" // ParentId
	SID = "SID" // SpiffeId
	TVI = "TVI" // Type-Value-ID
)

var (
	storeLoaded = false

	// NOTE: this is one bit greater than the delimiter - it is used to end
	// a range to get all key values for a given prefix.
	delend = string(delim[0] + 1)

	// Key creation and comparison values
	bundlePrefix = fmt.Sprintf("%s%s", bundleKeyID, delim)
	entryPrefix  = fmt.Sprintf("%s%s", entryKeyID, delim)
	nodePrefix   = fmt.Sprintf("%s%s", nodeKeyID, delim)
	selPrefix    = fmt.Sprintf("%s%s", selKeyID, delim)
	tokenPrefix  = fmt.Sprintf("%s%s", tokenKeyID, delim)

	allBundles   = fmt.Sprintf("%s%s", bundleKeyID, delend)
	allEntries   = fmt.Sprintf("%s%s", entryKeyID, delend)
	allNodes     = fmt.Sprintf("%s%s", nodeKeyID, delend)
	allSelectors = fmt.Sprintf("%s%s", selKeyID, delend)

	nodeExpPrefix = fmt.Sprintf("%s%s%s%s", indexKeyID, nodePrefix, EXP, delim)
	nodeExpAll    = fmt.Sprintf("%s%s%s%s", indexKeyID, nodePrefix, EXP, delend)
)

// New returns an initialized store.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger, cfg *Configuration) (*Shim, error) {
	store := &Shim{DataStore: ds, Store: st, log: logger}
	store.cache = NewCache(cfg, clock.New(), logger)
	if err := store.Initialize(); err != nil {
		return nil, err
	}
	return store, nil
}
