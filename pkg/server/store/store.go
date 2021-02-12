// Package store implements a new datastore layer with the proposed new store interface.
//
// Items are stored with a key string and the marshalled protobuf data as the value
// Keys are formatted as <item key><delim><unique identifier>
// e.g. "B|spiffie://example.com" for a bundle
// or "E|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a" for a registration entry
//
// Indices are stored with a Key string and no Value. Index keys are formatted as
// <I><item key><delim><index field identifier>[...<delim><field value>]<delim><unique item identifier>
// e.g. "NI|EXP|1611907252|spiffie://example.com/clusterA/nodeN" for an attested node expiry
//
package store

import (
	"fmt"

	"github.com/andres-erbsen/clock"
	"github.com/hashicorp/go-hclog"
	"github.com/roguesoftware/etcd/clientv3"
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
)

// Shim defines a temporary datastore shim to allow the new Store API
// to coexist with the existing datastore API during prototyping.
type Shim struct {
	datastore.DataStore
	store.Store

	// TODO - hack to workaround catalog non-plugin client/server issues
	Etcd *clientv3.Client

	Log hclog.Logger

	c     Cache
	clock clock.Clock
	cfg   *Configuration
}

// Configuration represents store wide config, supplied by the plugin
type Configuration struct {
	DisableBundleCache bool
	DisableEntryCache  bool
	DisableNodeCache   bool
	DisableTokenCache  bool
	EnableEotMarkers   bool
	HeartbeatInterval  int
	WriteResponseDelay int
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
	heartbeatID = "H"
	nodeKeyID   = "N"
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
	storeLoaded = false

	// NOTE: this is one bit greater than the delimiter - it is used to end
	// a range to get all key values for a given prefix.
	delend = string(delim[0] + 1)

	// End of transaction marker enables watchers to identify operations in a transaction

	// Key creation and comparison values
	bundlePrefix    = fmt.Sprintf("%s%s", bundleKeyID, delim)
	entryPrefix     = fmt.Sprintf("%s%s", entryKeyID, delim)
	heartbeatPrefix = fmt.Sprintf("%s%s", heartbeatID, delim)
	nodePrefix      = fmt.Sprintf("%s%s", nodeKeyID, delim)
	tokenPrefix     = fmt.Sprintf("%s%s", tokenKeyID, delim)
	TxPrefix        = fmt.Sprintf("%s%s", txKeyID, delim)

	allBundles   = fmt.Sprintf("%s%s", bundleKeyID, delend)
	allEntries   = fmt.Sprintf("%s%s", entryKeyID, delend)
	allNodes     = fmt.Sprintf("%s%s", nodeKeyID, delend)
	allSelectors = fmt.Sprintf("%s%s", selKeyID, delend)
	allTokens    = fmt.Sprintf("%s%s", tokenKeyID, delend)

	bundleIndex = fmt.Sprintf("%s%s%s", bundleKeyID, indexKeyID, delim)
	entryIndex  = fmt.Sprintf("%s%s%s", entryKeyID, indexKeyID, delim)
	nodeIndex   = fmt.Sprintf("%s%s%s", nodeKeyID, indexKeyID, delim)
	tokenIndex  = fmt.Sprintf("%s%s%s", tokenKeyID, indexKeyID, delim)

	nodeExpPrefix = fmt.Sprintf("%s%s%s", nodeIndex, EXP, delim)
	nodeExpAll    = fmt.Sprintf("%s%s%s", nodeIndex, EXP, delend)
)

// New returns an initialized store.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger,
	cfg *Configuration, etcd *clientv3.Client) (*Shim, error) {
	store := &Shim{DataStore: ds, Store: st, Log: logger, cfg: cfg, clock: clock.New()}
	store.c = NewCache(cfg)
	store.Etcd = etcd
	if err := store.Initialize(); err != nil {
		return nil, err
	}
	return store, nil
}

// Close shuts down the heartbeat and watcher channels.
func (s *Shim) Close() {
	// Close heartbeat and watchers
}
