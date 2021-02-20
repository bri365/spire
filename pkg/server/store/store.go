// Package store implements a new datastore layer with the proposed new store interface.
//
// Items are stored with a key string and marshalled protobuf data as the value
// Keys are formatted as <item key><delim><unique identifier>
// e.g. "B|spiffie://example.com" for a bundle
// or "E|5fee2e4a-1fe3-4bf3-b4f0-55eaf268c12a" for a registration entry
//
// Indices are stored with a Key string and no Value. Index keys are formatted as
// <item key><I><delim><index field identifier>[...<delim><field value>]<delim><unique item identifier>
// e.g. "NI|EXP|1611907252|spiffie://example.com/clusterA/nodeN" for an attested node expiry
//
// TODO add primary key field number/identifier to item keys?
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
	cfg   *Configuration
	clock clock.Clock
}

// Configuration represents store wide config, supplied by the plugin
type Configuration struct {
	EnableBundleCache           bool `hcl:"enable_bundle_cache" json:"enable_bundle_cache"`
	EnableBundleCacheInvalidate bool `hcl:"enable_bundle_cache_invalidate" json:"enable_bundle_cache_invalidate"`
	EnableBundleCacheUpdate     bool `hcl:"enable_bundle_cache_update" json:"enable_bundle_cache_update"`

	EnableEntryCache           bool `hcl:"enable_entry_cache" json:"enable_entry_cache"`
	EnableEntryCacheInvalidate bool `hcl:"enable_entry_cache_invalidate" json:"enable_entry_cache_invalidate"`
	EnableEntryCacheUpdate     bool `hcl:"enable_entry_cache_update" json:"enable_entry_cache_update"`

	EnableNodeCache           bool `hcl:"enable_node_cache" json:"enable_node_cache"`
	EnableNodeCacheFetch      bool `hcl:"enable_node_cache_fetch" json:"enable_node_cache_fetch"`
	EnableNodeCacheInvalidate bool `hcl:"enable_node_cache_invalidate" json:"enable_node_cache_invalidate"`
	EnableNodeCacheUpdate     bool `hcl:"enable_node_cache_update" json:"enable_node_cache_update"`

	EnableTokenCache           bool `hcl:"enable_token_cache" json:"enable_token_cache"`
	EnableTokenCacheInvalidate bool `hcl:"enable_token_cache_invalidate" json:"enable_token_cache_invalidate"`
	EnableTokenCacheUpdate     bool `hcl:"enable_token_cache_update" json:"enable_token_cache_update"`

	HeartbeatInterval int
}

// Key creation constants for items and indices
// NOTE: changing any of these constants will require migrating store data
const (
	// NOTE: prefer a printable character not part of a conforming URI
	Delim = "|"

	// Object identifiers
	IndexKeyID = "I"

	BundleKeyID = "B"
	EntryKeyID  = "E"
	HeartbeatID = "H"
	NodeKeyID   = "N"
	TokenKeyID  = "T"

	TxKeyID = "X"

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

	// End of transaction marker enables watchers to identify operations in a transaction

	// Key creation and comparison values
	BundlePrefix    = fmt.Sprintf("%s%s", BundleKeyID, Delim)
	EntryPrefix     = fmt.Sprintf("%s%s", EntryKeyID, Delim)
	HeartbeatPrefix = fmt.Sprintf("%s%s", HeartbeatID, Delim)
	NodePrefix      = fmt.Sprintf("%s%s", NodeKeyID, Delim)
	TokenPrefix     = fmt.Sprintf("%s%s", TokenKeyID, Delim)
	TxPrefix        = fmt.Sprintf("%s%s", TxKeyID, Delim)

	AllBundles = fmt.Sprintf("%s%s", BundleKeyID, Delend)
	AllEntries = fmt.Sprintf("%s%s", EntryKeyID, Delend)
	AllNodes   = fmt.Sprintf("%s%s", NodeKeyID, Delend)
	AllTokens  = fmt.Sprintf("%s%s", TokenKeyID, Delend)

	bundleIndex = fmt.Sprintf("%s%s%s", BundleKeyID, IndexKeyID, Delim)
	entryIndex  = fmt.Sprintf("%s%s%s", EntryKeyID, IndexKeyID, Delim)
	nodeIndex   = fmt.Sprintf("%s%s%s", NodeKeyID, IndexKeyID, Delim)
	tokenIndex  = fmt.Sprintf("%s%s%s", TokenKeyID, IndexKeyID, Delim)

	nodeExpPrefix = fmt.Sprintf("%s%s%s", nodeIndex, EXP, Delim)
	nodeExpAll    = fmt.Sprintf("%s%s%s", nodeIndex, EXP, Delend)
)

// New returns an initialized store.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger,
	cfg *Configuration, etcd *clientv3.Client) (*Shim, error) {
	store := &Shim{
		DataStore: ds,
		Store:     st,
		Etcd:      etcd,
		Log:       logger,
		c:         NewCache(cfg),
		cfg:       cfg,
		clock:     clock.New(),
	}
	if err := store.Initialize(); err != nil {
		return nil, err
	}
	return store, nil
}

// Close shuts down the heartbeat and watcher channels.
func (s *Shim) Close() {
	// Close heartbeat and watchers
}
