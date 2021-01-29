// Package store implements a datastore shim
package store

import (
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

var storeLoaded = false

// New returns an initialized storage shim.
func New(ds datastore.DataStore, st store.Store, logger hclog.Logger) *Shim {
	return &Shim{DataStore: ds, Store: st, log: logger}
}
