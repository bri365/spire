// Package store implements a datastore shim
package store

import (
	"github.com/spiffe/spire/pkg/server/plugin/datastore"
	"github.com/spiffe/spire/pkg/server/plugin/store"
)

// Shim defines a datastore shim to allow the new Store API
// to coexist with the existing datastore API during prototyping.
type Shim struct {
	datastore.DataStore
	st store.Store
}

// New returns an initialized storage shim.
func New(ds datastore.DataStore, st store.Store) *Shim {
	return &Shim{DataStore: ds, st: st}
}
