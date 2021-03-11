package store

import (
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/spire/pkg/server/api"
	"github.com/spiffe/spire/proto/spire/common"
	"github.com/spiffe/spire/proto/spire/types"
)

type aliasEntry struct {
	id    spiffeID
	entry *types.Entry
}

// aliadID represents a unique identifier comprising an entry spiffeID and an entry ID
type aliasID struct {
	sid spiffeID
	eid string
}

// EntriesByParent represents a map by parent spiffeID containing maps of entries by EntryID
type EntriesByParent map[spiffeID]map[string]*types.Entry

// NodeAliases represents a map by agent SPIFFE ID containing maps of entries by aliasID
type NodeAliases map[spiffeID]map[aliasID]*types.Entry

// Selector is a key-value attribute of a node or workload.
type Selector struct {
	// Type is the type of the selector.
	Type string
	// Value is the value of the selector.
	Value string
}

type selectorSet map[Selector]struct{}

type spiffeID struct {
	// TrustDomain is the trust domain of the SPIFFE ID.
	TrustDomain string
	// Path is the path of the SPIFFE ID.
	Path string
}

// Build queries the store cache for all registration entries and agent selectors and builds
// an in-memory representation of the data that can be used for efficient lookups.
func (s *Shim) Build() (EntriesByParent, NodeAliases, error) {
	type aliasInfo struct {
		aliasEntry
		selectors selectorSet
	}
	bysel := make(map[Selector][]aliasInfo)

	entries := make(map[spiffeID][]*types.Entry)
	s.c.mu.RLock()
	for id, entry := range s.c.entries {
		e, err := api.RegistrationEntryToProto(entry)
		if err != nil {
			s.Log.Error("invalid entry", "id", id)
			continue
		}
		parentID := spiffeIDFromProto(e.ParentId)
		if parentID.Path == "/spire/server" {
			alias := aliasInfo{
				aliasEntry: aliasEntry{
					id:    spiffeIDFromProto(e.SpiffeId),
					entry: e,
				},
				selectors: selectorSetFromProto(e.Selectors),
			}
			for selector := range alias.selectors {
				bysel[selector] = append(bysel[selector], alias)
			}
			continue
		}
		entries[parentID] = append(entries[parentID], e)
	}

	aliasSeen := map[string]bool{}
	aliases := make(map[spiffeID][]aliasEntry)
	for _, node := range s.c.nodes {
		id, err := spiffeid.FromString(node.SpiffeId)
		if err != nil {
			s.Log.Error("invalid agent SPIFFE ID", "id", node.SpiffeId)
			continue
		}
		agentID := spiffeIDFromID(id)
		agentSelectors := selectorSetFromCommon(node.Selectors)
		for s := range agentSelectors {
			for _, alias := range bysel[s] {
				if aliasSeen[alias.entry.Id] {
					continue
				}
				aliasSeen[alias.entry.Id] = true
				if isSubset(alias.selectors, agentSelectors) {
					aliases[agentID] = append(aliases[agentID], alias.aliasEntry)
				}
			}
		}
	}

	entriesByParent := map[spiffeID]map[string]*types.Entry{}
	for id, es := range entries {
		entryMap := map[string]*types.Entry{}
		for _, e := range es {
			entryMap[e.Id] = e
		}
		entriesByParent[id] = entryMap
	}

	nodeAliases := map[spiffeID]map[aliasID]*types.Entry{}
	for id, ae := range aliases {
		aliasEntries := map[aliasID]*types.Entry{}
		for _, e := range ae {
			aliasEntries[aliasID{sid: id, eid: e.entry.Id}] = e.entry
		}
		nodeAliases[id] = aliasEntries
	}

	s.c.mu.RUnlock()

	return entriesByParent, nodeAliases, nil
}

func isSubset(sub, whole selectorSet) bool {
	if len(sub) > len(whole) {
		return false
	}
	for s := range sub {
		if _, ok := whole[s]; !ok {
			return false
		}
	}
	return true
}

func selectorSetFromCommon(selectors []*common.Selector) selectorSet {
	set := make(selectorSet, len(selectors))
	for _, selector := range selectors {
		set[Selector{Type: selector.Type, Value: selector.Value}] = struct{}{}
	}
	return set
}

func selectorSetFromProto(selectors []*types.Selector) selectorSet {
	set := make(selectorSet, len(selectors))
	for _, selector := range selectors {
		set[Selector{Type: selector.Type, Value: selector.Value}] = struct{}{}
	}
	return set
}

func spiffeIDFromID(id spiffeid.ID) spiffeID {
	return spiffeID{
		TrustDomain: id.TrustDomain().String(),
		Path:        id.Path(),
	}
}

func spiffeIDFromProto(id *types.SPIFFEID) spiffeID {
	return spiffeID{
		TrustDomain: id.TrustDomain,
		Path:        id.Path,
	}
}
