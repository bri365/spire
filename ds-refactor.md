### Objectives
This document defines a proposal for updating the SPIRE datastore to a simpler pluggable solution, capable of supporting both SQL and KV backend stores. The primary objectives for this change are to:

1. Satisfy requests for more storage options
1. Simplify installation and operation, especially in container environments
1. Streamline plugin development and support
1. Scale from a few to hundreds of thousands of agents
1. Strengthen SPIRE resiliency solutions
1. Stage SPIRE changes across releases in a way that minimizes impact and risk to running systems

### Background

The current datastore design dropped support for plugin extensibility, and only supports sql (SQLite, MySQL, and PostgreSQL), with dependencies on gorm. The rapid evolution of the datastore interface made ongoing plugin support impractical. An investigation last year demonstrated the efficacy of a simpler interface for both SQL and KV stores.

The problems identified last year are still largely relevant:

- Implementation complexity
- Interface churn
- SQL-specific challenges 

The SPIRE server datastore is responsible for reliable persistence of agent nodes, selectors, registration entries, bundles, and join tokens. Bundles are associated with multiple registrations, nodes have multiple Selectors, registrations have multiple bundles, DNS names, and selectors.

Datastore operations currently include the following (not all objects support all operations):

- Create, Delete, Update, Append, Prune
- Fetch, Count, List
- Get, Set

Queries across several fields are currently supported for attested nodes and registration entries (see ListAttestedNodesRequest and ListRegistrationEntriesRequest protobuf messages). The proposed solution preserves, and in some cases enhances, flexible query capabilities.

High availability support has been added since the last investigation, adding cross server coherency concerns that must be addressed in proposed solutions.

###Proposal

This proposal aims to address the above objectives and problems in a phased approach with the goal of minimal disruption throughout the transition. Key elements include:

1. Leverage the current datastore caching concepts for read requests wherever possible
1. Introduce a simplified backend store plugin interface for item-independent operations
1. Add plugins for KV database(s), starting with etcd
1. Rewrite existing SQL plugin(s)

Leveraging the current datastore cache concept reduces the need for expensive crawling queries of the backend store by serving them from SPIRE server memory while persisting them externally. Most data are safely cacheable; agent authorization caching will be explored at a later time. Change notifications and periodic full refreshes will be employed to ensure server cache consistency and stale data invalidation.

A simplified pluggable store interface will be created, consisting of Put, Get, Count, and Delete operations for one or more entries. The interface will also provide operations to Begin, Commit, and Abort transactions containing multiple primitive operations for cases where read-modify-write operations are required (e.g. prune bundle) or objects reference other objects (e.g. Registration Entries and Selectors or Bundles). The interface may also define a Watch operation to report new or updated items for distributed real-time cache updates.

The initial KV store reference design will support Etcd and serve as the standard for additional KV store plugins. SQL store(s) will be refactored to the new simplified interface.

##Phasing

In the interest of minimizing disruption to production customers, offering clear value and migration paths,  and keeping changes to manageable sizes, the following phases are considered:

1. Add KV plugin support
- Move current datastore implementation from a plugin to a regular module, retaining the current interface, and SQL backend.
- Create a new “store” plugin with a simplified interface for KV backend(s) only. SQL backends continue to use the existing code in the non-pluggable module. KV backends supported by new code paths in datastore module using the new store interface.
- Create a new etcd KV store plugin with the new store interface.
2. Add SQL plugin support
- Create new SQL plugin(s) with the new store interface
- New SQL plugins exist as alternative to existing non-pluggable SQL store
3. Add Watch support for real-time cache updates
4. Retire legacy SQL support

Store API

The simplest store API passes keys and values rather than entry types and identifiers. This approach is chosen for simplicity of plugin development. Helper functions will be offered as needed to extract entry, ID, and index information from keys.

Put (single)
- key []byte
- value []byte

Get (single or range)
- key []byte
- prefix bool

Delete (single or range)
- key []byte
- prefix bool

Count (range)
- key []byte

Begin

Commit
- id

Abort
- id

Watch
- keys []byte[]





