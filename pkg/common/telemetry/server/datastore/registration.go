package datastore

import (
	"github.com/spiffe/spire/pkg/common/telemetry"
)

// Call Counters (timing and success metrics)
// Allows adding labels in-code

// StartCountRegistrationCall return metric
// for server's datastore, on counting registrations.
func StartCountRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.Count)
}

// StartCreateRegistrationCall return metric
// for server's datastore, on creating a registration.
func StartCreateRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.Create)
}

// StartDeleteRegistrationCall return metric
// for server's datastore, on deleting a registration.
func StartDeleteRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.Delete)
}

// StartFetchAuthorizedEntriesCall return metric for server datastore on fetching authorized entries.
func StartFetchAuthorizedEntriesCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.FetchAuthorized)
}

// StartFetchRegistrationCall return metric
// for server's datastore, on creating a registration.
func StartFetchRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.Fetch)
}

// StartListRegistrationCall return metric
// for server's datastore, on listing registrations.
func StartListRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.List)
}

// StartPruneRegistrationCall return metric
// for server's datastore, on pruning registrations.
func StartPruneRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.Prune)
}

// StartUpdateRegistrationCall return metric
// for server's datastore, on updating a registration.
func StartUpdateRegistrationCall(m telemetry.Metrics) *telemetry.CallCounter {
	return telemetry.StartCall(m, telemetry.Datastore, telemetry.RegistrationEntry, telemetry.Update)
}

// End Call Counters
