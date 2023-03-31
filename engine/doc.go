// Package engine provides a reference implementation of the provider.Interface in order to
// advertise the availability of a list of multihashes to indexer nodes such as "storetheindex".
// See: https://github.com/ipni/storetheindex
//
// The advertisements are published as a chan of diffs that signal the list of multihashes that are
// added or removed represented as an IPLD DAG.
// Walking the chain of advertisements would then provide the latest state of the total multihashes
// provided by the engine.
// The list of multihashes are paginated as a collection of interlinked chunks.
// For the complete advertisement IPLD schema, see:
//   - https://github.com/ipni/go-libipni/blob/main/ingest/schema/schema.ipldsch
//
// The engine internally uses "go-libipni/dagsync" to sync the IPLD DAG of advertisements.
// See: https://github.com/ipni/go-libipni/tree/main/dagsync
package engine
