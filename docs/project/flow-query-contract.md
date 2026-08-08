# Flow query contract

Flow lifecycle events, `flow.snapshot`, `active_flows`, `recent_flows`, and the
single `flow` query expose the same canonical `FlowRecord` semantics.

The active and completed query snapshots retain their flattened legacy fields
for compatibility and add an optional `record` field. New consumers should use
`record` for connection details. It contains:

- the flow revision and state;
- source address, source port, and available process metadata;
- target, route decision, selected path, and relay chain;
- user-direction and transport-boundary traffic counters;
- upload/download throughput and its explicit sampling time;
- complete timing and, for completed flows, the terminal result.

The engine builds query and event records with the same projection functions.
Replacing event-derived state with an authoritative query response therefore
does not discard fields after startup or reconnect.

Older servers may omit `record`, and older clients may ignore it. Consumers
that support the canonical shape should fall back to the flattened fields only
when `record` is absent.
