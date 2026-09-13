# Reverse / Rvs

Reference: Xray-core v26.3.27 (`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`).

The portal uses an explicit outbound with `protocol.type: vless_reverse` and no
server/port. A VLESS inbound user's `reverse_tag` references that outbound. Each
portal belongs to one authenticated user. Ordinary CONNECT cannot impersonate
Rvs by choosing a special target domain: admission checks command 4 and the
configured user role before starting the control connection.

The remote bridge uses a VLESS outbound's `reverse_tag` as a virtual inbound tag.
Route rules can match that tag and choose Zero native direct/proxy/reject actions.
No bridge-side listening socket or second management API is required.

The protocol owns command framing, MUX control protobuf, source/local address
metadata, worker demand, portal selection, drain notification and carrier state.
The runtime owns service execution, application routing, accounting, cancellation
and reload. Service preparation is transactional with configuration reconciliation;
failed preparation retains the active service. Successful reload retires old
portal pools and replaces bridge tasks. Control admission enforces principal
cancellation/device policy, and authenticated carriers discard fallback recording.

Worker demand is checked every two seconds. The bridge creates a worker when no
active worker exists or integer mean active connections exceeds 16. The portal
sends its first heartbeat and then one every ten seconds, announces DRAIN after
more than 256 total logical connections, and closes drained idle carriers using
the reference's sixteen-second idle observation. A separate twenty-four-hour
control timeout bounds abandoned state.

TCP uses the existing MUX wire implementation, including 8 KiB frame splitting
and upload close. Reverse UDP keeps one bounded response subscription per session,
so bursts and unsolicited replies do not depend on new requests. An endpoint-free
portal is admitted dynamically rather than cached as an unhealthy dial endpoint.

Status: native and pinned-reference bidirectional TCP/UDP, burst and unsolicited
UDP responses, and forced carrier reconnect tests pass. Reload, complete endpoint
metadata propagation, cancellation and final workspace gates remain open.
