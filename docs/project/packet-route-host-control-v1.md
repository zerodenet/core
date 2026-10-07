# Packet route and host L3 control v1

## Ownership and scope

Engine owns resource generations, recovery facts and the executed PacketRoute
catalog. Neutral runtime pins own cancellation, traffic leases and bounded
return delivery. Protocols own authentication, peer state and packet framing.
Host TUN implementations own descriptor validation and packet I/O.

Native and translated Packet conversations register actual leases. Flow
conversion and legacy Direct Echo keep their existing Flow/echo lifetimes;
they are not represented as independently manageable PacketRoute leases.
A route ID is unique within a core instance and is never reused.

## Capability discovery

Check `capabilities.features`, rather than product version:

- `network_endpoint_device_incarnation_v1`: endpoint generation changes when
  actual observed device components are replaced, including same-ID rebuilds.
- `network_endpoint_network_recovery_v1`: existing endpoint queries/events
  include `recovery` facts for physical egress reconciliation.
- `packet_route_management_v1`: native/translated path queries and close.
- `direct_packet_host_descriptor_v1`: compiled Linux/macOS host descriptor
  backend. A dedicated device binding is still required to execute Packet.

These are extensions to existing Query/Command/Event transports and permissions.
Connector may filter and deliver `packet_route.closed` and
`endpoint.state_changed`; it is not required for local query/control.

## Query examples

```json
{"packet_routes":{"offset":0,"limit":100,"endpoint_id":"endpoint:company"}}
```

Optional filters: `endpoint_id`, `inbound_tag`, `outbound_tag`. The result has
`core_instance_id`, current `config_revision`, `observed_at_unix_ms`,
`routes`, filtered `total`, and `next_offset`. Pagination reflects live state;
concurrent admission/expiry can change later pages. Clients periodically query
again after gaps; they do not calculate authoritative activity counts.

```json
{"packet_route":{"route_id":"INSTANCE:packet:12"}}
```

Each fact contains core identity, registration config revision, route identity,
inbound/outbound tags, related resource IDs/generations, source/destination IP,
IP protocol, translation flag, start time and `active`/`closing` state. These
are execution roles, not RX/TX directions or byte totals. Reuse traffic scope
queries for counters. Registry storage is bounded to 4096 active path leases.

## Acknowledged close

```json
{
  "method":"packet_routes.close",
  "params":{
    "route_id":"INSTANCE:packet:12",
    "expected_core_instance_id":"INSTANCE",
    "expected_config_revision":4
  }
}
```

Requires Admin. Core identity is mandatory; config revision is optional and
checks the current configuration. The route ID identifies a specific lease,
so it cannot accidentally close a later route or rebuilt resource.

Success confirms owner lease release and return-pump termination; the result
contains `closed:true` and a final route snapshot with state `closed`.
The normal query no longer returns that route. A structured
`packet_route.closed` event contains `payload.route` and the normal event
instance/revision/sequence envelope. Query recovery remains authoritative.

Close does not change route policy, endpoint intent, configuration, health,
other leases or Flow usage/quota totals. It blocks this pinned conversation
until its original idle expiry (600 seconds); new conversations can still use
the endpoint. Already accepted protocol packets may be in flight; closing a
lease does not reset a shared protocol engine. Queued native packets whose
return channel is closed are discarded before device handoff.

Errors use existing envelopes: `conflict` for stale instance/revision,
`not_found` for expired/deleted leases, `permission_denied` without Admin,
and `internal` if the five-second release acknowledgement cannot be confirmed.
After acknowledgement loss or timeout, query the route and endpoint. A retry
against an already released ID returns `not_found`, never closes another path.
A concurrent retry while closing returns `conflict`; it does not start another
cancellation waiter. Disconnection after dispatch does not cancel execution.
If acknowledgement times out, eventual release still publishes the close event.

## Endpoint rebuild and recovery

Existing endpoint snapshots and `endpoint.state_changed` events include optional:

```json
{
  "recovery":{
    "network_generation":7,
    "phase":"retrying",
    "observed_at_unix_ms":1790000000000,
    "retry_after_ms":1000,
    "error":"endpoint resolution unavailable"
  }
}
```

Phases are `preparing`, `retrying`, `recovered`, `superseded`. Recovery uses the
existing serialized orchestration and bounded exponential retry (1–30 seconds).
An obsolete prepared topology never publishes devices. Failed preparation
keeps committed device ownership. `recovered` confirms device publication,
not peer handshake or remote business reachability. Peer health remains a
separate observation.

Generation is a real device incarnation, not config revision, intent revision,
network generation or statistics epoch. Policy-only changes and failed
preparations do not invent a new generation. Actual replacement updates start
time/generation even with unchanged endpoint ID; retained device components
and direction contraction preserve generation.

## Direct host PacketSink

Explicit configuration:

```json
{
  "runtime":{
    "network":{
      "mtu":1500,
      "direct_packet_device":{"fd":9,"interface":"utun12","router_addresses":["10.64.0.1","fd64::1"]}
    }
  }
}
```

The embedding host/launcher must supply an existing, nonblocking, dedicated
TUN descriptor >=3, inherited by the core, with exclusive packet I/O and the
specified interface. Supply the actual host-owned router addresses (one per
provided IP family) for local TTL/MTU error sources. Missing-family errors are
silently discarded, never emitted with a fabricated source. Zero duplicates and validates it; it never creates or
configures an interface, enables forwarding, changes routes, adds NAT or
firewall rules. The original descriptor remains host-owned. Descriptors are
process-local; the launcher must establish the binding for each new process.
A normal file/socket or mismatched interface is rejected. The ingress TUN
must be a different device. Linux requires raw-IP TUN with NO_PI and no VNET_HDR;
macOS reuses utun's existing packet-header normalization.

Without this binding, Direct retains its existing Stream/Datagram/Echo
capabilities. With a live binding, the capability graph can choose a native
Packet path inside the already selected Direct target; forced Flow continues
to use Flow. Forced Packet fails clearly if no executable backend exists.
Windows/mobile descriptor adoption is not implemented and is declared
unavailable; a future platform adapter can implement the same neutral boundary.

Direct preserves source IP, handles forwarding TTL/MTU/fragmentation through
zero-stack and returns packets through the existing bounded ownership map.
Deployment must ensure host forwarding, source permissions and return routes
reach the dedicated TUN; sending into it alone does not prove end-to-end
communication. Candidate devices stay inactive until commit, avoiding competing
readers during preparation/rollback. Replaced devices stop before activation.

Only actual device writes count TX. Correlated device returns count role RX.
When host returns require fragment reassembly, exact role RX fragment bytes/counts
are unavailable: forwarding continues and the existing observer marks those RX
metrics unavailable, rather than presenting reassembled totals as complete.
This backend supplies Inner observations; it does not invent protocol Outer
bytes or claim complete host/system loss counters.

## Acceptance status

Kernel implementation and local regressions are complete for this scope.
The completed full gate retained seven DNS/TUN environment failures after
contract corrections and explicit-host reruns; it is not a green production
qualification. See [verification record](packet-route-host-verification-20261007.md).
Real host forwarding, external outages, long duration operation and platform CI
remain separate acceptance. No production network configuration was changed.
