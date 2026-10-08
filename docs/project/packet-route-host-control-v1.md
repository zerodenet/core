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
- `direct_packet_host_wintun_v1`: Windows can open an existing dedicated
  host Wintun adapter. This is separate from Unix descriptor adoption.

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
Windows does not consume Unix descriptors. With
`direct_packet_host_wintun_v1`, select the explicit existing-adapter backend:

```json
{
  "runtime": {
    "network": {
      "mtu": 1400,
      "direct_packet_device": {
        "backend": "wintun",
        "interface": "ZeroHostL3",
        "router_addresses": ["10.64.0.1", "fd64::1"]
      }
    }
  }
}
```

The Windows host creates/configures this dedicated Wintun adapter, retains
its adapter handle and supplies exclusive packet-I/O ownership to the elevated
core. Zero opens it by alias without a creation fallback, verifies the supplied
router addresses and MTU against existing interface facts, and starts only its
own packet-exchange session. Missing adapters, unready/unassigned addresses,
foreign binding backends and an excessive MTU fail clearly. A descriptor value
in `fd` is rejected for this backend. Omitted `backend` preserves legacy descriptor configuration.
Wintun ingress aliases are compared ASCII case-insensitively, including the implicit
`ZeroTun` ingress name. Host routes, forwarding, NAT and firewall remain host
policy; opening this backend does not configure them.

Candidate preparation does not start a packet reader. Device activation starts
the bounded reader; writes confirm driver acceptance, not a queued write.
Ring backpressure waits without busy polling or counting a retry as a discard.
Shutdown closes the return queue, wakes and joins the reader, and releases
only Zero's session. The host-owned adapter/configuration remain intact.
An adapter that cannot admit a new session is rejected during preparation;
the committed device is retained. For such an adapter, first remove the binding
and confirm its old session has stopped before applying a changed binding to
the same adapter; seamless replacement on an exclusive session is not promised.
Mobile host PacketSink adoption is not yet implemented.

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

The 2026-10-07 descriptor verification record remains historical evidence:
[verification record](packet-route-host-verification-20261007.md). Windows
existing-adapter qualification is an explicit privileged CI step; compilation
or the default ignored test result does not establish live Wintun acceptance.
Real host forwarding, external outages, long duration operation and production
qualification remain separate acceptance. No production network configuration
is changed by the backend.

2026-10-08 local checks: workspace check, Windows `zero-tun --tests`
cross-compilation, formatting and integration layout checks passed. The extra
Windows Clippy run with warnings denied is blocked by existing
`too_many_arguments` and `unnecessary_mut_passed` in `route/leak/windows.rs`,
`needless_lifetimes` in `route/windows.rs`, and `field_reassign_with_default`
in `route/windows/tests.rs`. Those modules are unchanged by this extension.
Workspace all-target/all-feature Clippy with warnings denied passed, including
the corrected test fixture. The new configuration target passed all 119 cases.
The complete workspace/all-feature gate ran with the current TUN retained from
2026-10-08 03:07:18 UTC to 04:54:20 UTC (6422 seconds), ending with exit 101:
146 test-result records including doctests, 2394 passed, 8 failed and 155 ignored.
One new configuration fixture initially enabled DNS hijacking without configuring
DNS; it was corrected and the entire configuration target passed on rerun.
The other runtime sources were unchanged during qualification. Six existing
proxy tests that resolve `localhost` through the system resolver failed:
the host currently returns `198.18.0.44` instead of a loopback address, matching
the route and connection assertion failures. The unchanged HTTP/2 origin test
also fails with a connection reset on a separate rerun; it uses the same system
resolver for `https://localhost`, so host DNS interference is the likely cause,
pending isolated-environment confirmation. No Windows privileged execution
result or all-green workspace qualification is claimed by these local checks.

Local logs are retained under `/Volumes/tool/tmp/zero-packet-platform-20261008/`:
`workspace-tests.log`, `config-regression.log`, `http2-localhost-recheck.log`,
`workspace-clippy-final.log`, `windows-tun-final-check.log` and
`windows-clippy.log`. Ignored external/privileged cases remain separate acceptance.

## Local acceptance checklist

Use a build containing this change and query capabilities first. The currently
installed core is not replaced by source edits or compilation.

1. With the existing WireGuard endpoint config, exercise company DNS and TCP,
   personal TCP/UDP and external Echo to the endpoint's configured local IP.
   Query endpoints, traffic scopes and PacketRoute leases during active traffic;
   Flow activity and Packet leases are distinct observations.
2. Close one active PacketRoute with its actual core instance and route ID.
   Confirm that lease disappears, other traffic continues, and the endpoint
   remains enabled. Stop/restart the endpoint separately and check its intent,
   lifecycle and device generation.
3. For Direct PacketSink, have the host provide a dedicated device and a working
   forwarding/return path. Force Packet on the selected Direct route and verify
   bidirectional payload, TTL/MTU errors and real Inner counters. No binding
   must produce a clear forced-Packet error; ordinary Flow remains available.
4. On Windows, provide `wintun.dll`, elevate the core, retain the host-created
   adapter and configure its addresses/MTU before applying `backend=wintun`.
   Verify missing adapter, unassigned router address, excessive MTU and ingress
   alias collision are rejected. Stop/reload Zero and check that host addressing
   and routes remain intact and its packet session can be reopened.

The Windows privileged CI fixture covers opening, read-only configuration,
deferred reader activation, rejection of a competing candidate while the owner
continues receiving, host packet receive, driver write acceptance and
reader/session release. It does not establish remote forwarding reachability;
that requires the host's deployment topology and an external receiver.
