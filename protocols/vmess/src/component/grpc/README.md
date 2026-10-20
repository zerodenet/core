# Explicit per-carrier VMess gRPC library

`Options::from_options_refs_at(..., NonZeroUsize, &actual_runtime)` binds the
adapter's actual carrier-admission state. It declares a real BackgroundCarrier,
an optional explicitly bounded File grant, and N actual scoped tasks for N
pending/live/retiring physical carriers. Each connect creates a fresh real H2
Client and one logical stream, with the original keepalive and read/write
futures polled inside that one registered driver. No GrpcPool or client cache is
created; legacy Client multiplexing APIs and production selection stay intact.

This first library admits one complete transport/credential/budget recipe per
actual Runtime. Complete equality and a checked opaque recipe nonce prevent
cold identity aliasing. All ordinary legacy TCP/UDP/relay gRPC transport entries
use the same real Runtime state before their own I/O. Legacy exposure is sticky
negative evidence, never a close receipt. A native claim refuses it; legacy
openers refuse a claimed state. Unsupported old clients cannot be adopted,
reset, or declared drained by Drop, AbortHandle, or a zero user count. A second
profile or owner generation requires future confirmed replacement work.

Build remains cold. Start claims that exact state, uses the existing CaOwner
and actual ResourceAccess File snapshot, then starts its controlled carrier
owner and admission. There are no new size/task/default schema values. Relative
CA paths retain the original source-dir base; connect never reopens the CA.

Reserve persists the opaque same-slot adoption before native preparation or
await. The actual upstream provider installs the original control, and the
original TLS/H2 driver retains its tag through preparation and the single
spawn_tracked transaction. Drop/cancel only request retirement. Physical quota
is reusable only after the real Scope consumes the exact driver exit and the
original native control issues its matching private close receipt. Unknown
acquisition retains finite capacity and original source. Reaped complete
metadata is reclaimed, not kept as an unbounded per-connect history; shutdown
reports the actual resources still owned at shutdown. Scope task IDs are not
reported twice by protocol cleanup. Returned I/O preserves the same actual
scoped failure Arc when the physical driver fails.

Each library demand supplies an explicit absolute deadline; missing deadlines
are rejected before I/O instead of inventing a new timer or resetting a prior
request budget. Request Session identity is forwarded unchanged. The supplied library
NativeCarrierOpener only supports literal/empty-prefix Service carriers and
rejects real Business Some(SessionId) before native I/O. The library success
fixtures therefore prove Service behavior only. Production Business provider,
host resource graph/replacement, hostname DNS, relay close-control and ordinary
VMess MUX remain separate requirements. The public ordinary Options and proxy
native qualification continue to reject gRPC/CA resourceful configurations.

Hosted Auxiliary exits are delivered into the same bounded carrier Record. User
failure reads and actual owner cleanup first retain the original TaskJoined and
its unchanged failure Arc, then mark local acceptance on that witness. The host
must still acknowledge its actual publication receipt. Acknowledgement does not
close the native carrier or refund its physical slot. Actual no-user cleanup uses
that same Record; there is no Auxiliary history map or second task manager.
Ordinary scopes retain their existing task/source behavior. Service-only native
provider restrictions and all production resourceful eligibility gates remain.
