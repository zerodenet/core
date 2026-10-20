# Controlled ICMP library cut

Implementation commit: `073d7ea81fec892ae1eb0ffe94fca57c518b06f5`, based on `10f88c5a`.
This is an independently testable platform library. It does not activate a
production factory, add a canonical declaration, or migrate ordinary ICMP/TUN/
FinalMask users. The rawIP caller and task graph are a separate integration.

`zero_platform_tokio::controlled_icmp` provides:

- `ControlledIcmpOwner::new(NonZeroUsize, CancellationToken)`: no IO or tasks.
- `bind(&mut self, IpAddr, Option<&EgressInterface>)`: synchronous acquisition,
  returning a unique `ControlledIcmpSocket`; the pool remains the native owner.
- `close(&mut self, &ControlledIcmpSocket)`: explicit same-entry native close,
  returning an opaque `IcmpCloseReceipt`. `matches(view)` compares the actual
  entry. Another pool's equal numeric id and an old view cannot close a new fd.
- `occupied`, `revoke`, `take_report`, `close_all`: bounded ownership, retirement
  and one-shot successful counters. Unknown originals and their sources persist.
- The unique view keeps async `connect`, `send`, `send_to`, `recv_from` with
  mutable receive ownership, plus `local_addr`, `reply_identifier`, `mode`,
  `reservation`, and the original optional RAW-permission error.

Install the actual pool in the persistent rawIP resource holder before calling
bind or scheduling a probe. The caller passes its existing64 concurrent limit;
this is not a platform default or a lifetime total. Closed/NoNative entries can
be reaped, so the65th healthy lifetime probe works at concurrency1. Checked ids
never wrap. Live, retiring and Unknown entries stay charged. A permanent Unknown
prevents new acquisition; it is never repaired by creating another socket.

The entry exists before native creation, with an Unknown sentinel before each
consuming call. A returned socket enters the same holder before flags, bind,
interface configuration, instrumentation or registration. Only RAW creation
PermissionDenied permits one DGRAM creation with the same ICMP protocol, after
recording that actual no-FD failure. A post-create failure explicitly closes its
original and never enters fallback. No ordinary UDP authority, cloned descriptor,
probe socket, detached task or payload queue is added. Unix uses direct native
creation with atomic CLOEXEC where available; Windows uses the existing explicit
Winsock initialization, without socket2's hidden initialization socket.

Registration consumes the original, so an error/unwind remains permanent Unknown
with its real source. Native close uses the existing socket close primitive:
Unix errors including EINTR never reconstruct/retry; Windows retries only its
explicitly retained known-open WSAEWOULDBLOCK outcome. Successful close and caller
Drop/cancel remain distinct. View Drop cancels that entry, retaining the original
for owner cleanup. Pool Drop cancels admission and never issues a report.

`IcmpOwnerReport` separates original acquisition and close faults, a retained RAW
permission source, live/Unknown entries and successful counters. A successful
permission fallback is not a terminal acquisition failure. `known_no_native`
counts terminal entries that acquired no original (including unused reservations),
not native attempts. Successful counters are consumed once by take_report; first
fault/source Arcs remain available independently. These are native facts only;
actual probe TaskExit, packet users, queues and stack restoration still require
caller-owned evidence. No error owns the pool/entry through a reverse cycle.

Native I/O and cancellation register only the entry's two fixed read/write
relays. A relay holds a weak reference to a target owned by the actual waiting
future, never an Entry, owner, TaskScope or protocol future. A counted native
critical section defers relay wake delivery; its guard unlocks the original
mutex before flushing external targets. External Waker clone/replacement/Drop
also runs outside native/relay/target locks. The native registration therefore
does not strongly retain the caller through a waker cycle. No worker, actor or
payload queue is introduced. Two additional native regressions exercise actual
pending receive plus Tokio cooperative synchronous wake and replacement Drop;
the external callbacks inspect and explicitly close the same real original.
These regressions still require root execution; source completion alone does
not qualify the reentry behavior.

## Validation status and commands

This agent ran only manual rustfmt --check and git diff --check. No Cargo,
compilation, socket creation or runtime test was executed in this worktree.
Two metadata/identity tests run by default. Ten actual native tests are
explicitly ignored until run in a suitable environment; a default suite passing
with these ignored does not qualify native ICMP.

Root is the sole Cargo owner. Suggested focused commands:

```sh
cargo test --locked -p zero-platform-tokio --all-features --lib controlled_icmp::tests
cargo test --locked -p zero-platform-tokio --all-features --lib controlled_icmp::tests -- --include-ignored --skip actual_raw_permission_denial_falls_back_to_real_icmp_datagram
```

The second command requires actual ICMP access, IPv6 loopback and an absent
TEST-NET source address for the deliberate post-create bind failure. Live cases
are serialized within this module. Echo targets are loopback only. Tests include
actual pending recv/cancel, native post-create panic cleanup and fd identity,
failed-bind source/close/reuse, dead-reactor registration Unknown, cross-pool/old
view isolation,65 successive real echoes with capacity1, and IPv6 identifiers.

For the permission fallback, root can execute the exact just-built test binary
in a separate child without RAW capability and with existing Linux ping-socket
permission, without changing host sysctls/network policy. For example, if available:

```sh
setpriv --bounding-set=-net_raw "$TEST_EXE" --exact controlled_icmp::tests::echo::actual_raw_permission_denial_falls_back_to_real_icmp_datagram --ignored
```

`TEST_EXE` must be the executable from root's pinned build metadata, not an
ambiguous cache wildcard. The test asserts actual PermissionDenied and actual
PingDatagram mode plus a real echo; it does not fabricate a source or skip on
missing permission. If this environment is unavailable, record that path as
unexecuted. Windows cross-compilation is not Windows runtime evidence.

A later rawIP typed host integration must declare a dedicated ICMP capability,
use this same actual pool as its sole provider and link it to real task/user/
restoration evidence. It must not relabel the resource Device or UDP, accept an
unsupported declaration with an empty report, or bypass authorization via the
legacy IcmpSocket path.

## Canonical host authority (staged)

`ResourceDeclaration::NativeIcmp { max_sockets: NonZeroUsize }` describes
native RAW/ping originals explicitly. It grants neither ordinary UDP nor Device
access. General `HostResources` continues to reject this declaration and its
coverage classifier records `Unaccounted(NativeIcmp)`; a filtered empty subset
cannot prove this capability covered. There is no generic `ResourceAccess`
method that could create a second provider for the same declaration.

The crate-private `HostIcmpProbeOwner::new(actual_instance, &declaration,
parent_cancel)` purely constructs the same persistent `ControlledIcmpOwner`.
The actual inbound registry must install this sole owner before start or any
probe; `ResourceLifecycle::start` opens its admission only while the actual
parent fence is live. `grant()` returns a Weak same-instance
`HostIcmpProbeGrant`, not the pool/control. Grant `bind(actual_bind_ip,
actual_host_egress)` requires Live admission, while `close(&same_view)` remains
available during retirement and validates the platform's exact pool/entry.
Host routing supplies egress; arbitrary protocol callers do not get this grant.

Grant operations use a short nonblocking pool mutex. Contention/reentrant
callbacks are rejected as NotSubmitted/Capacity before any native acquisition;
this is a transient admission result, not an extra native socket budget. The
caller must not interpret it as a successful probe. Owner cancellation is
issued outside that mutex; canceled/absent/stale owners cannot acquire sockets.
No task or waiter queue is added by the provider.

`shutdown` explicitly closes that pool's originals; Unknown remains charged
with the same original source and cannot become a complete report. A bounded
cumulative counter retains actual close receipts when another entry makes
shutdown return Err. `take_closed_io()` extracts only the not-yet-reported
aggregate, once; busy extraction returns no new receipts, never physical proof.
`fault()` retains the first actual ownership fault independently.
`physical_confirmed()` refreshes the same pool under a nonblocking lock, even
after an acquisition unwind poisoned the outer mutex; open admission, lock
contention, live originals and Unknown all refuse confirmation. The caller
still owes actual probe/task joins and transferred-user closure separately.

The provider has three ordinary no-I/O regressions plus four ignored real-native
regressions: pending receive/retirement/same-entry close; 65 actual acquisitions
with cap one and real release; dead-reactor Unknown preserving the original
source plus an earlier one-shot partial close receipt; and actual post-create
missing-reactor panic refusing stale unused-pool proof. They require explicit
execution by the sole Cargo owner. No compilation/runtime success is claimed
here; the normal TUN path and production activation gate are unchanged.

## Actual native wake qualification correction

Root47's original sandbox run failed all ten native cases before acquisition
(RAW EPERM, ping DGRAM EACCES). Its later unchanged immutable binary
`260f2a0fbd733aa99e5e041ba9be1044b1aefb28382c0d7bf86da8c464061b59`
in the authorized network-none container passed eight ordinary cases and the
separately executed real RAW-denial/ping-DGRAM fallback. The remaining original
cooperative-wake case failed its actual getter observation assertion. These
results do not qualify all ten cases.

Pinned Tokio1.54 `task/coop/mod.rs::poll_proceed` uses `runtime::context::defer`:
inside a scheduler Context, exhaustion queues a wake for the next scheduler
turn. The old test dropped its pending native future (and thus the sole strong
WaitTarget) before that turn and incorrectly assumed an immediate callback.
`consume_budget` commits each successful decrement with made_progress; a final
Pending creates no RestoreOnPending that would restore the exhausted budget.
The failure therefore does not show that native reentry happened or succeeded.

The corrected case keeps every original real pending-receive, exhausted-credit,
same-live-original getter, explicit close, exact receipt and one-shot count
assertion. An ordinary test drives its body with `Runtime::handle().block_on`:
Tokio's CachedParkThread installs a real coop budget without a scheduler Context,
so the same exhausted native poll uses context::defer's immediate wake path.
This directly exercises synchronous relay wake under NativeGuard without an
extra task/thread, fake wake, replaced socket or weaker assertion. It still
requires an actual native test rerun; no success is inferred from this change.
