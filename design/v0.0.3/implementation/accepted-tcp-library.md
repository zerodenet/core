# Persistent accepted TCP originals (library prerequisite)

This opt-in library cut extends the existing ControlledTcpListenerOwner. It
adds neither production activation nor a default/config/FFI budget. Its native
receipts do not certify the host's TaskScope, TLS completion, transferred users
or replacement. Ordinary uncontrolled/listener APIs retain their previous
behavior and are not retroactively qualified by this library.

## Exact API and authority

`ControlledTcpListenerOwner::new_with_accepted_limit(&TcpListenerDeclaration,
actual_bind_attempts, parent_cancel, NonZeroUsize)` is pure construction. It
reuses the existing canonical listener validation and sole listener owner. The
accepted ceiling is an explicit library parameter, unrelated to bind attempts,
UDP4096, Scope1, OS backlog or RS01 totals. There is no alternate manager/global
task map, second provider or implicit acquisition task. Existing new() leaves
accepted_evidence()==None, which means no accepted provider was installed.

Tracked `ControlledTcpListener::accept_owned()` returns `(AcceptedTcp, peer)`;
`AcceptedTcp::into_parts()` transfers a unique AcceptedTcpStream plus opaque
AcceptedTcpClaim. The stream supports AsyncRead/AsyncWrite/AsyncSocket and
ClientStream metadata, preserving the same original through ordinary boxes or
split wrappers. The claim contains private same-pool entry identity and checked
nonwrapping id. Neither gives the caller the native close control or raw FD.
Calling legacy accept() on this tracked view rejects before native acquisition.
The old untracked mode does not silently acquire this new authority.

## Installation and ambiguous registration

The bounded entry exists before polling native accept. Immediately before each
Tokio poll_accept it holds an Acquiring sentinel. Tokio1.54 internally performs
Mio native accept followed by consuming child registration; an Err or unwind may
therefore have acquired/consumed an original even though no TcpStream returned.
Such an entry remains permanent Unknown, with its original returned error or
pre-call unwind sentinel and charged slot. Errno alone cannot prove no FD.
No descriptor is reconstructed, retried or declared closed based on Drop.

A returned Pending proves that this synchronous poll returned without a child;
only that WaitingNoAcquisition reservation may be removed when its future is
abandoned. It produces no closed_io. Successful returned original moves into
the same entry's concrete ControlledTcpControl before nodelay, getters, hooks,
observers or protocol code. The new private from_accepted constructor performs
only the shared pure assembly with a constant label. The entry installs the
unique control through OnceLock before cleanup can borrow its async close gate;
installation cannot contend on that close gate or strand a returned control on
the accepting stack. Configuration then borrows
that same original through the existing synchronous SocketLease, leaving its
poll/close implementation unchanged. Configuration failure/panic retains the
installed control for explicit cleanup.

One fixed weak accept relay registers with both the cancellation future and
actual listener. External Waker clone/replacement/Drop occur outside native and
relay locks. Synchronous native/coop wake is deferred until the listener lock is
released. The registration retains no owner, protocol future or TaskScope; no
worker, actor or queue is added. Ordinary current-thread Tokio schedulers defer
coop wake themselves; the synchronous regression uses Handle::block_on's real
budget outside scheduler Context, preserving the ICMP qualification correction.

## Native close, capacity and honest evidence

`close_accepted(&same_claim, deadline)` calls only that entry's concrete original
control and returns an opaque AcceptedTcpNativeClosed on actual native closure;
None is unfinished. `matches(&claim)` checks the private same-entry Arc, not an
id/string alone. Foreign listeners, stale identities and reused slots cannot
close another original or reuse a receipt. Repeated close is idempotent; only
removing the matching retained entry increments the native count. Healthy
original slots may be reused beyond the configured concurrent count after
explicit native close; this is not a lifetime accept budget.

Native close may invalidate a still-retained I/O view. That is deliberately a
native-only primitive: it is not a transferred-user/task ACK. Every successful
transfer sets persistent history-free Users::Unaccounted. There is no public
"users complete" setter, Drop/Arc-count inference or fake TaskJoined. Therefore
tracked Owner.wait_closed always returns unfinished after such a transfer,
even when native originals have all explicitly closed. accepted_evidence also
requires revoked admission and NoTransfer before its accepted-subresource
provider_confirmed can be true. The listener and host scope remain separate.

Unknown holds its native slot and error. Close retains the existing Unix
EINTR/no-retry rule and Windows retry only for an actual known-open source.
First acquisition/close failure evidence survives healthy native reclamation.
The same owner's pending listener report and pool cumulative native counter
retain already confirmed receipts across error/canceled cleanup; native-only
report extraction consumes them once. Current unfinished observations are
regenerated rather than accumulating deadline history. take_native_report
allows partial receipt extraction when a child Unknown makes wait_closed Err.
A complete/empty extraction is never whole provider proof.

## Validation source and remaining integration

Source regressions use actual loopback clients/original controls: bounded
pending accept/cancellation and no legacy bypass; bidirectional accepted I/O;
real peer EOF after explicit close; foreign/same-id/old-claim isolation; expired
close retains its original; 65 actual accept/echo/close cycles at cap one;
retained split users never create provider confirmation; actual post-install
panic; hidden dead-reactor registration error and no-IO registration panic with
permanent Unknown; original error identity and one-shot partial listener ACK;
real zero-credit native wake getter reentry and external Waker Drop.

No Cargo or runtime execution is claimed by this implementation commit. The
sole root Cargo owner must compile/run the existing listener suite plus
`controlled_listener::accepted::tests`, then run platform/full required checks.
Windows cross-compilation alone cannot qualify Windows native runtime behavior.

The next required cut binds actual handler TaskIds/hosted exit receipts and
finite transfer/driver obligations to these same entries, persists handlers in
InboundOwner instead of a local JoinSet, and migrates selected inbound transport
background drivers to actual joins. Accepted budget/default calibration and
concrete TokioSocket/protocol transfer seams are also pending. Until then,
Users::Unaccounted cannot be cleared and production activation remains gated.
