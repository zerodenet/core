# Controlled root datagram library

This is an explicit library path. It does not activate production catalog gates,
introduce configuration/FFI fields, choose a capacity default, or convert public
service-purpose metadata into authority. Existing bare outbound UDP, per-peer
bound UDP and metadata/QUIC permissions remain distinct.

A canonical `ControlledOutboundDatagram` declares one exact literal peer,
original host `NetworkGeneration`, and caller-assigned native capacity. Default
dial policy and no preferred source port are the only supported policy. Capacity
counts live, retiring and Unknown original sockets. Each declaration ordinal
owns one persistent `ControlledDatagramOwner`, constructed without native IO.

Only the actual host factory calls `HostResources::issue_datagram_root` with the
original EgressInterfaceControl, exact declared ordinals, original Registrar and
an explicit independent query-record ceiling. The capability stores a Weak
reference to the same ResourceAccess and an opaque private root issuer identity;
a caller-created issuer or public purpose label cannot match the installed one.
The cold capability can be installed with a suspended actual Scope. It admits
no native operation until real lifecycle activation and original Scope resource
admission are open. Query task creation remains the original Scope's job.

`RootDatagramGrant::open` requires the actual query's original TaskJoinWitness,
matching the original weak Scope. It rejects a joined/foreign witness, wrong
peer/generation, cold/revoked lifecycle, and stale actual egress selection before
native acquisition. One native attempt per query/declaration reproduces the
original DNS candidate's single bind; packet send retries reuse that view.
The query record and Opening sentinel are persistent before native creation.
The original owner retains any native handle, including acquisition failures.
The exact control is stored before IO publication, generation rechecks or user
observers. A stale post-acquisition result returns the view but leaves its real
native owner charged. Registration consumption/unwind remains Unknown and does
not release the reservation or permit a replacement.

The sole RootDatagramIo exposes only original send/receive/local-address and
observer operations. It cannot export the raw socket or native close control.
Its Drop only marks view return. `grant.close(io)` consumes the unique view and
returns a same-entry opaque native receipt only after original explicit OS close.
The host retains that control and actual source independently of the caller.
Control revocation and actual close run outside all host/resource owner locks.

Only the actual acquisition owner's settled KnownNoNative/actual Closed proof
can mint RootDatagramRetryReceipt. It permits a later explicitly granted peer
only for that same root and original unjoined query. It does not authorize a
peer, a second attempt for the original declaration, or replay after payload.
Unknown and unverified diagnostics carry no retry evidence.

Native closure, unique view return and actual query Scope consumption are
independent. Shutdown fences all IO, closes returned views, and keeps pending
query/native obligations for another structured cleanup drive. A query body
return, cancelled wait, Scope Drop or empty records never manufactures native
ACK. Confirmed native counts are persisted before cancellation points and
extracted once as constant-size aggregate receipts. First acquisition/native
fault, current Unknown source, and first actual failed query receipt remain
bounded and independent of physical completion. Typed declaration coverage
matches the original ordinal/kind to this same provider's observations.

Source regressions cover real loopback bytes, 65 healthy acquisitions on one
reused FD slot, cancelled pending receive, original Scope join, foreign scope
and issuer denial, exact generation, original-address rebind, consuming Tokio
registration panic and native-only retry evidence. They have not been executed
by this worker; root owns all Cargo and runtime validation. The explicit DNS
consumer migration retains its original query job with one tracked registration
and original wire/source/ID behavior; full production default calibration and
unmigrated TCP fallback/DoT/DoH/DoQ roots are separate obligations.
