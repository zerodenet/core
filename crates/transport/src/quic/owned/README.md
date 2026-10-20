# Opt-in owned Quinn libraries

The metadata adapter consumes the actual platform owner and its same-original
unique RX, TX and control grant. Cross-owner numeric identity equality is not
accepted. On composition failure both native ownership and views are returned.
Only the abstract Quinn UDP view escapes; there is no FD, raw socket, RX clone
or caller-native closure. The host retains actual native control and original
main/probe close ledger in MetadataSocketOwner.

RX takes/restores the unique receiver under a short mutex. The outer mutex is
released before native polling, caller wakers or observer callbacks, including
panic/unwind. Concurrent invalid RX polling is rejected rather than acquiring
another receiver. Write pollers consume the platform's caller-supplied finite
quota; failure retains its original source in a failed poller, with no fake
readiness or fallback native socket. No tasks/queues are added by this adapter.

Metadata success consumes the actual prefix once, including Apple's partial
batch result. It never resubmits the full request. Actual native non-WouldBlock
OS errors keep pinned Quinn's nonfatal-drop disposition. Original neutral I/O
observation records no submitted bytes for those errors. One bounded diagnostic
slot retains the first original error Arc and a checked aggregate count with an
explicit overflow indicator, not an infinite error-source history. Locally
rejected admission, metadata, retirement and helper Unsupported errors remain
errors; they cannot silently become plain UDP or legacy fallback success.

This library does not provide the canonical host native grant, accepted-user
budget, FinalMask/port-hop worker ownership, or production activation. Native
close reports are separate from actual endpoint/connection driver joins and
application failure acceptance. Keeping or dropping an abstract view does not
constitute any of these receipts. Existing constructors remain unchanged.

QuinnDriverOwner takes the actual existing TaskRegistrar and an explicit finite
active + registering + retiring/undelivered record limit. It creates no TaskScope,
JoinSet, runtime or supervisor. Its runtime uses the two pinned typed spawn
origins: Endpoint is Critical and Connection is Auxiliary. It records the actual
TaskId/witness, original I/O error in one Arc, and same-driver typed close cause.
A real panic/abort has the host's original JoinError, not a fabricated body exit.
Registration refusal retains its exact SpawnError, stops admission, and creates
no fake task ID, successful join or native close receipt. Generic erased spawn
and raw-socket wrapping are unsupported; abstract preowned sockets are required.

Task status/body reads do not accept failures. The unique owning exit receiver
explicitly receives a real joined receipt, retains the same original failure Arc
in its bounded record, and only then acknowledges hosted local delivery. A
ConnectionError (including LocallyClosed), a body completion, or merely reading
a join witness is not that acknowledgement. The host must still retain and ACK
its own original HostedExitReceipt. Deadline/cancellation leaves the actual
record and failure receipt intact. Records are reclaimed only after explicit
reception of the real join; original sources drop outside the owner mutex.

This is a **task-only** owner, not an endpoint/user lifecycle completion API.
Callers must retain and logically close/drop their actual endpoint/connecting/
connection/stream users before endpoint driver join; the actual endpoint driver
requires no remaining endpoint handles and no connections. The library neither
uses wait_idle as FD proof nor declares all users gone. Full sealed connection/
stream admission, synchronous construction-time failure handoff and canonical
host resource/mask/user graph remain required before any production integration.
In particular, the legacy infallible spawn hook cannot turn a rejected late
connection registration into a fabricated protocol success/failure. Existing
production QUIC paths are unchanged and their activation gate remains closed.

External I/O observers can be invoked while Quinn holds its own protocol state
mutex, even after the platform native gate has been released. The owned runtime
therefore binds to the same actual MetadataSocketOwner. Only external observer
callbacks/last-observer Drop are caught; the original Box panic payload is
retained in that exact actual driver's poll slot. After the original driver
poll returns and releases Quinn's locks, the same payload resumes outside all
those locks. It becomes the actual task's native JoinError; it is not an I/O
error replacement or a successful native outcome. Direct calls outside an owned
driver boundary immediately resume the original panic. A different metadata
owner is rejected before native I/O rather than consuming another driver's slot.

A whole TX call keeps its declared send permit through observation. Together
with the unique RX and mutable control this gives a checked structural bound
on simultaneous callbacks. Sticky fault stops new callbacks/installations/driver
admissions. Already-entered ancestor callbacks may also panic: the first actual
payload wins and additional originals remain in bounded persistent slots, never
dropped while the primary unwinds. They transfer only through explicit
take_suppressed_observer_panic; native close does not accept them. Last observer
snapshots after fault remain owned until this persistent owner is retired.
Skipped observation after a fault is marked unavailable, not complete traffic
coverage. There is no new task, packet queue, runtime, socket or production
default. Scoped thread-local state describes only this synchronous actual poll
stack, restores on all exits and retains no Engine, policy or host registry.

OwnedEndpointOwner now supplies a cold, sealed endpoint/user graph for this
library. The existing registrar supplies its real cancellation child and actual
driver IDs. start runs only after the caller has retained the owner. The handle
contains only a weak admission capability; it exports no Endpoint, Connecting,
Connection, native control or raw streams. Pending connect/accept, every actual
connection handle (including try_clone), and every actual send/receive stream
consume the caller's explicit finite user limit. Returned stream futures borrow
the sealed stream. Raw stream/cancellation-waker fields drop before its user
permit is released. Logical cancellation, FIN, EOF and returning from a cancelled
read do not release a still-owned stream or declare native closure.

Because the pinned hook returns no admission result, connect/accept check this
same driver's sticky actual registration rejection synchronously after the raw
constructor and before delivering its Connecting. A refusal drops the
unaccepted Connecting and returns a ChannelError whose chain reaches the same
original SpawnError. It does not forge a ConnectionError, retry or fall back.
The actual endpoint driver and native owner remain retained for cleanup.

Shutdown stops new admissions, cancels these actual users, closes and releases
the owner's Endpoint, then waits for all sealed user handles to release and the
real host collector to join the actual drivers. Only then does it explicitly
close the original main/probe sockets. Collected witness/source/native receipts
stay persistent across deadlines and retries. Failure reception still belongs
to the distinct explicit receiver; shutdown never auto-ACKs it. Reports retain
unknown native sources and acquisition sources by actual reservation/namespace,
aggregate confirmed counts without lifetime-ID history, and distinguish actual
physical completion from rejected registration and undelivered failures.

This closes only the sealed Quinn graph created by this owner. Previously
issued abstract native views remain fenced by the original native control, but
protocol users independently constructed from those views are not retroactively
registered. Canonical host grants and the complete accepted/mask/protocol graph
must prevent or own such producers before production activation. Existing QUIC
constructors, protocol integrations and defaults are unchanged.

The canonical hosted variant consumes `GrantedMetadataListener`: the same
host's weak scope identity, unique receiver, bounded sender and unique
observation authority. `HostedMetadataSocket::new` checks the actual original
views and registrar; `OwnedEndpointOwner::new_hosted` checks that scope again
before any Quinn task starts. The granting host keeps main/probe native control.
`shutdown_users` reports sealed users and actual driver joins/delivery only.
Its `users_and_drivers_complete` deliberately makes no native-close assertion.
The existing local-owner `shutdown` rejects hosted native cleanup with
`Unsupported`; the host must separately retire/close/persist its own originals.
This interface does not turn missing canonical user proof into coverage.
