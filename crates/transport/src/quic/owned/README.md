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
