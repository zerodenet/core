# Controlled native datagram metadata

This opt-in platform library consumes no protocol/runtime and exports no Quinn
ADT, native socket, FD, socket clone, caller callback, or route-probe capability.
The pinned UDP helper supplies the existing native ancillary algorithms.

`MetadataOwner::new` is cold and takes only explicit `NonZeroUsize` limits. It
installs both the main and probe owners before acquisition. `bind` retains the
same original from native creation through metadata initialization and Tokio
registration. Linux GSO/GRO probes run on original separately owned sockets;
each successful/failing option test is followed by actual explicit close before
publishing its capability or attempting another address. A refusal due to
capacity or unconfirmed close is an error, not a fabricated capability=1.
Original native option diagnostics remain available for the current bounded
initialization; native unknowns retain their sources and original reservations.
Main/probe reports use separate namespaces and aggregate confirmed counts before
reclaiming slots. Registration consumption remains explicitly Unknown on panic.

The grant has one RX, cloneable send-only views and a unique host Control. A
sender borrows payload only during a synchronous native call. Its explicit
admission bound includes callers waiting for the native gate; overload rejects
immediately. Independent write pollers have their own explicit finite limit and
checked IDs. One native internal waker fans readiness out to those registrations.
External waker clone/drop/wake runs outside native/registry gates. Wakeups during
native polls defer until the original guard is released; synchronous reentry does
not recursively invoke callbacks. Cancellation broadcasts to every retained view.
Dropping a sender or poller does not retire the shared original. Control revocation
rejects future I/O but is not close evidence. Explicit close returns the actual
original native receipt; Unknown remains charged with its original source.

Send results describe actual syscall bytes/datagrams and whether the entire
request was submitted. Non-WouldBlock errors remain errors; partial Apple batch
success never authorizes replaying the complete payload. RX retains destination
IP, ECN and GRO stride, with original batch capacities. Invalid/unsupported
metadata is rejected before submission rather than silently sent as plain UDP.
The library does not authorize arbitrary source addresses, interface marks,
native route probes or business egress. A future host grant must enforce its own
exact binding/policy boundary before exposing these metadata views.

This is library qualification only. No Quinn runtime adapter, FinalMask worker,
listener transaction, accepted connection budget, host grant, configured default
or production activation is supplied here. Driver TaskExit proof remains a
separate host responsibility; this owner reports actual I/O only.

`MetadataDatagramControl::set_observer` installs the existing neutral I/O observer
on this original. Callbacks run after the native/poller gates are released. TX
counts only the submitted outcome prefix, split by its actual segment stride;
RX counts the actual GRO strides and final payload. Zero-length datagrams count
as one packet. Native send errors retain their original source and count no
submitted bytes; readiness backpressure is not an I/O failure. Observers must
not be stacked with a unit-success wrapper that assumes the full request was
submitted. A callback panic leaves the original in its persistent owner and
does not manufacture a native close receipt.

`try_send_detailed` additionally preserves whether a failure was produced by
local admission/validation/retirement or the original readiness/native helper
operation. It moves the same original I/O error; the existing `try_send` API
still returns that error unchanged. An error result provides no submitted-byte
receipt. In particular, helper Unsupported is not permission to fall back to
plain UDP or to swallow a rejected metadata request.
