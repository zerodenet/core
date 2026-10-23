# Finite H2 aggregate library seam

`spawn<B>` consumes one original `ControlledCarrier` from the caller's already
started persistent `CarrierOwner`. It calls that same carrier's adoption with
`CarrierIo::into_driver` before the handshake is polled. Exactly one TaskRegistrar
task drives the original handshake, returned Hyper outer future and every original
future delegated to its explicit executor. There is no pool, production activation,
new runtime, thread, executor spawn or default resource authority here.

`Options.delegate_limit` is explicit and nonzero. A real bounded unit channel
retains one occupancy token for each queued or active delegate. Its original
`try_reserve` failure contains `()`, never the rejected future. The first structured
failure Arc wakes every pending handshake/request/body waiter; the typed sender
does not expose a raw sender which could discard this source. Requests are sent
once without replay. CONNECT upgrades are rejected before submission.

The executor and occupancy return path hold only Weak inbox references. Cleanup
closes admission and drops queued originals outside the lock; it cannot leave a
future owning its own receiver or store the rejected future in the error. Normal
outer completion waits for the remaining delegates, and does not inject a new
error into buffered response bodies. Cancellation/error/abort still requires
actual scope JoinSet consumption before the caller's original native-control
confirmation releases physical capacity. `PendingH2::ready` cancellation or Drop
does not refund capacity or issue a close receipt. Its witness proves only this
actual aggregate task joined.

Caller responsibility remains: persistent owner lifetime; correct scope/class,
actual cancellation lifetime and finite limit; scope join progress; native close;
user/response leases and replacement admission. This seam does not qualify Xmux
pool maintenance, logical upload/download workers, H3/Quinn, TLS/CA/ECH resource
preparation or mixed prefix native ownership. It does not change legacy call sites.

The unique IO view records the first actual poll error as its original
`Arc<io::Error>`, then returns the same error kind with a shared-original source
wrapper. The facade's `raw_os_error()` is not the original code: read that code
from `failure_evidence().io_source()`. Pending, EOF and successful polls record
nothing. A raw poll error alone does not invent a fatal connection result; the
original protocol operation retains its error handling.

The pinned Hyper 1.9.0 has a narrowly optional connection-error observer, installed
only on this managed path. It consumes the actual `h2::Error` which `ConnMapErr`
would discard to `()`, without changing the delegated future's result or adding
tasks. That actual fatal result wakes the aggregate and its waiters. Legacy
builders have no observer. A stream RST remains a local Protocol/Session error;
it does not publish global connection failure and siblings can keep using it.

`H2ConnectionFailure` retains at most the first original IO, h2 and Hyper object
actually observed, with typed getters. The same first ChannelError Arc and all
seven decision fields stay unchanged when later evidence/cancellation arrives.
Some failure paths never produce one of these types: no synthetic Hyper/h2/IO
object fills a missing field. Request-local evidence stays separate from the
connection's fatal evidence. A diagnostic getter is not a resource grant.

The lower h2 protocol still stringifies IO sources internally and its `Error`
does not expose `get_io()` through `Error::source`. Hyper's `new_h2` also consumes
an IO-type h2 error. These APIs are unchanged: the separate original IO Arc and
the optional exact ConnMapErr h2 object preserve their actual evidence instead
of claiming those reconstructed h2 IO facades retain a native OS representation.
Panic/abort's original JoinError remains in the actual TaskScope/witness; a waiter
awakened by aggregate Drop receives Revoked, not an invented JoinError or
physical-close acknowledgement.

Hosted task failure delivery has an explicit per-carrier local receiver. The
actual caller must retain `PendingH2::local_task_receiver()` beside its existing
CarrierOwner before dropping request handles, then use `receive_joined` (or
`wait_and_receive` while the real supervisor drives joins) during no-user cleanup.
Failure stores one actual TaskJoined before marking its local acceptance. Its
original H2/Hyper/I/O first-error Arc remains distinct from the scope aggregate's
original failure Arc. Hosted request errors wait for that actual join and local
acceptance; ordinary scopes keep their previous error-return timing. Drop,
read-only witness observation and absence of a receiver never acknowledge an
error. The host still acknowledges its own original exit receipt to release task
credit; native capacity additionally needs the original close receipt. This is a
library capability and does not activate a production XHTTP owner or host grant.
