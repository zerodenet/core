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

The pinned Hyper `ConnMapErr` erases some underlying connection failures to `()`;
this seam preserves sources it actually receives, including executor admission,
outer Hyper errors and body/request errors. It cannot reconstruct an OS error
which Hyper already discarded. Panic/abort's original JoinError remains in the
actual TaskScope/witness; a waiter awakened by aggregate Drop receives Revoked,
not an invented JoinError or physical-close acknowledgement.
