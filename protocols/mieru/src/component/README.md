# Native library boundary

`ClientPool::component_options` references the adapter's actual `Arc<ClientPool>`;
it never substitutes another pool. The caller supplies a finite, nonzero total
pending/live/retiring carrier bound. `PoolFactory` is roleless and declares both
that task bound and `BackgroundCarrier`. Build requires the corresponding actual
host grant. Nothing here registers a production Mieru factory or adds configuration.

Native adoption is deliberately limited to a pristine pool. Any legacy `open`,
including a cancelled one or one retired by `clear`, permanently excludes native
adoption: the old detached driver has no transferable join evidence. After a native
claim, legacy opens are rejected. Unclaimed legacy pooling, retirement, credentials,
wire, padding and active-stream leases preserve their existing behavior.

The only native opener is the persistent owner's granted controlled `UpstreamTransport`,
with no business session. Keys include the exact owner generation, selected prefix,
dial policy, network generation and complete protocol options. Authentication uses
the same existing TCP reader/writer and wire implementation. Each driver registers
once through the same-slot adoption's real `TaskRegistrar::spawn_tracked`; its failure retains the original
I/O error object. No auxiliary Tokio task, pipe or runtime is introduced.

The public recipe creates a cold, finite `CarrierOwner` for the same actual pool
component generation and starts it before admitting a connection. Each attempt
reserves a grant and retains its opaque adoption in the pool record before native
preparation or the first await. The provider installs the original native control
before returning prepared I/O. The same-slot tag survives the existing Mieru
authentication and driver preparation; the one real spawn installs its exact join
witness in both owners without an intervening await.

Driver-future destruction only marks retirement. The real scope must consume that
driver's join before the controlled owner explicitly closes the original fd. Only
the matching opaque physical-close receipt and the same join witness allow the
record's capacity to be reused. Interrupted cleanup or unknown acquisition retains
the original owner and capacity. The host reports the actual task ID/failure;
protocol cleanup does not report it a second time. Previously observed closed
records are removed, rather than accumulated as an unbounded shutdown history.
Neither Drop, abort, a peer EOF, flush nor a caller-supplied report mints a receipt.

Providers without controlled support fail before I/O, without falling back to the
old `BoxStream` opener. A private `cfg(test)` recipe alone preserves the old
uncontrolled diagnostic tests: a real join there still leaves physical I/O
unfinished and cannot recycle capacity. It has a distinct recipe identity and is
rejected by standard Stream preparation; it is unavailable to production callers.

`PoolOptions::prepare_tcp` owns validated TCP options and the same pool recipe;
the host canonicalizes that recipe before binding its real roleless component ID.
The standard `StreamFactory` validates the actual dependency generation and complete
pool recipe, and rejects a changed selected upstream binding before I/O. Its only
target confirmation point is the complete encrypted SOCKS CONNECT success response,
including the BND address and port. A submitted request or flush is insufficient.
The existing Mieru establishment Flow result remains explicit zero; authentication,
padding and ciphertext remain physical Outer observations.

`NativeTcpStream` changes only native logical-error forwarding. If the actual shared
driver's command receiver has closed, it waits for that same driver's real join
witness and forwards the original `Arc<ComponentError>`, preserving decision fields
and the original rich I/O payload. A logical-session rejection alone never waits
for or closes the shared driver. The host must consume real scope exits; no hidden
supervisor is spawned. Legacy stream types and status behavior are unchanged.

The existing TCP logical shutdown sends its CLOSE request and flushes it; it is not
a remote close acknowledgement or an independently proven receive-half lifetime.
This implementation preserves that behavior and makes no stronger close claim.
UDP underlay and UDP roles remain on their existing paths. Production factories
remain unregistered pending actual host grants and confirmed resource replacement.
