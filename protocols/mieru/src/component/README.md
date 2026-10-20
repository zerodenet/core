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

The only native opener is the persistent owner's granted `UpstreamTransport`,
with no business session. Keys include the exact owner generation, selected prefix,
dial policy, network generation and complete protocol options. Authentication uses
the same existing TCP reader/writer and wire implementation. Each driver registers
once with the real `TaskRegistrar::spawn_tracked`; its failure retains the original
I/O error object. No auxiliary Tokio task, pipe or runtime is introduced.

Each physical attempt retains one bounded record. Driver-future destruction only
marks retirement. The actual scope join witness then proves task completion, and
the host owns reporting that task's ID/failure. The current `BoxStream` grant does
not convey physical-close authority: even after a real join, retirement consumes
its original capacity and shutdown reports unfinished physical I/O. Neither Drop,
abort, a TCP peer EOF in a test, nor flush is promoted to a generic close receipt.
The owner cannot be implicitly replaced. Controlled carrier adoption and real
physical-close receipts are required before production activation or slot reuse.

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
remain unregistered pending controlled carrier ownership and host replacement.
