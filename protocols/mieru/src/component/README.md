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

This first boundary does not yet expose a standard Stream role. That role must
validate canonical pool dependency IDs and await the complete encrypted SOCKS
CONNECT response before reporting target confirmation. UDP underlay and UDP roles
remain on their existing paths.
