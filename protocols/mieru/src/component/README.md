# Native library boundary

`ClientPool::component_options` references the adapter's actual `Arc<ClientPool>`;
it never substitutes another pool. The caller supplies a finite, nonzero total
pending/live/retiring carrier bound. `PoolFactory` is roleless, with one total task
and carrier budget. A complete `with_profiles` set uses `PoolProfile::new` for TCP
or UDP underlay. Each root has its actual original owner, declared as
`ControlledOutboundStream` or `ControlledDatagramCarrier`, registered by ordinal
against the same actual host TaskScope before the pool claim. Without an explicit
recipe, `BackgroundCarrier` remains unsupported by the ordinary catalog.
`with_background_carrier` retains the single TCP-root API. Domains in the fixed-root library path require the
host's actual `ResolvingCarrierOpener` and `DnsService`; fixed recipes reject
nonempty prefixes. `with_dynamic_profiles` separately requires the persistent
host provider to reselect the current actual binding before acquisition. It can
borrow real controlled Stream/Datagram views from an opted-in donor. The ordinary
Proxy registration remains closed; no public configuration is added.

Initial native adoption is deliberately limited to a pristine pool. Any legacy `open`,
including a cancelled one or one retired by `clear`, permanently excludes native
adoption: the old detached driver has no transferable join evidence. After a native
claim, legacy opens stay rejected, including between native generations. The same
pool can accept a different native instance only after explicit shutdown receives
the original carrier generation's native and users receipts, every actual driver
join, and all pending/returned logical users. Cache clearing, Drop, a canceled
waiter, an old role handle or just observing an empty pool cannot enable takeover.
The confirmed cache values return before the next owner can claim; their destructors
run outside ownership locks. Unclaimed legacy pooling, retirement, credentials,
wire, padding and active-stream leases preserve their existing behavior.

The only native opener is the persistent owner's granted controlled `UpstreamTransport`,
with no business session. Keys include the exact owner generation, selected prefix,
dial policy, network/resolver generation and complete protocol options. TCP uses
the existing reader/writer. UDP uses the existing packet reliability, ACK/window,
authentication and framing driver, prepared synchronously and registered in the
same actual scope before its initial OPEN_SESSION wait. Each driver registers
once through the same-slot adoption's real `TaskRegistrar::spawn_tracked`; its failure retains the original
I/O error object. No auxiliary Tokio task, pipe or runtime is introduced.

The public recipe creates a cold, finite `CarrierOwner` for the same actual pool
component generation and starts it before admitting a connection. Each attempt
reserves one pool record before resolution, then retains every candidate's opaque
adoption before its native acquisition. DNS cancellation before native acquisition
returns only that unused reservation. A failed native candidate must receive its
actual original close receipt before the next candidate uses the same reservation.
Settled candidates are accepted before continuing; completed-native history is not retained. The provider installs the original native control
before returning prepared I/O. The same-slot tag survives the existing Mieru
authentication and driver preparation; the one real spawn installs its exact join
witness in both owners without an intervening await.

A declared background carrier claims a unique driver-custody view before native
preparation and moves it through the existing initialization into the one real
driver body. This is separate from business/Service caller custody: no dummy
caller task or business SessionId is made. The original physical close still
requires that exact driver's actual scope join. Failed inline authentication has
no invented driver exit. Every pending native open also holds a pool user lease;
on success the returned `NativeTcpStream` retains it, including both split halves.
Original FD closure and driver join cannot close a still-held logical receive view.

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

`PoolOptions::prepare_tcp` owns validated stream-role options (including underlay) and the same pool recipe;
the host canonicalizes that recipe before binding its real roleless component ID.
The standard `StreamFactory` validates the actual dependency generation and complete
pool recipe, and rejects a changed selected upstream binding before I/O in fixed mode. Dynamic
mode uses only its persistent host provider, validates the profile dial, and
includes the selected actual prefix plus network/resolver generations in the key. Its only
target confirmation point is the complete encrypted SOCKS CONNECT success response,
including the BND address and port. A submitted request or flush is insufficient.
The existing Mieru establishment Flow result remains explicit zero; authentication,
padding and ciphertext remain physical Outer observations.

`NativeTcpStream` retains that logical user lease and native logical-error forwarding. If the actual shared
driver's command receiver has closed, it waits for that same driver's real join
witness and forwards the original `Arc<ComponentError>`, preserving decision fields
and the original rich I/O payload. A logical-session rejection alone never waits
for or closes the shared driver. The host must consume real scope exits; no hidden
supervisor is spawned. Legacy stream types and status behavior are unchanged.

The existing TCP logical shutdown sends its CLOSE request and flushes it; it is not
a remote close acknowledgement or an independently proven receive-half lifetime.
This implementation preserves that behavior and makes no stronger close claim.
The standard factory exposes both Stream and Datagram roles over either underlay.
UDP ASSOCIATE logical streams reuse the same actual ClientPool, authenticated
connection and driver as TCP roles. Each transferred TX/RX view retains custody;
logical Control waits for both returned views and actual logical shutdown. Its
report records logical resource release only, never a native close or task exit.
Partial writes preserve their frame; changing an in-flight frame is Unknown and
cancels that logical session. Fragmented receives retain state across cancellation.

`with_profiles` freezes the complete protocol-owned credential/endpoint/options/
binding set. Reordering preserves identity; changing even unused credentials
replaces the whole pool. Multiple endpoints and underlays have individual original
CarrierOwners registered in the same host scope and one combined pool budget.
`build_with_carriers` receives the actual matching providers in declaration order.
Bound roles retain their own secrets and weak pool references, not copies of the
whole profile collection. Actual owner identity is checked before role start.

Node resolution uses `DnsQueryRole::Node`, the actual resolver generation and
caller deadline/cancellation, with no system resolver fallback. Candidate tickets
retain the original domain and full binding; they cannot authorize another domain,
prefix, endpoint, dial policy, scope or owner. Resolution and cache reuse fence
resolver/network changes. Native UDP retains its exact original datagram owner,
control and unique view before the protocol sees it; cancellation, driver join,
view Drop and native closure remain distinct.

Production factories remain unregistered. Relay prefixes still require actual
host-certified dependency/loan ownership and donor close evidence. Complete Proxy
provider assembly and coordinated legacy removal must precede all-path opt-in.
The library's real host/provider tests do not grant production authority. The
adapter keeps its one existing legacy pool; no parallel native/legacy pools exist.

## Logical borrowing and network retirement

A managed driver retains its actual same-slot `CarrierOrigin`. Optional carrier
roles return a unique Stream/Datagram loan, its real logical controller, and that
original lineage. Loan return never claims the shared donor FD or task is closed.
Mieru's controller waits for actual view return, an ordered writer CLOSE barrier
and reader entry removal, or the actual joined driver for an already dead entry.
Interrupted waits retain the same work. UDP logical control uses this same real
session retirement instead of interpreting an I/O error as successful cleanup.
The borrower installs the original LoanOwner before any subsequent await; both
borrowed underlays use the existing one real driver in the actual pool Scope.

Dynamic network/DNS transitions cancel actual old record tokens and retain each
original native/borrowed disposition. A new network is admitted only after old
records have their original close receipts and old logical user/control debts
are zero. Clearing the cache, joining drivers or returning one view alone cannot
admit the successor. One total finite budget still covers every profile and both
underlays. `clear_legacy` preserves compatibility reload without clearing a host's
native generation outside its replacement transaction.

The actual Proxy qualification assembly is narrower than the library surface.
Same-owner Mieru prefixes are exercised there. Cross-owner prefixes need actual
host dependency replacement, and local domain roots need actual host-owned DNS
query providers; they fail before unowned I/O. Neither path silently falls back.
See `design/v0.0.3/implementation/mieru-prefix.md` for the remaining production gate.
