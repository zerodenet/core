# DNS production callers and deletion gate

Current source checkpoint: M `232958b344ac172e4c235d5b6fc6bd399633952a`.
This includes B's ordinary QUIC and cached/H3 original custody, the 102-file
mixed producer, A's DNS wire allocation/candidate/read-only view pair, actual
Direct/SS/SOCKS Datagram consumers. A13's nondefault WG/device slice was
subsequently identified as outside current configuration acceptance and is
retracted below; it has no runtime qualification.
Source acceptance is not runtime qualification or DNS-domain closure. Historical
results below remain tied to their stated source and lock.

The d68 exact pre-delivery Root/initializer tests passed. On f616, Mieru's four
UDP cases and concurrent ACK, SOCKS health, and SIP DoT/warm-DoH process cases
passed. The SS first-query failure on that input was traced to loss of the
actual peer ResourceAccess/Scope; its paired Source-context repair is now
installed but needs a new ordinary wire input. Direct's new actual UDP consumer likewise awaits its new Proxy wire input.
The two new A13 nondefault WG fixtures would fail before IO at the existing
Direct-only dial validator; they do not constitute a qualified caller.
The wire-policy/DoH followup's exact 070011 input passed DoH21, policy5 and
listener47. These separate results do not qualify current mixed/H3 or all DNS.

The actual installed ordinary VLESS/Trojan factory wraps its framing role in
`component/background/stateless::datagram::Datagram`; the inner role alone is
not the installed donor. Existing UDP-over-Stream framing remains the original
contract for those callers. Plain standalone and linked WG share the actual
standard Stream/Datagram provider and Scope; ff143's six standalone tests are
qualified only on that input. WG DNS is not a TEMP implementation dependency.
The cached/mixed donor uses accepted default-dial configuration. Its actual
self-owned Datagram classification and two-half UDP/persistent-DoH regression
pair is installed in c058c333a; runtime qualification is still pending.

| Actual entry/caller | Actual responsibility and standard landing | Remaining prerequisite / deletion gate / required regression |
| --- | --- | --- |
| Proxy default DNS, Node/bootstrap and native System queries | `runtime.rs` always installs `query_dns` and `native_dns`; `owned_root::exchange_backend_attempt` returns directly through the native source when present. Each actual query uses its original coordinator and registered Query caller; System blocking uses the configured real owner, original blocking job, registration release and native-complete evidence. | Already implemented native/System ownership is retained. An inert standalone resolver object is not another admitted blocking executor. Recheck real cancellation/network generation/exit; no new resolver or fallback. |
| Direct native UDP/TCP/DoT/DoQ | `native_dns::Provider` installs each original before IO and retains it in the same NativeUse. Native, logical users, actual task/driver outcome and registration ACK are independent. DoQ serial probes cost actual main+one probe, not configured slot count. | Existing actual native and standalone tests do not qualify every new donor. Preserve failure-before-next-candidate and late fault handoff; no resource-less replacement path. |
| Persistent DoH | Actual H2 pool driver is the NativeUse caller. Individual query leases share that physical owner; subscriber finish/cancel does not retire the pool. NativeUse closes the real logical LoanOwner before waiting for original Root settlement. | Joint pair adds real native/borrowed QUIC two-query reuse, cancel-one, active-budget Busy, idle replacement and exit tests. They are unrun. Do not request retirement per successful subscriber or wait on its still-legitimate pooled view. |
| Direct and ordinary resourceful detours (including managed SS/SIP paths) | `provider/detour.rs` captures actual ServiceSelections and installed roles; real caller/pending original and exact Stream/Datagram loan enter the same query row. Original controls and reported native/users/Scope facts remain authoritative. | Direct standard UDP now has its own sealed request, same actual CarrierOwner/Scope/N and original UDP receipt; its new wire regressions are unrun. SS/SOCKS actual native roots and existing SIP process owners stay separate. The complete arbitrary SIP whole-job provider remains a concrete prerequisite, never inferred from PID/EOF/group state. |
| Ordinary non-MUX VLESS/VMess/Trojan and existing Stream Relay | Actual installed Node/Service roles return their same loans. Setup Aux really joins and ACKs; VMess per-connection gRPC retains its real H2 driver per layer. Lower native and parent borrowing remain distinct. | B ordinary EOF/standing-retirement corrections need exact-input ordinary regressions. Three-node DNS must preserve each actual driver/parent/native receipt; no caller cancellation or deadline/assertion reduction. |
| Native MUX and cached gRPC to MUX | Child uses the real physical pool. VLESS cached-gRPC/MUX has upper MUX driver/native-none and borrowed lower H2 driver/native; other existing owning MUX paths retain their original native/driver shape. | Existing legal material consumers remain. New source is not qualified by old unit tests. Advanced or missing roles cannot borrow an anonymous pool through inferred dependencies. |
| New ordinary owning QUIC, native and borrowed | B seals the same per-call Entry/Phase/Scope/physical prefix into ProtocolOriginal before Aux or first await. A validates the canonical Query observer and exact owning installed role/provider/Scope; the same NativeUse prepays and retains its finite observation table. Returned loan custody is released before passive wait for true local and global Root removal. | The B39/A pre-delivery pair and subsequent returned-custody consumer are installed. Preserve real UDP pending suspension, same-binding foreign observer rejection, native-positive with held HostACK, global-N removal wake, cancellation/retry and persistent DoH reuse. New cached handoff proves exact logical original custody, never physical/global settlement. No second provider or fabricated early origin. |
| WG/plain linked/device/standalone routes | Actual Source/wire ownership and original packet driver/loan are separate from the Query child. Standalone native owner remains explicit. TEMP logical child close never proves its outer physical/reconnect ancestor closed. | Plain WG is available independently of TEMP. A13's nondefault WG slice is retracted: the unchanged validator accepts nondefault dial only on Direct. Original default-dial WG wire/TC/DoT/DoH/Scope/ACK tests are preserved byte-for-byte; no runtime qualification is claimed for the removed new fixtures. C G4 software outer Some uses the new memory-only workspace prelude but still needs the complete actual factory/wire/Ready/ACK caller pair; physical/reconnect ancestry stays Unknown. |
| Mieru and other advanced detours | Current genuinely installed managed donor paths retain their real owning Scope/pool; no second compatibility pool is admitted after native ownership. | B102 actual mixed XHTTP/Xmux TCP/H3 halves are now installed under one Entry/Scope/book; default-dial DNS selectors consume the installed cached-provider predicate. Nondefault cached/mixed dial is outside the unchanged Direct-only acceptance boundary; it is not a missing accepted caller. Browser/mKCP/remaining advanced factories, full mixed/legacy Mieru and C software outer callers retain their concrete implementation/qualification prerequisites. The two Kernel PacketPath(None) borrowers now have their actual Business custody pair; its presence is not blanket advanced donor qualification. A blocked legal configuration is not a completed implementation. |
| Reserved DoQ.detour | `model/dns.rs` explicitly reserves the field; `validate/dns.rs::validate_servers` rejects it after checking the referenced target. Both parsed and hand-built RuntimeConfig inputs reach this validation before Kernel resource acceptance. `zero-dns` also rejects a detoured DoQ backend rather than dial directly. | Not a current legal Kernel combination and not a TEMP dependency. This corrects the earlier A planning note that called it a missing implementation. Native DoQ and explicit standalone DoQ provider duties remain; no new detour syntax or validation relaxation is proposed. |
| Fake-IP Default source | Default-only actual ControlledJournal declaration/lease uses the same Source graph and real journal OriginalFiles; decode/write/rotate/native close and Scope/ACK remain separate. Node does not open another journal. | M recorded FakeIP14 and Journal8 on their exact earlier input. Ordinary Proxy journal reload still needs its actual ordinary test input. Keep matching close, paid read/write state, typed late errors, generation overflow rejection and unknown owner retention. |
| Installed ProxyDnsOutboundConnector | Only `runtime.rs` installs it; its private adapter contains Session(0) and prepare_datagram_exchange. All Kernel backend exchange attempts with native source return earlier. Therefore it has no Kernel execution, and does not protect advanced functionality. | User-required order remains: finish all legal real donors/callers, then delete installation, private adapter, cold current-services fixture and obsolete Session(0)/prepare branches together. Deleting this unreachable installation now would not complete any blocked donor. |
| Genuine zero-dns standalone/custom connectors | Explicit no-native-source DnsSystem construction uses its existing owned UDP/DoQ/custom connector or standalone System LookupOwner. Its own actual owner/close/task responsibilities remain. | These are real library callers, not a Kernel fallback. Standalone hard lookup16 is not evidence that the four production native-DNS configurable limits are absent; do not replace it with another executor or remove its legitimate API. |

## Bounded pre-delivery observation

The new table is capped by the same selected owning roles' checked finite
metadata/service-datagram quotas and prepays its actual allocation with the
existing Host PacketDriver metadata book. No new graph walk, registry or IO
authority is created by the observer. Actual instance/provider/Scope, ordered
physical prefix, canonical weak observer and current Query registration all
must match. Removed retry rows require positive original settlement; they and
paid credits drop outside the Query mutex. Metadata origin is recorded before
IO, while a borrowed driver's origin is recorded only when it really exists.

A successful persistent connection transfers custody to its actual pool/driver;
query completion does not retire that pool. Failed/pre-delivery cancellation
continues to hold the same row until original native/users/driver ACK and true
Root return. The Root's original fault receiver, actual report delivery and
NativeUse report receiver retain sources before local receipt acceptance. A
receipt is neither native closure nor Scope ACK.

## Exact whole-chain removal condition

Delete the obsolete Kernel connector installation only after ordinary legal
resource/shared-pool/device/advanced and both Kernel borrower caller sets use
their actual installed donors, including failed setup/cancellation/late error,
network/configuration replacement and shutdown. Keep standalone APIs with real
callers. API presence, a sealed DTO, a registered witness or a source-only patch
cannot substitute for actual provider/Ready/wire/native/join/ACK evidence.

## DoQ acceptance evidence and scope correction

* `crates/config/src/model/dns.rs`, Doq.detour: the field is explicitly reserved;
  its documentation states that validation rejects it.
* `crates/config/src/validate/dns.rs::validate_servers`: a specified DoQ detour
  returns `ConfigError::InvalidDns`; `validate_dns_config` invokes that check.
* `RuntimeConfig::parse_with_source_dir` (`model/mod.rs`) calls `validate()`;
  hand-built Proxy inputs pass `Engine::new` to
  `PreparedConfiguration::prepare` / `PreparedRouting::prepare_after_gate`,
  which also calls `config.validate()` before compile/preparation.
* `crates/dns/src/backends.rs::ResolverBackend::exchange` only admits DoQ when
  detour is absent. An explicit standalone `quic_bidi` connector is used on that
  branch; a detour returns Unsupported, without a direct-dial fallback.

The exploratory `a-dns-detour-doq-v1-mutable` plan was not applied and is not a
remaining legal-caller implementation requirement. No DoQ production hunk was
written. This is a scope correction, not evidence of a newly closed caller.

## Remaining deletion work, in order

1. Qualify the accepted default-dial cached/mixed DNS consumers against their
   actual installed provider, retaining successful persistent DoH physical
   custody and original Query logical return separately. Qualify Direct and
   SS peer repair on their actual ordinary inputs. Nondefault dial remains
   Direct-only; do not invent a Direct+WG relay to salvage a rejected fixture.
2. Consume each remaining concrete B/C factory when delivered: no generic
   Datagram-role allowance, second resolver, anonymous pool, fake Session or
   Source witness. C plain standalone WG is already available; C G4 outer Some
   and B browser/mKCP/other uncompleted factories are different prerequisites.
3. After all legal real caller sets have migrated and their cancellation,
   network/config replacement, fault delivery and shutdown obligations are
   proven, remove the old install in `runtime.rs`, private
   `runtime/dns_outbound.rs` adapter, its cold current-services fixture and its
   Session(0)/prepare_datagram_exchange branches together. The native-source
   early return in `zero-dns::owned_root::exchange_backend_attempt` makes this
   installation inert for Kernel execution now; keeping it supplies no advanced
   execution. Genuine standalone/custom connector APIs keep their real duties.

## Nondefault dial scope correction

`crates/config/src/validate/dial.rs::validate_outbound_dial` rejects every
non-Direct outbound whose dial is not default; `validate.rs` invokes it for
every configured outbound. The existing oracle
`crates/config/tests/outbound_dial.rs::dial_constraints_are_a_common_field_with_direct_only_nondefault_support`
asserts this exact public boundary. Neither B's internal Policy projection nor
C's sealed underlay binding makes a rejected RuntimeConfig legal.

The A13 new WG source_ip fixtures parse the configuration before Proxy/native
IO. Their first result would therefore be InvalidOutbound, before all proposed
wire/Scope checks. No run is claimed. The original frozen A13 source and its
hash manifest remain outside the repository as historical evidence. Its new
unreachable logical-root branch and two new unaccepted fixtures are retracted;
the original accepted default WG fixtures and assertions return byte-for-byte
to their previous source. B102 cached predicates, c058's actual self-owned
Datagram classification and all mature original ownership gates remain.
No validator, legal target, timeout, cap, native receipt or Scope ACK is weakened.
