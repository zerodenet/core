# DNS production callers and deletion gate

Source audit: private `b3ad8aa8dff38cb639f95891a69c4488756cd91c`, based on
M `420cbcc8900cf7d7a2993682bf5a3154a05f9971` and ordinary QUIC donor
`7d1c02121d53289e850b41dc0f5c984853e15967`. The new A/B protocol-original
pair is source-only and has no runtime qualification. This is a caller census,
not a per-transport test matrix or a declaration that the DNS domain is closed.

| Actual entry/caller | Actual responsibility and standard landing | Remaining prerequisite / deletion gate / required regression |
| --- | --- | --- |
| Proxy default DNS, Node/bootstrap and native System queries | `runtime.rs` always installs `query_dns` and `native_dns`; `owned_root::exchange_backend_attempt` returns directly through the native source when present. Each actual query uses its original coordinator and registered Query caller; System blocking uses the configured real owner, original blocking job, registration release and native-complete evidence. | Already implemented native/System ownership is retained. An inert standalone resolver object is not another admitted blocking executor. Recheck real cancellation/network generation/exit; no new resolver or fallback. |
| Direct native UDP/TCP/DoT/DoQ | `native_dns::Provider` installs each original before IO and retains it in the same NativeUse. Native, logical users, actual task/driver outcome and registration ACK are independent. DoQ serial probes cost actual main+one probe, not configured slot count. | Existing actual native and standalone tests do not qualify every new donor. Preserve failure-before-next-candidate and late fault handoff; no resource-less replacement path. |
| Persistent DoH | Actual H2 pool driver is the NativeUse caller. Individual query leases share that physical owner; subscriber finish/cancel does not retire the pool. NativeUse closes the real logical LoanOwner before waiting for original Root settlement. | Joint pair adds real native/borrowed QUIC two-query reuse, cancel-one, active-budget Busy, idle replacement and exit tests. They are unrun. Do not request retirement per successful subscriber or wait on its still-legitimate pooled view. |
| Direct and ordinary resourceful detours (including managed SS/SIP paths) | `provider/detour.rs` captures actual ServiceSelections and installed roles; real caller/pending original and exact Stream/Datagram loan enter the same query row. Original controls and reported native/users/Scope facts remain authoritative. | Keep real direct pending/candidate/native/ACK regressions. Mac arbitrary SIP whole-job provider is an actual implementation gap; no PID/EOF/group approximation grants closure. |
| Ordinary non-MUX VLESS/VMess/Trojan and existing Stream Relay | Actual installed Node/Service roles return their same loans. Setup Aux really joins and ACKs; VMess per-connection gRPC retains its real H2 driver per layer. Lower native and parent borrowing remain distinct. | B ordinary EOF/standing-retirement corrections need exact-input ordinary regressions. Three-node DNS must preserve each actual driver/parent/native receipt; no caller cancellation or deadline/assertion reduction. |
| Native MUX and cached gRPC to MUX | Child uses the real physical pool. VLESS cached-gRPC/MUX has upper MUX driver/native-none and borrowed lower H2 driver/native; other existing owning MUX paths retain their original native/driver shape. | Existing legal material consumers remain. New source is not qualified by old unit tests. Advanced or missing roles cannot borrow an anonymous pool through inferred dependencies. |
| New ordinary owning QUIC, native and borrowed | B seals the same per-call Entry/Phase/Scope/physical prefix into ProtocolOriginal before Aux or first await. A validates the canonical Query observer and exact owning installed role/provider/Scope; the same NativeUse prepays and retains its finite observation table. Returned loan custody is released before passive wait for true local and global Root removal. | Joint B39 + A validation3/private consumer is the current freeze. Preserve real UDP pending suspension, same-binding foreign observer rejection, native-positive with held HostACK, actual global-N removal wake, cancellation/retry and persistent DoH reuse. No second Datagram provider or fabricated early carrier origin. |
| WG/plain linked/device/standalone routes | Actual Source/wire ownership and original packet driver/loan are separate from the Query child. Standalone native owner remains explicit. TEMP logical child close never proves its outer physical/reconnect ancestor closed. | C Source/PacketDriver code and earlier plain/standalone results are not new ordinary TEMP qualification. Require concrete same-input TEMP Stream+Datagram wire/Ready/ACK and full caller custody before removal of its old chain. |
| Mieru and other advanced detours | Current genuinely installed managed donor paths retain their real owning Scope/pool; no second compatibility pool is admitted after native ownership. | Full mixed/legacy Mieru, QUIC/H3 advanced variants, XHTTP/Xmux/mKCP/REALITY/browser/reverse/encryption and two Kernel PacketPath(None) borrowers remain actual B/C implementation duties. Blocked native selection is not an acceptable completed implementation for a legal configuration. |
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
