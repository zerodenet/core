# A control callers: concrete retirement and remaining work

Current Logger proposal base: `93b31dc37a5b59597956152518f08f26a368b278`.
The table began at `a26d3955a46f49971b2f1e51cb7ac02eade2b8e4`;
its historical “not yet tested” statements are superseded only by the exact
input results below. M applied the complete Logger pair as `b8d55cdac` and
corrected actual joined-ACK/prepaid IO diagnostics in `d49b79e472bd1d5b766ef98e1193283646b32686`.
M reports the nine actual Logger tests passed on d49; this qualifies that group,
not the whole control domain or every ApplicationServices reload caller. Initial/early input is already implemented and separately
qualified: M `7e521a47` plus `74d4873b9697ab9a2b5df9efadd02847d994d074`,
lock `576b313f…`, four actual source/oracle/recovery tests PASS. This does not
qualify the new logger backend or the whole control/DNS domain.
This is a source/caller audit, not a whole-domain qualification. A did not run
Cargo, rustc or runtime tests. Coordinator results apply only to their recorded
source/lock/ELF. Windows execution and GUI behavior remain separate.

## Current source check (67183ba17de630aa67271a6ebfc574e2f5896ce9)

This later check follows the actual constructor, original Arc, issuer and
consumer, rather than treating an API name or a historical test count as proof.
The existing server/subscription/command/client, TLS, initial-input, inspection
and Logger original-close/join/report consumers remain installed. M has applied
Windows terminal panic/independent-peer-source retention as `a62961e6a`; it adds
no Windows runtime qualification. Its actual raw peer TaskId/JoinError and
original panic Box are preserved together after the original cleanup finishes.

Configuration persistence has a real memory gap in this source: pretty output
and the retained previous file have no allocation credit, and the previous read
is unbounded. The complete 19-file proposed fix is frozen outside the tree at
`a-source-persistence-memory-v1` (patch `0176a36a…`), not yet applied or qualified.
It uses the same actual Host book and exact candidate, preserves original
compensation/disposition/revision, and does not introduce a second book.

Quota native ownership is installed; quota task/memory ownership is not declared
complete. The production worker is an actual retained std thread, with actual
join and Source fault/lease receivers, but its spawn does not receive a
TaskRegistrar/TaskScope or the Host component-task budget. There is no Scope ACK
to infer from its std join. Its real startup/inspect decoder still materializes
balances/duplicate-check storage without a configured file/decoded bound, and
its worker clones snapshot keys/rows before allocating a serialized Vec without
prepayment. These are separate concrete followups; they are not covered by the
configuration-persistence proposal. The public unused raw quota read method is
not evidence that production calls that method: actual startup uses read_with.

| Actual caller | Actual owner / admission | Native, task and delivery gates | Source status / remaining work |
| --- | --- | --- | --- |
| Windows IPC server and every accepted pipe | `ipc/windows.rs`, `windows/owner.rs`: same `Originals::Entry`, Connection plus PipeWorkspaceBytes paid before first open | Real ControlledPipeOwner raw Win32 handle; real std worker and Scope witness; raw peer JoinSet ID; native matching close, view return, actual join and registrationReleased before entry charge release | Implemented. No Tokio NamedPipeServer or regular-file fallback. `request_stop` retains an entry and invokes native wake outside the registry lock. Panic/drain source pair and M a629 terminal server-panic/independent-peer JoinError retention are in source. No Windows runtime qualification is inferred. |
| Unix IPC server / pending activation / accepted connections | `ipc/unix{,/server,/originals}.rs`: original listener and preaccept Connection charge, real raw JoinSet and original controls installed before activation | Same listener close receipt and path removal; peer actual join before accepted close; complete actual report independently allows accepted row removal | New a26 report receiver preserves complete-report task/RF faults and returns them to the same raw server/shutdown receiver. No new executor or second book. This source is not yet tested. |
| HTTP listener and connection workers | `http_adapter/{lifecycle,server,connections}.rs`: installed original listener, actual accepted graph and Scope | Original native close and Scope report completeness decide retirement; request Work/Apply and payload credits persist through original joins | New a26 same-handle report receiver strongly accepts original task/RF payloads before local receipts. History does not turn a positive native/Scope report into an unfinished report. Typed failure still returns. New tests not run. |
| gRPC listener, accepted H2 connections and streams | `grpc/{lib,native,messages/executor}.rs`: same original graph, actual stream TaskClass registration, Root and per-connection counts | Actual H2 stream Work drains the same accepted command, actual original Scope/native and request credits are separate | a26 receives reports before work-result early exits. The separate `a-grpc-panic-cleanup` proposal also preserves simultaneous original work panic Box and cleanup sources, without inventing JoinError/TaskId. No new qualification for these inputs. |
| IPC event bridge and writer | `ipc/connection/events.rs`: existing bridge and writer handles, cached actual results | Cancel only signals stop; both real JoinHandles stay in EventJobs until consumed. Interrupting a join waiter does not lose either handle or an already-collected error | Implemented recovered EventJobs repair. New report metadata does not substitute for either actual join. |
| HTTP SSE and gRPC subscription worker | `http_adapter/sse.rs`; `grpc/{subscriptions,tasks}.rs`: actual worker slots and same Root/local frames/bytes | Real blocking worker join, output frame owner, stop/slow consumer behavior and original stream task/Scope retirement are distinct | Existing actual subscription owners remain. No restore of deleted owned flow-snapshot fallback. M owns actual event/replay/initial-snapshot forwarding. gRPC competing panic/drain delivery has a concrete followup above. |
| Commands: HTTP / IPC / gRPC / embedded NoCURRENT | Same Host command owner and canonical request rows; actual scoped registration, adopted PendingCommand, original ApplyObservation | Actual raw Tokio ID/outcome is not a fake TaskJoined; accepted Apply continues past the original 15-second wire timeout until the same actual operation joins and ACK is received | Existing concrete caller pair remains. No second control Host/book. Async Probe/Target/lookup/trace producers avoid nested blocking-pool execution. Ordinary error/result completion alone does not release native/Scope ownership. |
| CLI control / TUN / status / events / reload | `application::execute` installs one ClientAttempt and application-owned pure ControlBudget before all 13 concrete remote command callers; no daemon book borrowed | Actual Unix or Windows client provider installed before first await; real driver, native close, actual Scope/registration ACK and frame/DecodeFailure lifetime retained independently | Implemented paired client limits and raw/borrowed wire projection. Retired local copies release paid diagnostic payload after exact external delivery, while failed-driver observation stays failed. Reload document now uses the same Source bounded read/seek snapshot, not raw fs fallback. |
| Initial control TLS cert/key/CA | `application/run/control_material.rs` calls the existing actual ConfigurationSource with loaded `file_read_max_bytes` | Same OriginalFile close must match before bytes return; startup slot retains unknown Source | Implemented. Do not replace or rebuild this hot/initial material path. |
| Independent CLI connector/quota inspect | `application/inspect.rs`, `inspect/state.rs`, `connector/state.rs`, `outbox/replay.rs` | Same Source read/seek snapshot; actual inspection attempt keeps original resources, explicit matching close, and independent work/cleanup sources | Implemented. Streaming bounds and existing SQLite/JSONL report shapes remain. A library wrapper is not a fallback for missing production resources. |
| Principal quota lease and background persistence | Engine quota worker plus `configuration/source/quota.rs` | Original std handle and actual std join; same quota storage fault receiver; lease settlement and native close distinct. No actual Scope witness/ACK found at this worker spawn. | Native/fault pair installed and reported flag monotonic. Concrete Scope/global-task and snapshot/decoded-memory followups remain as above; do not reconstruct correct lease transactions. |
| Configuration Source persistence/recovery | Same ConfigurationSource retained by actual Proxy/Engine source transaction | Existing original FD matching close, SourceDisposition, compensation and revision gates remain separate | Current pretty and previous buffers are unpaid; full Source19 proposal frozen, not applied. Kernel writers must consume actual Host book/candidate; genuine Engine-only is not a Kernel fallback. |
| Early logging document and initial configuration read | `ConfigurationSource` same-inode bounded UTF8/facts/rewind reader; `main.rs::init_tracing_from_config` consumes the existing returned log recipe | Actual matching file close precedes document delivery; unknown retains the original Source. Runtime budgets and original serde duplicate/null/magic behavior are preserved | Implemented and M 74d four tests PASS, including original 20-case oracle, 8MiB+17 late-cap, close/UTF8 recovery and low-limit growth. Do not redo. |
| Ordinary process file logging | main `ProcessLogging` plus `ScopedTracing`; same ApplicationServices logger book and one actual per-output Scope job at a time | Closed controllers before IO; real worker/witness/start gate; idle and failed predecessors join+ACK before replacement; close/rotation/recovery on actual output worker; failed chunks and pure original fault sources stay paid | Complete pair applied by M b8d/d49; M reports nine actual Logger tests PASS on d49. Per-file isolation and old stderr/filter/JSON/WarningBridge behavior retained; actual live budget candidate and main shutdown consumers included. This does not qualify all application reload paths or whole G8. |

`CloseReport::is_complete` is only `unfinished.is_empty()`. Before a26 the
server helper returned Ok for a complete report and dropped failed task/RF rows.
The repaired receiver retains actual TaskId/class/source and exact RF, then
accepts that source's local receipt. This never acknowledges HostedScope exits
or creates native-close evidence. Physical retirement and typed return error
are now separate values. The followup `420cbcc8900cf7d7a2993682bf5a3154a05f9971` makes returned errors
keep an immutable projection of the actual task/RF sources, rather than a
mutable receiver. Returning that error through the same Scope cannot create a
receiver-to-itself strong cycle; exact source identity and local receipts remain.

The original tracing-appender backend was a real production gap, not a library
exception. The new proposal selects `ScopedTracing` in actual main; the old
`init_tracing` remains only a genuine library entry. It is never used when the
production Scope/controller is absent. No logger test/build qualification is
inferred from the separately passed initial-input or control-server results.

## DNS deletion gate remains active

Mature ordinary native/shared/framing, plain/mixed H3 and device sources are
installed; they must not all be labeled blocked on H3/TEMP. Default mixed DG
classification is M `c058c333a`, and actual existing software Source DNS callers
are covered by the three new test files applied as `a92775ac2`; neither has a
new Proxy runtime qualification from the old ELF. The supposed new shared-cell
Runtime projection was withdrawn: HostResources already receives exactly
ComponentSlot.packet_source through new_hosted_with_packet_source.

The actual remaining donor implementation gates are B F8 browser WS/XHTTP
Accepted standard Loan/cached receiver, B F7 mKCP/Hysteria-appearance owning
Factory, B F11 platform whole process tree, and the Mieru whole-configuration
mixed/shared-pool consumers. These are not permission to turn an otherwise
legal configured combination into a terminal rejection. B's forthcoming F8
Source may be a roleless explicit dependency instance rather than the VLESS
role instance; only the complete exact provider/original validator pair can
admit it. Source/root/current-query/canonical-observer gates stay exact.

DoQ.detour and non-Direct nondefault dial are explicitly rejected grammar, not
pending legal capabilities. The original outbound_dial test is the authority.
The mis-scoped A13 slice was withdrawn by `60446c75c`; old WG wire/Scope tests
were restored byte-for-byte, while B102 and legal mixed classification remain.

The old Kernel connector installation is inert after the actual native Source
early return; retaining it does not preserve advanced execution. Its installation,
adapter/cold fixtures, Session0 and `prepare_datagram_exchange` chain are deleted
together only after each legal resource,
shared pool and device donor has been consumed with its original pending,
LoanOwner, native/user and Scope/ACK evidence. Existing native Default/Node,
System tracked-blocking, persistent DoH and FakeIP journal paths are retained;
no second resolver/Source/pool is introduced to bypass a missing donor.

## Later exact-input verification

Coordinator input `9aa037bcaea80b88b31a785ce058985d8f65b92d` with lock
`576b313f0fdc9223aca8404dc0d8c4a3b838fe42b5dbdeccb3c5a7bc38dc6513`:
gRPC standalone `tasks::` eight actual tests passed, including panic/drain,
Scope/Unix reports and feedback-cycle tests. Input
`80b2db9e50bdd6371d3ba22c459b1b4355b022bf` with that same lock: minimal Root
status-api ELF passed seventeen actual tests (IPC reports four, HTTP shutdown
one, CLI wire nine, CLI Source document three). Those results qualify their
recorded input and features, not this whole table, all-feature production, or
Windows execution. Initial input subsequently passed its exact four-test 74d group. Logger
source has its own later exact d49 nine-test result above. The older 8/17
counts do not qualify it, nor does the Logger result qualify all frontends.
