# A control callers: concrete retirement and remaining work

Source checkpoint: `a26d3955a46f49971b2f1e51cb7ac02eade2b8e4`.
This is a source/caller audit, not a whole-domain qualification. A did not run
Cargo, rustc or runtime tests. Coordinator results apply only to their recorded
source/lock/ELF. Windows execution and GUI behavior remain separate.

| Actual caller | Actual owner / admission | Native, task and delivery gates | Source status / remaining work |
| --- | --- | --- | --- |
| Windows IPC server and every accepted pipe | `ipc/windows.rs`, `windows/owner.rs`: same `Originals::Entry`, Connection plus PipeWorkspaceBytes paid before first open | Real ControlledPipeOwner raw Win32 handle; real std worker and Scope witness; raw peer JoinSet ID; native matching close, view return, actual join and registrationReleased before entry charge release | Implemented. No Tokio NamedPipeServer or regular-file fallback. `request_stop` retains an entry and invokes native wake outside the registry lock. Panic/drain source pair is in source. No Windows runtime qualification is inferred. |
| Unix IPC server / pending activation / accepted connections | `ipc/unix{,/server,/originals}.rs`: original listener and preaccept Connection charge, real raw JoinSet and original controls installed before activation | Same listener close receipt and path removal; peer actual join before accepted close; complete actual report independently allows accepted row removal | New a26 report receiver preserves complete-report task/RF faults and returns them to the same raw server/shutdown receiver. No new executor or second book. This source is not yet tested. |
| HTTP listener and connection workers | `http_adapter/{lifecycle,server,connections}.rs`: installed original listener, actual accepted graph and Scope | Original native close and Scope report completeness decide retirement; request Work/Apply and payload credits persist through original joins | New a26 same-handle report receiver strongly accepts original task/RF payloads before local receipts. History does not turn a positive native/Scope report into an unfinished report. Typed failure still returns. New tests not run. |
| gRPC listener, accepted H2 connections and streams | `grpc/{lib,native,messages/executor}.rs`: same original graph, actual stream TaskClass registration, Root and per-connection counts | Actual H2 stream Work drains the same accepted command, actual original Scope/native and request credits are separate | a26 receives reports before work-result early exits. The separate `a-grpc-panic-cleanup` proposal also preserves simultaneous original work panic Box and cleanup sources, without inventing JoinError/TaskId. No new qualification for these inputs. |
| IPC event bridge and writer | `ipc/connection/events.rs`: existing bridge and writer handles, cached actual results | Cancel only signals stop; both real JoinHandles stay in EventJobs until consumed. Interrupting a join waiter does not lose either handle or an already-collected error | Implemented recovered EventJobs repair. New report metadata does not substitute for either actual join. |
| HTTP SSE and gRPC subscription worker | `http_adapter/sse.rs`; `grpc/{subscriptions,tasks}.rs`: actual worker slots and same Root/local frames/bytes | Real blocking worker join, output frame owner, stop/slow consumer behavior and original stream task/Scope retirement are distinct | Existing actual subscription owners remain. No restore of deleted owned flow-snapshot fallback. M owns actual event/replay/initial-snapshot forwarding. gRPC competing panic/drain delivery has a concrete followup above. |
| Commands: HTTP / IPC / gRPC / embedded NoCURRENT | Same Host command owner and canonical request rows; actual scoped registration, adopted PendingCommand, original ApplyObservation | Actual raw Tokio ID/outcome is not a fake TaskJoined; accepted Apply continues past the original 15-second wire timeout until the same actual operation joins and ACK is received | Existing concrete caller pair remains. No second control Host/book. Async Probe/Target/lookup/trace producers avoid nested blocking-pool execution. Ordinary error/result completion alone does not release native/Scope ownership. |
| CLI control / TUN / status / events / reload | `application::execute` installs one ClientAttempt and application-owned pure ControlBudget before all 13 concrete remote command callers; no daemon book borrowed | Actual Unix or Windows client provider installed before first await; real driver, native close, actual Scope/registration ACK and frame/DecodeFailure lifetime retained independently | Implemented paired client limits and raw/borrowed wire projection. Retired local copies release paid diagnostic payload after exact external delivery, while failed-driver observation stays failed. Reload document now uses the same Source bounded read/seek snapshot, not raw fs fallback. |
| Initial control TLS cert/key/CA | `application/run/control_material.rs` calls the existing actual ConfigurationSource with loaded `file_read_max_bytes` | Same OriginalFile close must match before bytes return; startup slot retains unknown Source | Implemented. Do not replace or rebuild this hot/initial material path. |
| Independent CLI connector/quota inspect | `application/inspect.rs`, `inspect/state.rs`, `connector/state.rs`, `outbox/replay.rs` | Same Source read/seek snapshot; actual inspection attempt keeps original resources, explicit matching close, and independent work/cleanup sources | Implemented. Streaming bounds and existing SQLite/JSONL report shapes remain. A library wrapper is not a fallback for missing production resources. |
| Principal quota lease and background persistence | Engine quota worker plus `configuration/source/quota{,/worker}.rs` | Original std handle, actual worker join and ACK; same quota storage fault receiver; lease settlement and native close distinct | Implemented concrete pair. Worker reported flag is monotonic; delivered historical fault does not itself invalidate physical settlement. Do not reconstruct quota transactions. |
| Early logging document and initial configuration read | `main.rs::init_tracing_from_config`, `ConfigurationSource::{load_initial_configuration,read_initial_document}` | Original file ownership/explicit close already exists, but `io.read_text` still reads into unbounded String; early logging constructs whole Value and clones log subtree | Actual remaining implementation. Outside neutral fact probe is UNFROZEN and not accepted as completion: fixed-memory root/runtime budget facts must pair with same-inode bounded read and actual earlylog consumer/original serde oracle. Unknown facts may neither silently select 8MiB nor activate an unbounded fallback. CLI reload source pair is already separate and must not be redone. |

`CloseReport::is_complete` is only `unfinished.is_empty()`. Before a26 the
server helper returned Ok for a complete report and dropped failed task/RF rows.
The repaired receiver retains actual TaskId/class/source and exact RF, then
accepts that source's local receipt. This never acknowledges HostedScope exits
or creates native-close evidence. Physical retirement and typed return error
are now separate values. The followup `420cbcc8900cf7d7a2993682bf5a3154a05f9971` makes returned errors
keep an immutable projection of the actual task/RF sources, rather than a
mutable receiver. Returning that error through the same Scope cannot create a
receiver-to-itself strong cycle; exact source identity and local receipts remain.

The bootstrap `zero_logging::init_tracing` backend is a distinct ordinary production
caller in `main.rs`: it creates RotatingWriter fs files and tracing-appender workers
and retains WorkerGuard for process lifetime. This document does not claim
those handles are controlled Source/Scope originals, or reclassify the
early input fix as a logger-provider migration. No logging backend ownership
rewrite is included in the bounded-document proposal.

## DNS deletion gate remains active

B/C have not supplied new complete advanced/TEMP donor qualification. B's
ordinary QUIC Factory is being split from H3 and still needs actual
ControlledMetadataCarrier + ControlledServiceDatagram classification/transfer
and ordinary native/borrowed wire. The real two Kernel PacketPath(None)
callers are in the B/M concrete datagram/payload-custody pair, not standalone
libraries. C's complete TEMP/source code and Mac helper are not evidence for an
arbitrary SIP whole job or for full ordinary TEMP qualification.

Therefore the remaining actual `ProxyDnsOutboundConnector`, detour Session0 and
`prepare_datagram_exchange` callers must remain until each legal resource,
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
Windows execution. Early logging/initial input and logger ownership above
remain open implementation duties.
