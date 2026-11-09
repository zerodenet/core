# A recovery after the execution snapshot rollback

Recovered source base: `bd51dc4a2c8bee3985161e14a8d2dc8eac2d3ff7`.
The old unpushed worktrees, proposals, logs and ELF artifacts were lost. Historical
results are not qualification of these reconstructed sources. All Cargo remains
coordinator M's serial responsibility.

Private recovery commits `c64b251f1` and `819e57b6f` restore IPC EventJobs ownership:
each original JoinHandle remains stored until actual Ready, and each result Arc
remains stored while awaiting its sibling. Cancellation cannot turn an already
collected error or a still-running child into an empty, successful cleanup.
The real Pending TCP/blocked worker regression retains the original five-second
limit and compares actual task identities and original error Arcs.

M integrated the recovery plus the two public Pipe repairs and the typed original
identity fixture as `d45782548af2ca25b2b3865b9078778a3efee337`. This checkpoint does
not claim their runtime/platform qualification.

| Remaining actual responsibility | Required original owner / deletion boundary |
| --- | --- |
| CLI control/tun/status IPC clients | Replace Drop-only std UnixStream/File and unbounded lines with the actual application ClientAttempt, original native control and real Scope/worker joins. Use a client-owned existing ControlBudget book, never the daemon's book or another Host. |
| Client input/output frame storage | Optional global `--control-client-max-frame-bytes` and `--control-client-total-max-bytes`; omitted defaults from existing ControlMessageLimits. Validate `1 <= frame <= total <= 256 MiB`, pay before allocation, retain actual capacity and all held copies. |
| Dynamic JSON and output metadata | Borrow the same original frame/RawValue; arrays stream without whole Value materialization. Measure actual concurrent object-index/owned-string/parser storage before allocation. TUN projection pays its concrete typed vectors. The same-version `serde_json/raw_value` feature is a necessary M-owned manifest proposal. |
| Server request decoding | Existing `reserve_decoded(wire.len())` does not establish complete dynamic DTO/Vec metadata accounting. This is still implementation work; describing envelope bytes as non-RSS does not waive retained copies. |
| Client cancellation/recovery | Stop business, keep the same actual driver/IO task/native original, consume actual joins and ACK before replacement. Preserve original raw JoinError separately from real Scope TaskJoined. Reuse the original observer deadline through structured cleanup; a new observation never rewrites an unknown original as closed. |
| Known control query frontends | Recheck actual borrowed Engine/Proxy projections for HTTP/IPC/gRPC; do not restore deleted production OwnedDTO fallback handlers. Genuine QueryService/EventSource/library APIs retain their actual standalone callers. |
| TLS/quota/inspect/persistence | Existing ConfigurationSource/OriginalFile, original stdworker/lease, bounded reader and strong typed fault receiver already exist. Recheck their restored callers; do not rebuild the Source transaction or create another Host. |
| Full DNS old-chain deletion | Existing Default/Node native query sources, System blocking Scope, persistent DoH, FakeIP journal and actual foreign-capsule receiver remain. Delete Session0/ProxyDnsOutboundConnector/prepare_datagram_exchange only after every legal remaining B/C donor and actual caller has migrated. |
| DNS donor prerequisites | Advanced QUIC/XHTTP/H3/REALITY, Mieru cross-executor/mixed, two real Kernel PacketPath(None) borrowers and full TEMP qualification remain explicit implementation/qualification gates, not genuine-library exemptions. |
| Platform qualifications | Real Windows execution/GUI and constrained-host pressure remain separate. Darwin arbitrary SIP whole-job authority is a known implementation blocker; it cannot be represented by direct-child/PGID/EOF evidence. |

CLI wire and default command output, the daemon's original 15-second accepted
Apply reply semantics, control ACK/revision/events/identity and settlement remain
authoritative. Client connect/read gain no arbitrary observer timeout or retry.
Windows client acquisition must be a real named-pipe provider, not a regular-file
ConfigurationSource grant. No second Runtime, resolver, pool or generic executor
is introduced.

## Reconstructed control input checkpoint

The client migration is now implemented by the complete ClientAttempt/native
provider/Scope pair (M integration `81097b687` and later fault handoff repairs).
One actual application-owned ControlBudget is reused through recovery. Original
connect/read have no new deadline; the native, raw driver, worker Scope and host
registration acknowledgements remain independent. Retired local copies release
paid data only after a real external fault receiver accepts it; a delivered
failed task remains a failed task instead of becoming `Ok(())`.

Fresh coordinator evidence at `42da3030f7643914a272ef7c7f1b9e20ca01c1b2`
(records under `/workspace/r2-cont-evidence/recovery-42da-*`) reports CLI9,
HTTP input4, owned command decoding2 and original event-child1 passing, plus the
Root all-feature binary check. Its exact lock is
`e531d45444635b84e30795e5b09703c65f440a6abbdad03f75334356f635fd74`.
Those results do not qualify subsequent sources or Windows execution. A later
private `25a293e3c` adds the malformed, unterminated escaped-prefix scratch repair
and a real held DecodeFailure regression; it has not been executed by A.

| Frozen subsequent pair | Actual owner and current status |
| --- | --- |
| Server decoded JSON, 30 files; patch SHA256 `4e82cb0c436ec7c845b2efcddeabffff01ad1c099827a12837fc6fbe388f490b` | Outside proposal `a-server-decoded`, exact base `7250cc3b5`, M-only apply. IPC borrowed fixed envelope removes the intermediate Content tree. HTTP HeldRequest and gRPC Record prepay actual tree/Content/typed vectors before the original serde result producer, retaining input through original Work/join/ACK. Pure runtime measurement creates no decoder/Scope/IO authority. The synchronous rejection path retains original input until its actual unstarted DTO/future is freed. Rustfmt and apply-check only, no build or test qualification. |
| Canonical configuration String, 5 files; patch SHA256 `141ac5eff8007e5a95aff21bd4d98de9de1dbd1bd97737c8bfce78dc5a999f74` | Outside proposal `a-config-canonical-input`, M-only apply. Proxy ConfigApply/Runtime and ConfigValidate count immutable Value output before allocating from their original Host book. Bounded serialization preserves original bytes. The original Engine validation body still uses the same source, error mapping and routing material checks. It counts only temporary canonical document storage, not an AST or native-close receipt. |

Both outside pairs include exact input/output file manifests and their actual
production consumers. Source construction facts for JSON capacity are kept in
outside evidence (Rust1.99.0 BTreeMap nodes, serde1.0.228 Content, requested Vec
capacity/overlap); archive/license checks do not imply runtime qualification.
RawValue magic-token interpretation, duplicate/null/first-key behavior and
original serde errors are tested against the actual former decoder. Capacity
measurement does not replace that decoder with a different JSON protocol.

The post-decode candidate is long-lived core state: actual Apply captures
RuntimeConfig, and the immutable Engine snapshot and its plan/borrowers retain
its configuration Arc beyond the request. A request's 15-second return is not
its storage lifetime. A blanket permit field shared by deep `RuntimeConfig`
clones or a permit beside a snapshot whose public config Arc escapes would not
account the actual copies; no such shortcut is implemented or claimed. The
early logging whole-Value projection and concrete normalization/materialization
storage remain separately identified work. Existing file/source/resource and
control request bounds retain their documented units; they are not a process
RSS allocator or a second configuration pool.

B/C reconfirmed no new complete advanced or TEMP donor qualification on this
checkpoint. Two actual Kernel PacketPath(None) borrowers are in B/M's complete
pending producer/consumer pair, not standalone-library exemptions. Therefore
`ProxyDnsOutboundConnector`, detour Session0 and `prepare_datagram_exchange`
retain their remaining callers until the real legal paths and their original
native/user/Scope/ACK receipts are complete. This control checkpoint does not
close the DNS domain.
