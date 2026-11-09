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
