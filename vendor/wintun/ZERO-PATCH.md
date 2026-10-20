# Pinned Wintun explicit session close

Vendored `wintun` 0.4.0, original crate checksum and immutable VCS commit in
`UPSTREAM.json`; original MIT license retained byte for byte. No protocol or
Wintun DLL version changes, and no DLLs are included in this copy. The existing
normalized package manifest and dependencies are preserved.

The patch adds a sealed per-actual-session identity and consuming
`Session::close_owned`, explicitly calling the original `WintunEndSession` before
issuing an `EndSessionReceipt`. The function has no native failure return; its
receipt reports invocation completion, without inventing a Windows success code.
The original shutdown event is explicitly closed too. Its original typed Windows
error and the ended-session receipt are retained together on event close failure.
Drop skips handles already consumed by explicit close and remains fallback only.

Callers must stop every packet user and join the original receive thread before
consuming the unique Session. A receipt is not a task/thread join, adapter close,
acquisition-failure classification, or permission to start a replacement.

The existing `Adapter::start_session` acquisition gap is **not fixed here**:
CreateEventA can fail after WintunStartSession, before Session is returned. A
future persistent acquisition owner must install the original provisional Session
before event creation/other fallible setup; its failure cannot be called NoAcquire.
The Zero existing-Wintun backend is not yet switched to the new close method.

Integration changes the resolved package source from registry to this path, with
same version and dependencies. The root build owner updates the ignored frozen
Cargo.lock; no Cargo command or lock mutation was performed by this patch author.
