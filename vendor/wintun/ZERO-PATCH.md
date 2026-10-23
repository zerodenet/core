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

`Adapter::start_session` now installs the original provisional Session immediately
following a successful WintunStartSession. If CreateEventA fails, it explicitly
ends that same original before returning the original typed Windows error together
with the sealed EndSessionReceipt (inside `Error::Io`, downcastable as `SessionStartError`). No event was
acquired on this branch. This is an acquired-and-ended session, never NoAcquire;
Drop sees the consumed native handle and cannot end it twice. This failure branch
has not yet been exercised on native Windows. Cross-compilation alone cannot
qualify the actual Windows API behavior.
The Zero existing-Wintun backend is not yet switched to the new close method.

Integration changes the resolved package source from registry to this path, with
same version and dependencies. The root build owner updates the ignored frozen
Cargo.lock; no Cargo command or lock mutation was performed by this patch author.
