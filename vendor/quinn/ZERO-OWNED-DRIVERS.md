# Exact Quinn driver result hook

This is crates.io `quinn` **0.11.9**, unchanged package version/features/MSRV.
Archive SHA256: `b9e20a958963c291dc322d98411f541009df2ced7b5a4f2bd52337638cfccf20`.
Packaged VCS: `b2b930a0662b18b2e351264a21e175478bb3c3f1`, path `quinn`.
Original MIT and Apache-2.0 licenses and `.cargo_vcs_info.json` are retained.
The packaged Cargo.lock is omitted, as with the workspace's other pinned vendors.

Only two actual driver spawn sites change: `Endpoint::new_with_abstract_socket`
in `src/endpoint.rs` and `Connecting::new` in `src/connection.rs`. They call the
new default `Runtime::spawn_driver` with an explicit origin and their original
I/O result. The connection site also reads its same driver's original typed
close reason, without a new protocol reference. The default hook retains the
existing `spawn`, tracing span and the two original I/O-error logging messages.
Existing Runtime implementers require no change. No new task, socket or runtime
is introduced by the default hook.

An override can register actual driver tasks with its existing host registrar.
DriverExit is a body result: it is neither TaskExit/join, native I/O closure, nor
proof that the application received or acknowledged a connection failure. Those
receipts remain independent. There is no production activation in this patch.
