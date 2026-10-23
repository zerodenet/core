# Local Hyper patch scope

The vendored package remains Hyper **1.9.0**, under its original MIT license.
The workspace already resolves it through `[patch.crates-io]`. This source patch
adds no dependency, version change, lockfile change, runtime, or protocol feature.

`client::conn::http2::Builder::connection_error_observer` optionally installs a
synchronous typed callback. `proto/h2/client.rs::ConnMapErr` gives that callback
the owned `h2::Error` it was about to discard to `()`. The original log, future
output, execution model and default observer-None behavior remain. The callback
must not block or panic; its owner is responsible for bounded storage.

This observes connection failure only, not ordinary per-stream resets. Zero's
managed H2 aggregate records the first actual h2 object and wakes that connection's
waiters. It does not manufacture a Hyper error, alter h2's lower source conversion,
replace an executor, authorize new I/O or prove task/native closure. Legacy
transport call sites do not install the observer.
