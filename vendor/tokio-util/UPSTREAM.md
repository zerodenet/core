# Upstream

This directory preserves the crates.io `tokio-util` 0.7.18 source and license.
Upstream commit: `9cc02cc88d083113cd9889a74b382e39e430e180`, subdirectory
[`tokio-util`](https://github.com/tokio-rs/tokio/tree/9cc02cc88d083113cd9889a74b382e39e430e180/tokio-util).

Local change: cancellation-tree traversal records its actual cancelled nodes
and delivers notifications after releasing all ancestor locks. A waker panic
cannot poison the cancellation tree; the remaining notifications are delivered
before the original first panic resumes. No native I/O or task-join evidence is
created by this change.

Regressions run in the Zero workspace's platform registrar tests and proxy
monitor cancellation callback test. This patch does not change the upstream
package version or the protocol implementation baselines.
