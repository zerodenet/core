# Round2 candidate lock

`candidate.Cargo.lock` is the exact lock used by M's Rust 1.99.0 verification.
It is an input fixture, not a generated log, cache or release artifact. Keep its
SHA-256 file and the external candidate evidence archive consistent whenever
workspace manifests require a lock change; do not upgrade protocol baselines.

Only PRs targeting `codex/v003-cloud-r2-integration` and explicit round2 refs
use this fixture and Rust 1.99.0 in CI. They check out the actual PR head rather
than a synthetic merge revision, preserve the selected checks and deadlines,
and upload per-job source/lock provenance even after failure. CI rejects an
existing different lock and any mutation of the validation lock. Other refs
keep their existing merge-revision/stable behavior.

The input artifact alone is not qualification. Retain the same workflow run's
raw logs, actual commands and job outcomes, including failures, cancellation,
ignored tests and selected checks that did not execute. Native platform and
privileged-environment results remain separate from Linux compilation.
