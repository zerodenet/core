# Zero repository documentation

Public user, configuration, protocol, and control-plane documentation is maintained
in the [ZeroDeNet documentation repository](https://github.com/zerodenet/docs) and
published at <https://docs.zerodenet.org/projects/core/>.

This directory contains repository-owned engineering material only:

- `project/`: architecture rules, implementation boundaries, design notes, and
  project history.
- `protocols/`: protocol implementation notes used while changing protocol crates.
- `control-plane/`: historical control-plane designs; these are not public API
  contracts.
- `testing/`: focused test plans and verification records.

The release compatibility ledger lives in
[`release/breaking-changes.md`](../release/breaking-changes.md). Do not restore a
VitePress project, public documentation copies, or deployment workflow here.
