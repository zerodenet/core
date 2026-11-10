# M route File consumer handoff — source only

This commit preserves the exact five-file warehouse draft, paired with C's SDK
`9114022f8d51f922ba0a4d47c0eaed384889e164`. It is WIP, not a verified candidate.
No Cargo, runtime test, privileged TUN operation or network modification ran for
this preservation. The original warehouse and exact before/after proof remain.

- `owned/native.rs`: each actual Family owns a persistent RouteFileOriginals
  before install; retry first retires its same pending originals; final close
  retains the guard until close succeeds and also retires the staged originals.
- `owned/memory.rs`: the same original prepaid metadata credit covers the new
  SDK payload and finite diagnostic wrappers; no second reserve/book is added.
- `owned/diagnostics.rs`: pure fixed operation/cleanup/File cause custody plus
  the existing prepaid CleanupFailureReceipt and RetainedError contracts.
  Receipt acknowledgement proves local error delivery only, never native close,
  Scope joins, caller return or Host registration release.
- `owned/worker.rs` and `owned.rs`: the actual blocking worker and existing
  Source shutdown forward that original diagnostic; accepted historical errors
  do not substitute for or permanently deny actual native completion.

Required completion before integration/qualification:

1. Typecheck the complete SDK/consumer pair; none of these five files was built.
2. Audit all cleanup/error branches, including Mac helper early failures, and
   guard against a route worker panic retaining originals without a surviving
   cleanup continuation in the same original responsibility. This was identified
   but not implemented; do not interpret a joined/panicked worker as native close.
3. Resolve metadata-pause progress for actual journal encode/read/write during
   StopOld. Preserve old native originals; do not relax admission or invent a
   NativeClosed receipt from empty slots or historical errors.
4. Exercise exact FD/lock/transient close, failed candidate retention, actual
   worker/monitor joins and Source acknowledgement. Keep existing deadlines,
   capacities, production stack and rollback/TCP-continuity assertions.
5. Finish remaining Snapshot/query/helper/bootstrap/process ownership and the
   applicable configured budgets. This pair does not close C, G8 or Z003-07.

Do not merge to develop/main, deploy or publish. Final qualification still needs
one frozen implementation, same lock, CI and downloadable original evidence.
