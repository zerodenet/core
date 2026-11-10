# C route File originals: unactivated handoff

Base: `96f85f97504ca0f71ef00e5e473ade8519e03b20`.
This commit saves the complete C SDK26 producer/three-platform consumer source,
then the two owning-Debug fixes. No Cargo, test or platform execution was run.
The snapshot is not activation or completion evidence.

Composition inputs:

- SDK26 patch SHA256 `f61384bfd729092a65812cea38bb94f65e7253e5f4c95fc6a5da3e938ff7407b`.
- Debug2 overlay SHA256 `dcaa56af1bf81601725b4e5a1bbabbc19393ccd2b597cd200c5aecf2840b0bc9`.
- Exact composed source tree, before this note: `99731911244fc4887330a61513799f94fe627685`.

The required M5 Proxy consumer remains M-owned and is deliberately absent:
`inbound/tun/routes/owned/{native,memory,diagnostics,worker}.rs` and `owned.rs`.
It prepays the single Source diagnostics payload, splits the existing credit
into `RouteFileFaults::from_prepaid`, stores each Family pending original, and
hands all original causes to the real Source cleanup receiver. Do not activate
this SDK snapshot separately.

The saved SDK stages the exact family lease before native open/flock and other
fallible preparation; moves it into the real journal only after preparation;
retains Linux/Windows journal transient File originals in that same family;
and reuses the Mac Session's actual family/transient providers. Unknown close
keeps the actual original identity and does not retry its syscall. Error history
and native closure remain independent. Six supplied regressions are unexecuted.

Still to reconcile before qualification: metadata pause spanning StopOld may
block new journal read/write/encode admission during cleanup. Direct descriptor
close and error archival consume only their already-paid cells, but journal
persist still allocates through existing Root admission. Verify the actual
transaction pause lifetime and complete reusable cleanup storage if required;
do not bypass Root policy or treat a historical error as native-not-closed.

Mac bootstrap/path/argv/helper-decoder owning changes were only investigated;
no new source for them existed at interruption, so none was invented here.
Remaining concrete G2/G3 owners and G5 caller deletion are not declared complete.
The existing TEMP physical exception is unchanged. No new Root, Scope, worker,
provider authority, configuration cap/default or timeout is introduced.
