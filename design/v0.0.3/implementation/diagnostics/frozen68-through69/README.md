# Frozen rounds 68–69 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 68 | `3f6e553b35e0` | fmt | 0 | compile/static only or not run |
| 68 | `3f6e553b35e0` | workspace-alltargets-check | 101 | compile/static only or not run |
| 69 | `a01856471e6c` | fmt | 0 | compile/static only or not run |
| 69 | `a01856471e6c` | workspace-alltargets-check | 101 | compile/static only or not run |

Both rounds stopped before runtime after actual workspace/all-target compilation failures. Round68 failed a new carrier owner visibility access. Round69 failed a root provider generation getter access and three new fixture import/path/ownership issues. No runtime counts or successful tests are inferred. Original fmt successes, complete compiler output, source/tree/lock digests and frozen flags are preserved.

The fixes are separate later commits; neither these compile-only rounds nor earlier Windows cross-target checks establish final ALL17 qualification. Earlier independent archives remain unchanged.
