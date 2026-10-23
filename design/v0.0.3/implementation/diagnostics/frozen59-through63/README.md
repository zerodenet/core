# Frozen rounds 59–63 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 59 | `56a14a0250f9` | fmt | 0 | compile/static only or not run |
| 59 | `56a14a0250f9` | platform-unit | 101 | compile/static only or not run |
| 59 | `56a14a0250f9` | transport-library | 101 | compile/static only or not run |
| 59 | `56a14a0250f9` | proxy-library | 101 | compile/static only or not run |
| 59 | `56a14a0250f9` | workspace-alltargets-check | 101 | compile/static only or not run |
| 60 | `dd375e146fa7` | fmt | 0 | compile/static only or not run |
| 60 | `dd375e146fa7` | platform-unit | 101 | compile/static only or not run |
| 60 | `dd375e146fa7` | transport-library | 101 | compile/static only or not run |
| 60 | `dd375e146fa7` | proxy-library | 101 | compile/static only or not run |
| 60 | `dd375e146fa7` | workspace-alltargets-check | 101 | compile/static only or not run |
| 61 | `9b4557790e22` | fmt | 0 | compile/static only or not run |
| 61 | `9b4557790e22` | platform-unit | 0 | [224, 0, 11] |
| 61 | `9b4557790e22` | transport-library | 101 | compile/static only or not run |
| 61 | `9b4557790e22` | proxy-library | 101 | compile/static only or not run |
| 61 | `9b4557790e22` | workspace-alltargets-check | 101 | compile/static only or not run |
| 62 | `a8d6c548db58` | fmt | 0 | compile/static only or not run |
| 62 | `a8d6c548db58` | platform-unit | 0 | [225, 0, 11] |
| 62 | `a8d6c548db58` | transport-library | 0 | [221, 0, 1] |
| 62 | `a8d6c548db58` | proxy-library | 101 | compile/static only or not run |
| 62 | `a8d6c548db58` | workspace-alltargets-check | 101 | compile/static only or not run |
| 63 | `efa787d8cd85` | fmt | 0 | compile/static only or not run |
| 63 | `efa787d8cd85` | workspace-alltargets-check | 0 | compile/static only or not run |
| 63 | `efa787d8cd85` | proxy-library | 101 | [768, 3, 6] |
| 63 | `efa787d8cd85` | test-layout | 0 | compile/static only or not run |
| 63 | `efa787d8cd85` | source-runtime-guard | 0 | [189, 0, 0] |
| 63 | `efa787d8cd85` | source-architecture-guard | 0 | [26, 0, 0] |
| 62-strict | `e5e4962ba75e` | fmt | 0 | compile/static only or not run |
| 62-strict | `e5e4962ba75e` | strict-platform-stack | 0 | compile/static only or not run |

Rounds59/60 failed new metadata acquisition evidence fixture compilation. Rounds61/62 passed their actual platform tests but Proxy compilation remained blocked by cross-crate literals and then the canonical fixture Arc type. Original compile failures remain archived with null test counts; their absence of runtime is not a successful test result.

Round62-strict is only platform/stack strict Clippy, separate from the broad round62. Its success does not qualify workspace Clippy. Earlier native/aborted/failure archives remain unchanged. Final ALL17 qualification remains incomplete.
