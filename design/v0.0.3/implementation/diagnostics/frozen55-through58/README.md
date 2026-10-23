# Frozen rounds 55–58 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 55 | `13b67b1e731a` | fmt | 0 | compile/static only or not run |
| 55 | `13b67b1e731a` | platform-unit | 0 | [203, 0, 11] |
| 55 | `13b67b1e731a` | proxy-library | 101 | [713, 3, 6] |
| 55 | `13b67b1e731a` | workspace-alltargets-check | 0 | compile/static only or not run |
| 55 | `13b67b1e731a` | test-layout | 0 | compile/static only or not run |
| 55 | `13b67b1e731a` | platform-contracts-all | 0 | [82, 0, 0] |
| 55 | `13b67b1e731a` | windows-platform | 0 | compile/static only or not run |
| 55 | `13b67b1e731a` | vmess-library | 0 | [27, 0, 0] |
| 55 | `13b67b1e731a` | source-runtime-guard | 0 | [189, 0, 0] |
| 55 | `13b67b1e731a` | source-architecture-guard | 0 | [26, 0, 0] |
| 55 | `13b67b1e731a` | strict-platform-stack | 101 | compile/static only or not run |
| 56 | `4dd116d2068b` | fmt | 0 | compile/static only or not run |
| 56 | `4dd116d2068b` | quinn-owned-hosted | 0 | [1, 0, 0] |
| 56 | `4dd116d2068b` | platform-unit | 0 | [205, 0, 11] |
| 56 | `4dd116d2068b` | transport-library | 0 | [214, 0, 1] |
| 56 | `4dd116d2068b` | proxy-library | 101 | compile/static only or not run |
| 56 | `4dd116d2068b` | workspace-alltargets-check | 101 | compile/static only or not run |
| 57 | `091022a77054` | fmt | 0 | compile/static only or not run |
| 57 | `091022a77054` | platform-unit | 0 | [211, 0, 11] |
| 57 | `091022a77054` | transport-library | 0 | [218, 0, 1] |
| 57 | `091022a77054` | proxy-library | 101 | compile/static only or not run |
| 57 | `091022a77054` | workspace-alltargets-check | 101 | compile/static only or not run |
| 58 | `34532f62e6cd` | fmt | 0 | compile/static only or not run |
| 58 | `34532f62e6cd` | platform-unit | 0 | [212, 0, 11] |
| 58 | `34532f62e6cd` | transport-library | 0 | [219, 0, 1] |
| 58 | `34532f62e6cd` | proxy-library | 101 | [751, 9, 6] |
| 58 | `34532f62e6cd` | workspace-alltargets-check | 0 | compile/static only or not run |
| 58 | `34532f62e6cd` | test-layout | 0 | compile/static only or not run |
| 58 | `34532f62e6cd` | platform-contracts-all | 0 | [82, 0, 0] |
| 58 | `34532f62e6cd` | windows-platform | 0 | compile/static only or not run |
| 58 | `34532f62e6cd` | vmess-library | 0 | [27, 0, 0] |
| 58 | `34532f62e6cd` | source-runtime-guard | 101 | [188, 1, 0] |
| 58 | `34532f62e6cd` | source-architecture-guard | 0 | [26, 0, 0] |
| 58 | `34532f62e6cd` | strict-platform-stack | 101 | compile/static only or not run |
| 57-preflight | `45d3fa2fed8a` | fmt | 1 | compile/static only or not run |
| 57-preflight2 | `478887826471` | fmt | 0 | compile/static only or not run |
| 57-preflight2 | `478887826471` | proxy-check | 101 | compile/static only or not run |
| 57-preflight3 | `1e157255bf45` | fmt | 0 | compile/static only or not run |
| 57-preflight3 | `1e157255bf45` | proxy-check | 101 | compile/static only or not run |
| 57-preflight4 | `1b8c89753153` | fmt | 0 | compile/static only or not run |
| 57-preflight4 | `1b8c89753153` | proxy-check | 0 | compile/static only or not run |

Round57 preflight1 failed formatting/module resolution before any Cargo compilation. Preflights2/3 recorded the actual non-Send future errors; preflight4 passed library compilation only. Whole round57 later failed a distinct new VMess fixture moved-value check. These results remain separate.

Round58 actual Proxy runtime result is 751 passed / 9 failed / 6 ignored. Runtime source guard is 188 / 1 and strict platform checks failed three oversized error-return lints. No subset or compile success provides global ALL17 qualification. The old native ICMP proofs and earlier archives are unchanged.
