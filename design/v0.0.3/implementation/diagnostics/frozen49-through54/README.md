# Frozen rounds 49–54 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 49 | `6af5228dcb8e` | fmt | 0 | compile/static only or not run |
| 49 | `6af5228dcb8e` | workspace-alltargets-check | 101 | compile/static only or not run |
| 50 | `61f01a2b260a` | fmt | 0 | compile/static only or not run |
| 50 | `61f01a2b260a` | platform-unit | 0 | [157, 0, 11] |
| 50 | `61f01a2b260a` | proxy-library | 0 | [679, 0, 6] |
| 50 | `61f01a2b260a` | workspace-alltargets-check | 0 | compile/static only or not run |
| 50 | `61f01a2b260a` | test-layout | 0 | compile/static only or not run |
| 50 | `61f01a2b260a` | platform-contracts-all | 0 | [82, 0, 0] |
| 50 | `61f01a2b260a` | windows-platform | 0 | compile/static only or not run |
| 50 | `61f01a2b260a` | transport-library | 0 | [200, 0, 1] |
| 50 | `61f01a2b260a` | vmess-library | 0 | [27, 0, 0] |
| 50 | `61f01a2b260a` | source-runtime-guard | 0 | [189, 0, 0] |
| 50 | `61f01a2b260a` | source-architecture-guard | 0 | [26, 0, 0] |
| 50 | `61f01a2b260a` | strict-platform-stack | 101 | compile/static only or not run |
| 51 | `f64b490cc3b8` | fmt | 0 | compile/static only or not run |
| 51 | `f64b490cc3b8` | platform-unit | 101 | compile/static only or not run |
| 51 | `f64b490cc3b8` | tun-library | 0 | [38, 0, 1] |
| 51 | `f64b490cc3b8` | stack-library | 0 | [36, 0, 1] |
| 51 | `f64b490cc3b8` | proxy-library | 101 | compile/static only or not run |
| 51 | `f64b490cc3b8` | workspace-alltargets-check | 101 | compile/static only or not run |
| 52 | `19913b10f396` | fmt | 0 | compile/static only or not run |
| 52 | `19913b10f396` | platform-unit | 0 | [174, 0, 11] |
| 52 | `19913b10f396` | engine-library | 0 | [50, 0, 0] |
| 52 | `19913b10f396` | tun-library | 0 | [38, 0, 1] |
| 52 | `19913b10f396` | stack-library | 0 | [40, 0, 1] |
| 52 | `19913b10f396` | proxy-library | 101 | compile/static only or not run |
| 52 | `19913b10f396` | workspace-alltargets-check | 101 | compile/static only or not run |
| 53 | `4f367b5fea86` | fmt | 0 | compile/static only or not run |
| 53 | `4f367b5fea86` | platform-unit | 101 | compile/static only or not run |
| 53 | `4f367b5fea86` | engine-library | 0 | [50, 0, 0] |
| 53 | `4f367b5fea86` | tun-library | 0 | [41, 0, 2] |
| 53 | `4f367b5fea86` | stack-library | 0 | [40, 0, 1] |
| 53 | `4f367b5fea86` | proxy-library | 101 | compile/static only or not run |
| 53 | `4f367b5fea86` | workspace-alltargets-check | 101 | compile/static only or not run |
| 54 | `203c65e109e2` | fmt | 0 | compile/static only or not run |
| 54 | `203c65e109e2` | platform-unit | 101 | [202, 1, 11] |
| 54 | `203c65e109e2` | engine-library | 0 | [50, 0, 0] |
| 54 | `203c65e109e2` | tun-library | 0 | [41, 0, 2] |
| 54 | `203c65e109e2` | stack-library | 0 | [40, 0, 1] |
| 54 | `203c65e109e2` | transport-library | 101 | compile/static only or not run |
| 54 | `203c65e109e2` | proxy-library | 101 | compile/static only or not run |
| 54 | `203c65e109e2` | workspace-alltargets-check | 101 | compile/static only or not run |
| 53-windows-tun | `4f367b5fea86` | windows-tun | 0 | compile/static only or not run |

Round51 platform and round54 transport aborted with SIGABRT after actual panic/destructor failures. They have no aggregate test result; partial printed passing tests are not counted. Compile failures and not-run scopes remain explicit. Round53 supplemental Windows TUN is cross-compilation only, with no native Windows execution.

The round53 lock change replaces exact quinn0.11.9 and wintun0.4.0 registry sources with the same-version vendor paths. No package version upgrade is implied. Source snapshot and exact lock diff are retained. Earlier evidence archives remain unchanged.
