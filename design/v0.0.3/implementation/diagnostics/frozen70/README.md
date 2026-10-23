# Frozen rounds 70 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 70 | `59fc25092dc7` | fmt | 0 | compile/static only or not run |
| 70 | `59fc25092dc7` | workspace-alltargets-check | 0 | compile/static only or not run |
| 70 | `59fc25092dc7` | raw-tcp-ingress | 101 | [0, 1, 0] |
| 70 | `59fc25092dc7` | platform-unit | 0 | [258, 0, 11] |
| 70 | `59fc25092dc7` | transport-library | 0 | [224, 0, 1] |
| 70 | `59fc25092dc7` | dns | 0 | [74, 0, 0] |
| 70 | `59fc25092dc7` | proxy-library | 101 | [809, 5, 6] |
| 70 | `59fc25092dc7` | test-layout | 0 | compile/static only or not run |
| 70 | `59fc25092dc7` | platform-contracts-all | 0 | [82, 0, 0] |
| 70 | `59fc25092dc7` | windows-platform | 0 | compile/static only or not run |
| 70 | `59fc25092dc7` | windows-tun | 0 | compile/static only or not run |
| 70 | `59fc25092dc7` | source-runtime-guard | 0 | [189, 0, 0] |
| 70 | `59fc25092dc7` | source-architecture-guard | 0 | [26, 0, 0] |
| 70 | `59fc25092dc7` | strict-platform-stack | 101 | compile/static only or not run |

Round70 passed all-feature/all-target compilation and actual Platform258/0/11, Transport224/0/1, DNS74/0/0 and platform contracts82/0/0. Actual Proxy809/5/6 and exact rawTCP0/1 failed. Strict Platform/Stack Clippy failed the new root UDP error size. Full original output and ignored tests remain recorded; failures are not converted into successes by later fixes.

The raw TCP fixture reached the new typed OS-source assertion but did not yet establish its native ConnectionRefused cause. The Direct failure was the rejected-call retained-slot assertion, not a byte-meter assertion. Two listener replacement regressions and the actual WG witness fixture failed separately. Windows results are compile-only cross-target checks. No final ALL17 qualification is implied.
