# Frozen rounds 64–67 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 64 | `a7171bb14149` | fmt | 0 | compile/static only or not run |
| 64 | `a7171bb14149` | workspace-alltargets-check | 101 | compile/static only or not run |
| 65 | `08da84c30fc1` | fmt | 0 | compile/static only or not run |
| 65 | `08da84c30fc1` | platform-unit | 101 | [237, 1, 11] |
| 65 | `08da84c30fc1` | transport-library | 0 | [222, 0, 1] |
| 65 | `08da84c30fc1` | dns | 0 | [68, 0, 0] |
| 65 | `08da84c30fc1` | workspace-alltargets-check | 0 | compile/static only or not run |
| 65 | `08da84c30fc1` | proxy-library | 101 | [781, 4, 6] |
| 65 | `08da84c30fc1` | test-layout | 0 | compile/static only or not run |
| 65 | `08da84c30fc1` | platform-contracts-all | 0 | [82, 0, 0] |
| 65 | `08da84c30fc1` | windows-platform | 0 | compile/static only or not run |
| 65 | `08da84c30fc1` | windows-tun | 0 | compile/static only or not run |
| 65 | `08da84c30fc1` | source-runtime-guard | 101 | [188, 1, 0] |
| 65 | `08da84c30fc1` | source-architecture-guard | 0 | [26, 0, 0] |
| 65 | `08da84c30fc1` | strict-platform-stack | 101 | compile/static only or not run |
| 66 | `7e0d8c406f2d` | fmt | 1 | compile/static only or not run |
| 66 | `7e0d8c406f2d` | workspace-alltargets-check | 101 | compile/static only or not run |
| 67 | `b889e3b5ecb6` | fmt | 0 | compile/static only or not run |
| 67 | `b889e3b5ecb6` | workspace-alltargets-check | 0 | compile/static only or not run |
| 67 | `b889e3b5ecb6` | raw-tcp-ingress | 101 | [0, 1, 0] |
| 67 | `b889e3b5ecb6` | platform-unit | 101 | [244, 2, 11] |
| 67 | `b889e3b5ecb6` | transport-library | 0 | [222, 0, 1] |
| 67 | `b889e3b5ecb6` | dns | 0 | [68, 0, 0] |
| 67 | `b889e3b5ecb6` | proxy-library | 101 | [795, 2, 6] |
| 67 | `b889e3b5ecb6` | test-layout | 0 | compile/static only or not run |
| 67 | `b889e3b5ecb6` | platform-contracts-all | 0 | [82, 0, 0] |
| 67 | `b889e3b5ecb6` | windows-platform | 0 | compile/static only or not run |
| 67 | `b889e3b5ecb6` | windows-tun | 0 | compile/static only or not run |
| 67 | `b889e3b5ecb6` | source-runtime-guard | 0 | [189, 0, 0] |
| 67 | `b889e3b5ecb6` | source-architecture-guard | 0 | [26, 0, 0] |
| 67 | `b889e3b5ecb6` | strict-platform-stack | 101 | compile/static only or not run |

Rounds64 and66 stopped before runtime because source compilation or formatting failed. Round65 preserved actual runtime failures, then round67 preserved two metadata fixture failures, two Proxy failures and the exact raw TCP failure. The raw67 trace establishes StalePlan before native connection because the fixture generation had not been started; it is not evidence of successful ECONNREFUSED isolation. All failure output and null compile-only counts remain unchanged.

Round67 strict Clippy failed the new generic tuple type complexity; compile-only and Windows cross-target checks are not native runtime qualification. Earlier independent archives remain unchanged. Final ALL17 qualification remains incomplete.
