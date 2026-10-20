# Frozen rounds 42–48 evidence

This independent archive preserves failures and ignored tests. Each command retains its own source HEAD, tree and lock digest. Compile-only checks, actual tests and permission failures remain separate. Overall ALL17 qualification is **not complete**.

| Round | Source | Command | Exit | Passed / failed / ignored |
| --- | --- | --- | --- | --- |
| 42 | `10f88c5ae074` | fmt | 0 | compile/static only or not run |
| 42 | `10f88c5ae074` | workspace-alltargets-check | 101 | compile/static only or not run |
| 43 | `5d5f1f2fea7a` | fmt | 0 | compile/static only or not run |
| 43 | `5d5f1f2fea7a` | workspace-alltargets-check | 0 | compile/static only or not run |
| 43 | `5d5f1f2fea7a` | test-layout | 0 | compile/static only or not run |
| 43 | `5d5f1f2fea7a` | platform-unit | 0 | [124, 0, 1] |
| 43 | `5d5f1f2fea7a` | component-unit | 101 | [158, 2, 0] |
| 43 | `5d5f1f2fea7a` | source-runtime-guard | 0 | [189, 0, 0] |
| 43 | `5d5f1f2fea7a` | source-architecture-guard | 0 | [26, 0, 0] |
| 43 | `5d5f1f2fea7a` | strict | 101 | compile/static only or not run |
| 44 | `d1bd6c50b6f0` | fmt | 1 | compile/static only or not run |
| 44 | `d1bd6c50b6f0` | workspace-alltargets-check | 0 | compile/static only or not run |
| 44 | `d1bd6c50b6f0` | test-layout | 0 | compile/static only or not run |
| 44 | `d1bd6c50b6f0` | platform-unit | 0 | [129, 0, 1] |
| 44 | `d1bd6c50b6f0` | platform-contracts-all | 0 | [82, 0, 0] |
| 44 | `d1bd6c50b6f0` | windows-platform | 0 | compile/static only or not run |
| 44 | `d1bd6c50b6f0` | transport-library | 0 | [200, 0, 1] |
| 44 | `d1bd6c50b6f0` | vmess-library | 0 | [27, 0, 0] |
| 44 | `d1bd6c50b6f0` | proxy-library | 0 | [662, 0, 2] |
| 44 | `d1bd6c50b6f0` | source-runtime-guard | 0 | [189, 0, 0] |
| 44 | `d1bd6c50b6f0` | source-architecture-guard | 0 | [26, 0, 0] |
| 44 | `d1bd6c50b6f0` | strict | 101 | compile/static only or not run |
| 45 | `f200e15dd6c3` | fmt | 0 | compile/static only or not run |
| 45 | `f200e15dd6c3` | workspace-alltargets-check | 101 | compile/static only or not run |
| 46 | `9e5c640a78f5` | fmt | 0 | compile/static only or not run |
| 46 | `9e5c640a78f5` | workspace-alltargets-check | 101 | compile/static only or not run |
| 47 | `ad409401874e` | fmt | 0 | compile/static only or not run |
| 47 | `ad409401874e` | platform-unit | 0 | [146, 0, 11] |
| 47 | `ad409401874e` | icmp-native | 101 | [0, 10, 0] |
| 47 | `ad409401874e` | stack-library | 0 | [36, 0, 1] |
| 47 | `ad409401874e` | workspace-alltargets-check | 101 | compile/static only or not run |
| 48 | `fa950902a888` | fmt | 0 | compile/static only or not run |
| 48 | `fa950902a888` | platform-unit | 0 | [157, 0, 11] |
| 48 | `fa950902a888` | stack-library | 0 | [36, 0, 1] |
| 48 | `fa950902a888` | workspace-alltargets-check | 101 | compile/static only or not run |

Round47 explicitly attempted ten native ICMP tests in the task process. They failed before acquisition: RAW EPERM (1), ICMP datagram fallback EACCES (13), with KnownNoNative. Those results do not prove post-acquisition close, panic, echo, wake or Unknown behavior. Any later container run is separate evidence and cannot overwrite this result.

The round48 lock change retains quinn-udp0.5.16, replacing its registry source with the checked-in same-version patch and adding the platform dependency. Original lock and exact diff are archived. Earlier through39 and independent through41 manifests remain unmodified.
