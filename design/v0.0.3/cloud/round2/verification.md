# 第二轮候选统一资格索引


## 当前续作输入与未获资格

下方 `70907f0a`、旧锁、workspace/互通数量和“最终交接”字样均是先前阶段历史，
不得冒充本次最终资格。五域实现仍继续，当前剩余集合见 [remaining.md](remaining.md)。
Rust 1.99.0，当前同锁 SHA-256：
`9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`。

| 精确被测实现 | 必要定向及实际结果 |
| --- | --- |
| `04babdd4d35763f679981e31290b6952534046bb` | Runtime/Platform lib build exit0；OwnedStart 实际冷拒/取消/退休后首启 4 PASS，未选进程 Plan 1 PASS，真实 Process/Endpoint 4 PASS，原 Process tree/actor/延迟 Host ACK 1 PASS，真实 Carrier 总额度替换 1 PASS。误筛选零项的单独记录不算通过 |
| `f73f2dd0`（原始记录含完整 SHA） | Platform lib build exit0；同预付声明、foreign Scope 拒绝、冷原件转交、实际 actor join 与延迟 Host ACK 共 3 PASS |
| `f73f2dd0` / `f280ef3277e098ea967755985cfa47f6251f9562` | SS/Proxy 组合构建分别 exit101；首组 11 个 SS 模块/可见性/所有权类型错误，次组仅 fixture Address/Display 错误。B 已追加修正，原日志保留；没有冒称生成了新 Proxy 测试产物 |
| `23996e788e1fa2132a2c81e96a2c001f5ab8b574` | DNS/Platform/Transport lib+web build exit0；query_tasks 12 PASS、wire_call 2 PASS。H2 3 PASS/1 FAIL（原 8s/1-byte window/256KiB，收到210386字节 phase1）；未提高窗口、容量、栈或期限 |
| `8a545c16`（完整 SHA 在原始 input） | Proxy 构建 exit101，DNS `.tasks` 私有访问两项错误；A `8df43dbf` 修正后才获得下行产物 |
| `1350aadf00f204082d6a3d93f5aa1f1a24024e87` | Proxy lib build exit0/634.01s；SS 真 UDP/原件 2 PASS、Process quota 夹具因重复监听 1 FAIL（尚未执行 quota）；Source scoped join/ACK 2 PASS、输入暂停 1 PASS、Source DNS 2 PASS、原 Host DNS 2 PASS、安全历史/总额度 4 PASS；Windows GNU platform/tests check exit0，只代表该输入类型检查 |
| `2c55f763ab14735dbcb2cbce0358e8900678e6ff` | Runtime/Platform build exit0；实际 Scope census/join/延迟 ACK 2 PASS、OwnedStart 4 PASS；warm Process cap 1 FAIL 于旧 blanket gate，M `4f8a32df` 修后仍须复验 |
| `a12c00afa01c04630ff12f7d91c8fbb154db1b35` | Error/Platform build exit0；显式同 payload cleanup receipt 1 PASS、taskless 原件/unique view 2 PASS、related 双原件/总额度 3 PASS。不以错误交接回执充当 native/Scope/用户关闭证明 |
| `398f2ad7`（完整 SHA 在原始 input） | Runtime 组合构建 exit101/31.44s，VLESS 私有方法与次生握手类型错误；B `4765e55f5010070bd1de047c37103ae1040cb8da` 修后普通合入 `2d563e22042257ee8b99e2239a2a32e934812ab1` 排队复验，未提前给资格 |

最新定向接续（不构成统一最终候选）：

| 精确输入 | 实际资格与未通过项 |
| --- | --- |
| `2d563e22042257ee8b99e2239a2a32e934812ab1` | Runtime build exit0/88.51s；真实暖 Process cap 1→2 在活跃业务时 Busy，原 worker/native/ACK 退休后才安装新 cap，同安全历史保留，1 PASS |
| `f99877cc`（完整 SHA 在 input） | Proxy lib build exit101/395.58s，22 项模块/fixture/可见性/类型错误；原错误和各责任域修正提交保留 |
| `70556176`（完整 SHA 在 input） | Proxy/root 组合 build exit101/616.44s，剩 VMess material fixture 一项错误；没有生成新的 Proxy 单测资格。B `0c609b35dfdc23402d255fc5a860020d747ec74a` 修正后普通合入 |
| `953ec49a354e857b87b0bff17b3f9775cc9583c5` | H2 原 8s/1-byte window/256KiB：3 PASS/1 FAIL，实际215225字节 phase1。误写不存在的 web target 的单独命令未执行测试，不计通过 |
| `70da5dfb`（完整 SHA 在 input） | gRPC inline 6 项4 PASS/2 FAIL：实际 split/取消 Pending reader 后 writer 唤醒及旧 prepared 三项通过；2MiB 反向/半关闭超时、真实 timeout payload 诊断失败待归因。H2 同输入3 PASS/2 FAIL，其中新 reset fixture 在实际收到 SETTINGS 前误断言首帧1字节，fixture修后未复验；原小窗口仍未过 |
| `61b07202`（完整 SHA 在 input） | `cargo +1.99.0 check --locked -p zero --all-features --bin zero --tests` exit0/347.71s；包含控制 sampler Scope、quota 同Source与真实退休判断，只是类型检查，未执行运行回归 |

新控制 sampler 和 quota 原件接线、C standalone WG、A WG DNS/故障恢复、B VLESS/VMess/
Trojan 及同错误 payload 回归均继续按相同冻结输入排队，不继承先前产物的通过结果。
C process lookup 首提案因 Linux proc File/DIR 原件仍靠 Drop，M 未接入；C 正补完整原件版。
A Source 总额度的完整 producer/原 acquisition guard 仍在实现；不能提前开放子集并称预算完成。

所有命令、逐 target 结果、工具/环境、真实结束码、源码逐文件输入和产物摘要均由 M
外部 harness 保留。每组仅对应自己的输入；不同 feature 图和产物不得合并为统一候选。
CI `e525f688` 仅准备 round2 的 exact head/Rust1.99/同锁/输入 artifact，最终 CI 尚未运行。
开发证据压缩包已生成；Pages 上传返回 outcome unknown，未收到可确认文件引用，
仍须补齐最终可下载归档，不能只交云端路径。

## 以下为先前阶段的原始资格历史

本轮架构实现仍未全部完成；完整旧调用者/owner/删除边界及缺口见 [execution.md](execution.md)。
此文件记录精确实现输入与实际运行结果。最终交接提交若只更新此索引、工程规则和兼容记录，
其源码差异另列；不会把不同实现输入或历史失败改写为同一候选通过。

## 精确输入

- 被构建和测试的实现提交：`70907f0a2e1c9e5a6b5d50d5860af85c486e22d3`。
- tree：`5f57965cfdbb9ad7ae42b168fbddb3a0c92edf52`；tracked tree manifest SHA-256：`584c5a9796686eb9474ef7af01eb4473c7b143edf3b951da817ec6b9ea821034`。
- 冻结时工作区：`clean`。
- 仓库忽略的 Cargo.lock SHA-256：`05843fd0d9b831fd66bd3c69e67dd257cbe27429607040d0b1f2f858d2ecf599`。外部 artifact 提供 `candidate.Cargo.lock`。
- 工具：Rust 1.99.0 / b940084d7、protoc 35.0；CARGO_BUILD_JOBS=4，dev/test debug=0、incremental=0。
- 测试线程使用 RUST_MIN_STACK=16777216；实际 2 MiB worker 回归保持原生产栈。
- 公共源码由 M 唯一整合；三个实现 agent 各用独立 worktree，首波后复用 A/control、B/budgets。

## 严格检查与必要回归

| 检查 | 真实结束码 / 结果 | 外部日志 |
| --- | --- | --- |
| `cargo +1.99.0 clippy --locked --workspace --all-targets --all-features --no-deps -- -D warnings` | 0 | `m-final-clippy.log` |

严格 Clippy 使用 --workspace --all-targets --all-features --no-deps -- -D warnings。
vendored blake3 的上游 deprecated warning 不改写为工作区新增 allow，也不称依赖零 warning。

| 修复阶段 ebcf8b4c 的受影响测试组（不是新输入资格） | 真实结束码 / libtest 汇总 |
| --- | --- |
| original-1mib-stack | exit=0; test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 998 filtered out; finished in 0.02s |
| proxy-unit | exit=101; test runtime::route_runtime::shared::tests::domain_trace_rechecks_resolved_ip_rules ... FAILED; test runtime::route_runtime::shared::tests::resolved_ip_bypass_overrides_an_already_matched_domain_rule ... FAILED; test runtime::tcp_ingress::runtime::tests::unmatched_domain_is_rechecked_against_resolved_ip_rules ... FAILED; test runtime::udp_ingress::tests::unmatched_domain_is_rechecked_against_resolved_ip_rules ... FAILED; test result: FAILED. 991 passed; 4 failed; 4 ignored; 0 measured; 0 filtered out; finished in 163.90s |
| root-unit | exit=0; test result: ok. 101 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.67s |
| runtime-unit | exit=0; test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 238 filtered out; finished in 0.00s; test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 238 filtered out; finished in 0.00s; test result: ok. 234 passed; 0 failed; 5 ignored; 0 measured; 0 filtered out; finished in 14.25s |
| engine-unit | exit=0; test result: ok. 59 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s |
| engine-policy | exit=0; test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 41.05s; test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 41.42s |
| kernel-process | exit=0; test result: ok. 22 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 27.50s |
| endpoint-contracts | exit=0; test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3.82s |
| proxy-dispatch | exit=0; test result: ok. 116 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 18.81s |
| runtime-boundary | exit=0; test result: ok. 189 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.18s |
| wireguard-inbound | exit=0; test result: ok. 9 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 37.37s |

该阶段命令、各产物 SHA-256、每项日志摘要：`after-full-ebcf8b4c/m-after-full-results.json`。
该阶段仍有4项纯 localhost 回归，随后37cb4a85修复代码；当前完整 workspace 输入和结果另列。特权 Packet 仍忽略，不外推普通 Direct/TUN source 已完成。

## 完整 workspace

输入提交 `70907f0a2e1c9e5a6b5d50d5860af85c486e22d3`，命令 `RUSTUP_TOOLCHAIN=1.99.0 RUST_MIN_STACK=16777216 bash scripts/test-workspace.sh`。
实际 exit=0，耗时 791.1 秒；155 个执行条目，累计 passed=4270, failed=0, ignored=177。
该累计数含单元、集成、文档测试；忽略项不计为通过。

逐 target、失败名、忽略原因和无结束摘要条目保留在 `m-final-workspace-targets.json`；原始完整输出为 `m-final-workspace.log`。

## 独立 feature / no_std / DAG 与固定互通

### 独立面与轻量守卫

| 项目 | 结束码 | 外部日志 |
| --- | --- | --- |
| workspace-check | 0 | `m-final-surface-workspace-check.log` |
| minimal-core-only | 0 | `m-final-surface-minimal-core-only.log` |
| minimal-http | 0 | `m-final-surface-minimal-http.log` |
| minimal-socks5 | 0 | `m-final-surface-minimal-socks5.log` |
| minimal-mixed | 0 | `m-final-surface-minimal-mixed.log` |
| minimal-shadowsocks | 0 | `m-final-surface-minimal-shadowsocks.log` |
| minimal-hysteria2 | 0 | `m-final-surface-minimal-hysteria2.log` |
| minimal-trojan | 0 | `m-final-surface-minimal-trojan.log` |
| minimal-vless | 0 | `m-final-surface-minimal-vless.log` |
| minimal-vmess | 0 | `m-final-surface-minimal-vmess.log` |
| minimal-mieru | 0 | `m-final-surface-minimal-mieru.log` |
| minimal-wireguard | 0 | `m-final-surface-minimal-wireguard.log` |
| minimal-dns | 0 | `m-final-surface-minimal-dns.log` |
| optional-status-api | 0 | `m-final-surface-optional-status-api.log` |
| optional-event-dispatcher | 0 | `m-final-surface-optional-event-dispatcher.log` |
| optional-connector | 0 | `m-final-surface-optional-connector.log` |
| optional-grpc-api | 0 | `m-final-surface-optional-grpc-api.log` |
| optional-status-api-connector | 0 | `m-final-surface-optional-status-api-connector.log` |
| no-std | 0 | `m-final-surface-no-std.log` |
| runtime-contracts | 0 | `m-final-surface-runtime-contracts.log` |
| standalone-browser | 0 | `m-final-surface-standalone-browser.log` |
| minimal-dns-query | 0 | `m-final-surface-minimal-dns-query.log` |
| windows-tun-lib | 0 | `m-final-surface-windows-tun-lib.log` |
| fmt | 0 | `m-final-surface-fmt.log` |
| test-layout | 0 | `m-final-surface-test-layout.log` |
| plan | 0 | `m-final-surface-plan.log` |
| diff-check | 0 | `m-final-surface-diff-check.log` |
| validation-only | 0 | `m-final-surface-validation-only.log` |
| guard-layout-tests | 0 | `m-final-surface-guard-layout-tests.log` |
| guard-runtime-tests | 0 | `m-final-surface-guard-runtime-tests.log` |
| guard-entry-tests | 0 | `m-final-surface-guard-entry-tests.log` |

`m-final-surface-results.json` 保留精确命令、输入/产物与日志摘要。
### 固定工具互通（选定 targets）

| 项目 | 结束码 | 外部日志 |
| --- | --- | --- |
| vmess_xray_interop | 0 | `m-final-interop-vmess_xray_interop.log` |
| vless_xray_interop | 101 | `m-final-interop-vless_xray_interop.log` |
| trojan_xray_interop | 0 | `m-final-interop-trojan_xray_interop.log` |
| wireguard_xray_interop | 0 | `m-final-interop-wireguard_xray_interop.log` |
| hysteria2_official_interop | 0 | `m-final-interop-hysteria2_official_interop.log` |
| mieru_official_interop | 0 | `m-final-interop-mieru_official_interop.log` |
| reference_interop | 0 | `m-final-interop-reference_interop.log` |

`m-final-interop-results.json` 保留精确命令、输入/产物与日志摘要。

参考版本固定为 Xray 26.3.27/d2758a0、Shadowsocks-rust 1.21.2/a03006a、Mieru 3.33.0/48ddb69d5d343d76c9004c5054ee36609579ba13、Hysteria2 2.12.2/619a6f856b69fb7ee6a7a379e810e68b84004605。sing-box 1.13.14 是补充互通，未替换协议基线。
互通直接复用完整 workspace 生成的精确可执行文件；不会运行 workspace --ignored。Mihomo、未选择 target、特权及其他条件测试保留为未执行。

## 历史失败、旧链及剩余资格

- `qualified-2cc64747/`：该输入严格 Clippy/构建成功，必要 root 96 PASS / 4 FAIL；后续 IPC classification 与实际 fresh/same-Scope recipe 修复来自 5dd94af9/94ff50cd。该目录结果不冒充后续输入。
- `full-94ff50cd/`：第一轮完整 workspace exit101；有结束摘要条目合计3249 PASS/22 FAIL/173 IGNORE，另 Proxy 单元5项已失败且随后1MiB SIGABRT无结束摘要，缺失项未计为通过。其后按实际失败修复并重冻结；CLI旧产物22项重跑PASS但首失败仍未归因，新增原子日志留证不等于修复首因。
- `m-final-b6df-failed-*`：gRPC relay fixture 借用/错误握手等待的编译失败，9f949a2f 修复并保留零 I/O 及 matching 原件断言。
- 更早的真正 2 MiB SIGABRT、root 编译器布局递归、Domain DNS/旧夹具及 endpoint DNS token/facts 失败均保留外部原日志；对应修复与准确输入分别记录。
- 曾增加的非 Direct non-default dial 夹具违反既有 schema，已普通 revert；没有扩组合或提高 timeout 换取通过。
- `m-final-caller-audit.json` 在已知实际生产文件中记录 DNS compatibility owner 6 处（含构造）、Session::new 1 处、prepare_datagram_exchange 4 处和 sampler abort 0 处。它是删除边界索引，不是原件关闭证明或全仓归零。
- 未资格 Direct 原型 `codex/v003-cloud-r2-device-stage` / `2c4e21411cbf7364f4c92411fbedfaea00579b36` 排除候选；`c-device-stage.bundle` 和 `c-direct-stage-index.md` 保存后续实现。普通 Packet activation 未应用。
- 完整 GUI/面板同产物、特权 TUN/系统 route/DNS 恢复、真实 Windows/macOS、全部适用互通和 1CPU/1GiB RSS/FD/tasks/吞吐/延迟/恢复退出没有由 Linux 定向或预算配置替代。
- 标准 Packet/System TUN provider/Source/动态 OwnedCreate、剩余旧 DNS/outbound transport/process、无 observer 的即时取消、Windows pipe、完整 replay/worker/Packet-return 预算等是未完成实现，不能列为纯环境待验。
- TEMP-WG-OUTER-UDP-01 单独未关闭、未扩大。FinalMask 并发 UDP 与早期 cold-spawn 未归因记录没有冒称已修复。

## 外部证据与本地复验

原始 logs/json/产物摘要及锁放在 `/workspace/r2-evidence`；交接归档在仓库外，不是新 checkout 的构建依赖，不提交测试二进制、临时快照或缓存。
本地固定最终交接 SHA，并使用交接 artifact 的同一 Cargo.lock 后运行上述 Rust 1.99.0 命令。保持消费者冻结；没有发布、部署或合入 develop/main。
五个实现 PR 的 base 均为 codex/v003-cloud-r2-integration，源码完整头与 B→A/共享依赖见 execution.md。GitHub 远端 CI 只按实际 run 记录，旧 e818fd9e 的失败 run 不属于此输入资格。

## REALITY/Vision 原始诊断

选定互通累计 68 PASS / 1 FAIL（另 4 项 Mihomo 未执行）。VLESS REALITY/Vision 的 FAIL 不改写成通过。
随后直接复用同一 VLESS 测试二进制，仅开启日志并在原 TempMaterial 清理前保存同一次 Xray 原始文件；没有修改目标、断言或超时。
`m-reality-diagnostic.json` / `.log` 和 `reality-original-xray/xray.stdout` 记录：Xray 26.3.27 解析原夹具 `www.cloudflare.com` 时，向环境 DNS `172.31.3.157:53` 查询得到 `connection refused`；随后 Zero 的原本地 REALITY 连接收到 `Connection reset by peer`。
这一项是外部目标解析受环境阻塞的未获资格，不是新的 TEMP；恢复正常 DNS/外部网络后须保持原夹具复验。没有修改用户网络或通过改目标绕过。

最终交接提交只允许上述文档/工程规则与此索引变化；精确交接 SHA、相对被测实现的逐文件 blob 一致性证明和远端 ref 见外部 `m-final-delivery.json`，以及五个 PR 的最终验收表。

文档冻结后补检 `git diff --check`、Rust 1.99.0 fmt、plan、layout、runtime-contracts 和复用原产物的 workspace_architecture，全部 exit 0；精确命令与日志摘要见 `m-final-doc-check-results.json`。未再次运行无代码变更的完整 workspace。
