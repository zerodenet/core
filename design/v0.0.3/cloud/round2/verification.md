# 第二轮候选统一资格索引

## 开发接续检查点 2026-11-07 00:10 UTC

远端 integration 实际完整 SHA 仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`，
remote 为 `https://github.com/zerodenet/core.git`。普通接续，无 reset/强推/覆盖 dirty。
最新冻结构建输入 `f7af2259`（原始 input 保存完整 SHA）仍在必要定向构建；
不是最终候选，五个职责域均未获完整关闭资格。

当前实际 `Cargo.lock` 和受版控 `.github/round2/candidate.Cargo.lock` SHA-256 均为
`402d67cc184df4d4bc306d50e0badb8a57ccf43f2807b490502eca3b480dd698`。
变化来自 inspect 消费既有 ConfigurationSource 的实际依赖；先前 `9026e325…`
测试继续按原锁单列，绝不跨输入借资格。

| 实际被测输入 | 本次真实定向结果与边界 |
| --- | --- |
| `91beda0531c7e03cbc31db2ac19e3771fe8d5ad1` | 九包 all-features lib/bin/web no-run exit0，769.43s，原始日志 SHA-256 `338cc81c8963d0c4e2f2ad83ef0e5f467890f46dbf432af6bf6bca68d4aae847`；同产物 Source 全局真实预算1、普通 H2/Upgrade Business Domain1、SIP1、Runtime cold13/late10/实际 policy phases2、独立 inspect1、quota 真 worker4、控制资源退休29、DoQ6、DoH callback1、staged N1 deadline3、ALPS2、software firstPending/真实 Scope1 PASS。Echo3 PASS/1 ignored（保原忽略原因） |
| 同 `91beda…` | H2 原1字节窗口/256KiB/8秒：5 PASS/1 FAIL，收到224397字节 phase1；linked WG 3 standalone PASS/5 linked FAIL 于初始 apply；Packet budget1 FAIL 于初始 apply；DNS stateless10 PASS/新 cached gRPC→MUX1 FAIL Revoked；ordinary MUX4 PASS/2 FAIL；DoH故障恢复1 FAIL 原 panic 未交清。全部失败原始记录保留，不称环境待验或完成率 |
| `1e96a503`（完整 SHA 在原始 input） | Runtime/Engine no-run exit0/90.51s；完整 network restart 实际旧 Business Busy/原 TCP 连续/停旧后刷新 cold 集合3 PASS；统计 byte 同 owner原子配置/真实held Busy9 PASS；实际carrier/process policy phase2 PASS。尚非 TUN 生产装配资格 |
| `6c7511ac746b62fdff67f312dfba44768ac20573` | 组合 no-run exit101/32.39s，新 Service 回归误用不存在的 adoption getter；`f7af2259` 修为实际 retained LoanObservation。没有生成该输入新测试资格 |

本次实际实现推进（均待各自新输入回归，不当作整域完成）：

- A DNS：保原 Query/持久池；`9b82a195` 将原 DoH native close 与同故障 canonical receiver/真实 Host ACK 分开，保原错误；待复验。完整 donor 集合、设备/TEMP 调用者及旧 DNS bridge/Session/prepare_datagram_exchange 删除仍未完成。
- B/M：普通 VMess/VLESS/Trojan 完整既有 MUX transport 材料进入同池，原 H2最后 DATA/END真实flush修复通过 Business/scoped 原件回归；`1e96a503` 修原 pooled preparation strong owner 提前 Drop，`0623919a` 修原 VMess MUX END→正常 upload EOF 后继续 downlink，待复验。Relay/Service、REALITY navigation、QUIC/XHTTP/mixed及全旧执行器删除继续。
- C/M：删除不可达 TUN legacy executor；`e4808629` 修 Source host拒已校验 PacketDatagramReads 的真实初启缺口，待 linked/Packet 回归。`5852c659` 接实际 System TUN 停旧→同 Root target policy→明确 candidate Arc Source Ready/真实网络代次→同 immutable材料 cold refresh→BuildNew→Source binding；失败恢复全链、完整 TX/SDK/8项实际Root、TEMP软件root/provider、macOS受控helper仍须继续。
- G8：独立 inspect 已消费同 ConfigurationSource/真实FileClose与既有scratch；quota未接最终worker fault保同Source且新原件前交接，4真worker回归通过。Windows NamedPipe/raw handle与订阅、真实跨平台恢复仍要适用环境资格。
- 预算：新增统计历史/每订阅可选生产字段已消费实际同账，缩额不驱逐held事件；9项通过。其余 Metadata/QUIC、Packet/TUN/transport/process及SS replay完整投影继续；固定常量不当作生产配置已完成。

所有定向原始 input/result/命令/工具版本/日志/产物摘要保存在 Git 外。
`m-91beda-engine-final-quota-worker-fault` 误筛选0项不计通过，正确后续
`m-91beda-quota-actual-four-worker-lifecycles` 4 PASS；一次software诊断从dirty主树
发起，保留并排除输入资格，正式 frozen91beda 记录另列。Callgrind诊断保原8秒，
受插桩影响的失败只作归因，不作运行资格。

尚无统一最终同 head CI，也尚无确认可下载的云端原始归档；Pages先前上传
outcome unknown，未伪造 FileID/下载链接。唯一 TEMP 仍严格为
TEMP-WG-OUTER-UDP-01 原 outer_udp_proxy/reconnect 范围。Darwin任意SIP003
缺少可用whole-job原生provider/权限与普通跨平台运行待验分列；其他可做内核实现不停。
不合 develop/main，不发布部署。下方先前内容均为历史输入。



## 当前续作输入与未获资格

下方 `70907f0a`、旧锁、workspace/互通数量和“最终交接”字样均是先前阶段历史，
不得冒充本次最终资格。五域实现仍继续，当前剩余集合见 [remaining.md](remaining.md)。
Rust 1.99.0，当前同锁 SHA-256：
`9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`。

2026-11-06 22:53 UTC 更新（优先于下方所有历史输入）：

| 精确实际 checkout / 必要命令 | 实际结果与不能替代的边界 |
| --- | --- |
| `aa6364c0caa31e1eaeb9ea20e38f1292d07fd303`，六包 all-features lib/bin no-run | exit101/582.58s，WG witness 私有访问及 Mieru 测试 RuntimeConfig import；`1c0ae919`/A `6f843f3b` 修复。保原日志 |
| `03c6320dde3514931c40968116e868211ab597a1`，`cargo +1.99.0 test --locked -p zero -p zero-platform-tokio -p zero-runtime -p zero-proxy -p zero-dns -p zero-connector --all-features --lib --bin zero --no-run` | exit0/814.12s；随后复用该同一批产物串行定向。实际 lock 为上列同 SHA。此输入早于后续 sealed dependency/两阶段 Source/TUN capsule，不能证明新代码 |
| 同 `03c6320d`：DNS VLESS stream 全组 / WG 全组 | 10 PASS；WG 3 PASS/5 FAIL（linked 初始 apply、warm 重复监听夹具）。原失败保存。SIP 1 FAIL（夹具误选 UDP owner）；DoH recovery 正确 module 1 FAIL（独立缓存退出顺序）；`7b6938f9` 修正待验。曾用错 module 的 zero-selected 结果不计通过 |
| 同 `03c6320d`：DoQ / DoH callback / stateless 全测试组 | 6 PASS / 1 PASS / 3 PASS；后者为真实绝对期限、独立原任务 Host ACK、N1 custody。初次只选 preparation 的 1 PASS 不是全部三项，原命令均保留 |
| 同 `03c6320d`：控制 services / bootstrap rule-set | 29 PASS / 3 PASS；仅 Linux 同产物职责，不代替 quota/inspect 全入口审计、真实 Windows/GUI、特权 TUN 或跨平台 |
| 同 `03c6320d`：cold Source / late native view / 全局 Source budget / Mieru warm | 12 PASS / 实际 late 筛选 10 PASS / 1 PASS / 1 PASS。late 组包括同 Bound late-view/JoinACK/原件回收，不把筛选名称当全部是一个 target |
| 同 `03c6320d`：普通 Business refused Domain / encrypted Packet budget | 各 1 FAIL：前者实际完整正确前缀但尾 1025 字节未交付，B 正修 H2 server native flush；后者初始 apply 5s 未完成，下一源保真实阶段诊断。未改期限、容量或断言 |
| `e3e846c86c56c11af74f8228c3fb29da7d9bc1b1`，同六包 all-features `check --tests` | exit101/346.25s：dns_wire 的真实 backend future Send 类型深度和 TUN original Owner.request_stop 缺方法。A `7b6938f9` / M `6f40d307` 修复，未提高递归限制/栈；下一精确输入 `3ec3b64879ccfb7078188c5ccb394d3276704575` 类型检查仍排队执行 |

本轮外部证据传输和同 head 最终 CI 尚未补齐，不具最终候选资格。源清单及原始日志由
Git 外 run-v3 同一冻结 checkout 记录；现有失败/取消 CI 或其他提交结果不复用为资格。
22:51 UTC REALITY 原目标仍 DNS errno -3，未更换目标/解析器；忽略/未执行项继续保留。

以下 21:10 及更早内容为历史证据：

2026-11-06 21:10 UTC 接续，仍无统一最终候选：

| 精确输入 | 实际结果与限制 |
| --- | --- |
| `90ac0f97f3bf2bf69863e425f364273f704edcc8` | Rust 1.99.0，zero/platform/runtime/proxy 全 feature 含测试代码 check exit0/373.83s；仅类型检查，没有执行新增暖 DNS、Source 换代或 G8 回归 |
| `c5848c57`（完整 SHA 见原 input） | platform/transport 构建 exit101/19.91s；新增 HostedScope helper 标注及 gRPC fixture 私有路径/旧字段问题。修正追加提交，原始失败保留 |
| `2bb13461`（完整 SHA 见原 input） | platform 构建 exit101/10.53s，拆分 user receipt 后模块路径遗漏；`5c511b72` 修正，没有变更关闭断言 |
| `5c511b72d80acfbb43f7825d224f180b6d4e3360` | platform lib build exit0/22.02s；Logical Packet 真实 Pending 软件 driver 与同总额度 N1 两项 PASS：body 结束未 ACK 不退款，实际 ACK 后才复用，始终无 native close；未覆盖尚未落地的 TEMP 实际消费者 |
| `c31dabd3b3cb791bb481d0cffdd45f30b2a58537` | transport lib/web build exit0/38.37s；gRPC 原 8s/2MiB 单/多路完整回程及半关在实际生产 provider 下 1 PASS/0.08s，断原 TCP_NODELAY、matching native close，accepted 用户仍明确 Unaccounted，不冒称 Scope 完整资格 |
| 同 `c31dabd3` | H2 原期限/窗口/容量 4 PASS/1 FAIL，实际199246/262144字节 phase1；没有提高8s、一字节窗口、生产栈或队列。随后生产唤醒合并 `9c1667bc` 尚未复验，不称已解决 |

本次新增 Source 同 pass 命名 ID/最终 generation 投影、原 Source StopOld 屏障、
同 DNS token 的所有实际 donor 换代、B 原 Slot 准备 Aux/Host ACK→单向交付、
应用初启同一个 Host/root 及多故障接收均继续接线与定向回归；准备值不等于域完成。
当前 GitHub REST 在云端返回 Forbidden；Pages 先前上传仍无可确认文件引用。
已向用户请求可写证据上传地址，期间内核实现不停；尚未交付可下载云端原始证据。

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

2026-11-06 19:24 UTC 开发接续（以下仍不是最终同一候选）：

| 实际输入 | 新的原始结果及边界 |
| --- | --- |
| `469bad98` | 控制 JSONL/HTTP 定向 15 PASS/6 FAIL；5 项配置实际需要 Direct UDP 而本次 feature 图未提供，另1项新 fixture 未关闭真实控制 admission；不算 G8 通过 |
| `0718f93ffc71201603636bf928843ac635f44d8a` | 改用 mixed 真实功能图完成 build；控制最后一项复用未释放的测试启动暂停而中断 SIGINT，exit -2，137.74s，没有 target 完成记录；前20项逐名日志保留，不计整组通过 |
| `81c514c2`（完整 SHA 在 input） | 只移除新 fixture 在旧原件均退休后的启动暂停，保留真实 native/Scope/hook/network/control/quota 断言与外层5s；重新构建后实际旧故障保留且原件全部关闭后 fresh startup 1 PASS/0.52s |
| `551cb6b2`、`6f5527a9`（完整 SHA 在 input） | Source/lookup 首次组合 build 均 exit101；分别为 owner 可见性/旧 fixture 字段，和 lookup 真实类型路径/内部 sibling 可见性，修正追加提交，原日志保留 |
| `6e379cd8`（完整 SHA 在 input） | 平台/transport lib build exit0/30.94s；真实 process lookup 原OS和同Host global/helper ACK 3 PASS，accepted lookup 1 PASS，listener owner 原件13 PASS，UDP Source2 PASS，metadata Source及回调锁边界2 PASS。gRPC41项40 PASS/1 FAIL，多模式回程原8s收到1376256/2097165；coop修正未解决，不冒称根因 |
| `6e379cd8` | Proxy/Runtime组合 build exit101/365.90s：lookup跨crate路径/owner可见性及VMess fixture误取只读Observation；独立Runtime build exit101/87.78s：新registering fixture发送断言额外要求原件Debug。均已追加窄修，未生成这些新组的运行资格 |
| `add32bbd`、`84ea6151d567e782012a01033d7d5d820772445a` | MUX实际Scope辅助准备切片首build因遗漏同源错误helper exit101/17.45s；修为原ChannelError完整retained后build exit0/25.43s，pooled recipe/authority7 PASS；不等于普通MUX实际生产矩阵通过。gRPC原8s透明native诊断仍1 FAIL，固定约44ms读写节律、两端原nodelay=false，日志保留；生产既有native provider固定nodelay=true，尚不以此冒称修复 |

C process lookup v1 未接入；补齐 Linux 原File/DIR匹配关闭、Windows原HANDLE和
实际 blocking helper join/ACK 的 v2 及同Host global原件计额已接入，以上3+1项只证明该边界。
A Source77 全producer与真实acquisition窗口已接入，平台组通过，Runtime/Proxy普通
StopOld、linked换代、ICMP真实资格仍待验证。控制初启同Host额度/真实Epoch limits、
C完整TX与helper、B全合法transport/MUX及旧执行链删除继续，不继承先前产物的通过结果。

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
