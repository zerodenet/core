# 第二轮续作：剩余生产调用者冻结

## 当前续作状态 2026 11 06

本节优先于下方历史检查点；五个职责域均未结题。最近完成 Proxy 定向构建的输入为
`1350aadf00f204082d6a3d93f5aa1f1a24024e87`；之后的完整生产接线继续定向验证，
没有统一最终候选或最终 CI。续作 Draft PR 为 M #133、B #134、A #135、C #136，
全部 base 为 integration；远端草稿是开发检查点，不能替代实际源码/验证输入。
实际 remote integration 再核仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`；
本地普通续作保留原历史，不回退、覆盖 dirty 或强推。#128–#132 仅历史交付。

| 当前实际调用集合 / 唯一 owner | 本次已接线及精确前置 | 尚须完成的执行、删除和必要验证 |
| --- | --- | --- |
| A DNS：System、所有直接/绕行 backend、Node、持久 DoH、失败 retry | 原 Query/Blocking/池保持；A `20035c0476578a3c4f586e0727db1ccf36412ba4` 的匹配 QueryCall 和有界同错误 Arc 接收已合；M `23996e78` 与 C `334cbae` 接 Source UDP/TCP 实际调用者 | B 完整资源型 donor、C standalone WG/设备 donor 全到位后删除 ProxyDnsOutboundConnector、占位 Session、prepare_datagram_exchange；原 Pending 取消、后备、网络/配置换代和退出组合尚需验证 |
| B SS/SIP：普通 TCP/UDP、共享响应 FIFO、实际 Process | B `5d11287509c44b87a2c7dedc9327ef849a146144` / `dfa38499` / `d324f749` 与 M `31a40deb` 同组合，删旧 runtime waiter/独立 response pump；M `04babdd4` 先付同 job 额度再建 Scope/actor | 1350 输入真实 UDP/空包/多目标已通过；maxJobs 夹具在配置校验发现重复监听而未执行 quota，B `1f18cc8a` 修真实监听键后仍须复验；保留历史轻量 key/cross-executor 和 macOS whole-job 实现缺口，不能称 SS 整体完成 |
| B 全 transport/MUX/Mieru/mixed/device 与无 observer 消费者 | 同真实 Carrier/Metadata/Bound 总账，原 donor/control/Scope；M `85eee212` 抽取原 CA/File immutable 材料，`8a545c16` 只读实际旧 named instance | B `64234560`/`d432467e` 与 M 同材料/登记已接 VLESS 与 VMess 现有 TLS/WS/H2/Upgrade/gRPC 适用路径；尚须实际组合回归。其余 MUX、gRPC/H2/QUIC/XHTTP、mixed 和跨执行方式实际退休、完整 pre-delivery custody 及旧 executor/投影仍未全删。不得以 noFile/TCP 子集或关闭 gate 结题 |
| C Packet/Direct、System TUN、独立 WG/适用进程 | 实际 Source pause、输入取得屏障、原 SDK async worker/TaskRegistrar、四类 return/native-read 预算已合；M `616911dc` Bound retirement 另等实际 driver registration ACK；C Source DNS 消费者与 M shared 已组合 | C `3ba424f2a660e3665cfdc65726cfef55c7e38f6d` 与 M `398f2ad7` 已接 standalone WG 真实 Bound provider/选中 peer/生产登记和原暂停凭据，三项实际回归未执行；实际 process lookup 原件/global总额在 `6e379cd8` 平台3+1项通过；macOS helper、完整TX所有生产者、Packet/MTU/计量和旧链删除仍在实现。ScopedSourceTasks 实际 ACK 两项在1350通过，新容量拒新回归待验。保留方向撤除与回滚旧 TCP 连续性 |
| 控制 G8：Windows pipe、服务器/订阅、TLS、quota/inspect/持久化 | 首波后已复用 A/control；原 handle/std worker/Scope/native 与实际故障交接接线保留，Linux 历史真实控制恢复结果单列 | 当前组合的登记窗口修复及独立入口退出、恢复和同故障接收仍需复验；GNU 检查不等于真实 Windows。缺实现不改列平台待验 |
| 预算 Z003-07：同 Host 各实际 owner | M/A 原 Carrier/Metadata/Bound 总额度和配置 StopOld；M `f73f2dd0` 同账本预付、实际 actor 与 Host ACK；原 SS 安全历史、TUN/Packet/worker 配置继续复用 | A `a46cdc21` 与 M 同 Process typed facts 和精确冷声明 gate 已合；warm cap 在 `2d563e22` 已实际通过。Source77 listener/accepted/UDP/metadata/ICMP producer已纳同总账，平台部分通过，真实Runtime/Proxy关闭窗口及普通换代待验；事件网络、完整TX、transport/SS 历史 key 边界仍需实际owner投影。pending/failed/retiring 不退款，不驱逐活跃业务或安全历史 |

B→A（实际 donor）及需要的 B→C 按上述和后续精确提交继续普通合入。所有共享源码由
M 唯一写入；agent 仅给仓库外建议补丁。全部实现 PR base 为 r2-integration。

当前验证锁 SHA-256 为
`9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`；
`.github/round2/candidate.Cargo.lock` 保存同字节输入，CI 的 round2 路径固定 actual head/
Rust 1.99.0，并保留每 job 输入摘要、检测锁变更，未据此取得任何运行资格。
开发原始证据归档已生成（非最终资格）；Pages 上传返回 outcome unknown，尚无已确认
可下载的文件引用，不以 `/workspace` 路径代替交付。最终必须补齐可取得的归档和摘要。
REALITY 原 `www.cloudflare.com` 目标、断言和期限保持，环境 DNS 恢复后仍须原项复验。
TEMP-WG-OUTER-UDP-01 仅原 outer_udp_proxy/reconnect 范围保留未关闭、不纠正不扩大。

## 以下为历史调用者盘点与开发检查点


续作起点 `59615e69ea5a413d657a873157ea4adf8f204a90`；remote fetch/push 均为
`https://github.com/zerodenet/core.git`，与核对的远端 integration 相同，工作树干净。
固定包基线仍为 `c940b727193f69752c66a9f49a7d64693ad74c1c`。
历史 #128–#132 及其结果只作为历史输入，不代表各域完成或剩余完成率。
本次按完整职责域持续实施，不以准备接口、局部组合、PR 合入或复杂度作为结题条件。

A/B/C 复用同一环境与独立 worktree，原任务分支普通接入 integration 后追加提交。
C 的未资格 device-stage 原型保持独立，实际续作为 device-cont worktree；不整段盲合原型。
M 唯一写入共享契约/装配，agent 的共享建议只放仓库外，经 M 审阅串行提交。
Rust 1.99.0 / protoc 35.0；同一 Cargo target，构建和对应测试由 M 排队。
起始 Cargo.lock SHA-256 为 `05843fd0d9b831fd66bd3c69e67dd257cbe27429607040d0b1f2f858d2ecf599`。

| 负责人 / 实际调用集合 | 当前 owner 与缺少责任 | 标准执行落点 / 前置 | 旧链删除点与必要回归 |
| --- | --- | --- | --- |
| A：所有 DNS backend、direct/detour、Node bootstrap、失败重试 | 原 Source/Query Scope、System blocking、持久 DoH；剩余 detour 尚消费裸流/旧 exchange | 同 native DNS provider；B 全合法 Stream donor，C 同 WG endpoint Datagram donor；单次 selector/LB resolve | 删除 ProxyDnsOutboundConnector、占位业务 Session、prepare_datagram_exchange；真实 Pending 取消、TC→TCP、失败后备、selector ABA、换代/退出 |
| A：standalone DNS 库入口 | 显式库调用者，无 Proxy 生产后备责任 | 保留有实际职责的原 connector API；host 绑定后全走同 Source | host/standalone 分流回归；禁止缺原件后回退 standalone |
| B：SOCKS UDP ASSOCIATE/root 与既有 prefix | 原 TCP 控制与动态 BND UDP 两件，缺同 owner 双原件 custody | M：ControlledAssociationPool、相关 peer Node 候选与同原件关联；prefix 借真实 DatagramLoan | 删除裸 UDP/session 生产后备；真实 ASSOCIATE/BND/UDP Pending、两 view 归还、matching 两件 close、源/族/dial 约束 |
| B：Mieru 全部既有 TCP/UDP、mixed/device/nondefault 合法图 | 同 adapter pool，whole-legacy 尚未受总额度/完整 Scope 约束 | 同 Managed records/grants/Scope、真实 Stream/Datagram donor；B→A 必要前置 | 删除 whole-legacy 资格分支及旧 dial/relay 投影；配置/网络/跨执行方式实际退休与继续原业务 |
| B：VLESS/VMess/Trojan 全既有 transport/MUX | 私有 TLS/REALITY/Vision/encryption、gRPC/H2/XHTTP/Xmux、QUIC/H3/Hysteria、preconnect/browser/reverse 与同池职责未全接 host | 同实际原件/driver/Scope 和标准角色，M 唯一全局登记/transport 接线 | 全调用者迁后删兼容 executor/投影；认证/profile、安全历史、Pending、半关闭、空包/多目标、复用及计量 |
| B：SS UDP/replay、SIP003 出站 | 实际安全历史与原 Child，缺普通 process/provider/后代及预算全投影 | 原协议 owner 与同 host finite process/resource/task grant；不重做已迁入站 | 删除剩余 outbound process/UDP 兼容执行；真实 readiness Pending、转交 I/O、child/后代适用确认、有效 replay 保留 |
| M：普通 business/Service/association 首次 role await | 已有 accepted graph/RetiredDatagrams Entry，未全接 pre-delivery adoption | 在原有限 caller 账本首 await 前保实际 adoption；association 寿命独立首业务 Session；不另起 reaper | 删除无 observer 即时取消缺口；成功须实际 loan/driver custody；取消首 waiter 不破坏仍活跃共享用户 |
| C：Direct Packet、既有 TUN descriptor/Windows adapter | 原 AdoptedTunOwner/OwnedWintunOwner、私有 host driver/returns | M concrete device provider，C 实际 factory/Packet Tx/Rx/control；routing/MTU/Inner/Outer 接普通 dispatch | 删除私有 open/start/Packet route 路径；原 buffer/ReturnAssociation、Unknown 不重放、held view/Scope/native 各确认 |
| C：plain linked WG、独立 WG UDP、Packet→Flow/DNS | 同 EndpointStackOwner、冷 union；独立 wire 缺 source-preserving BoundDatagram，DNS donor origin 未补 | M 同 host Bound provider/真实原件 lineage；C 同 crypto/stack 标准 Packet/Stream/Datagram | 删除非 TEMP direct raw bind/private workers/old exchange；认证 roaming、Pending、回程/片段/MTU、方向撤除和恢复旧 TCP |
| C/M：配置与动态 System TUN start/stop/recovery | 同 TunCompletion/native channels，缺标准 InboundSource/动态 actual slot/TaskRegistrar | M 同 RuntimeHost/canonical allocator 与真实 Packet-device source identity；C 原设备、channels、main/route workers | 删除私有 create/spawn/completion-only ownership；保 TunInfo/ACK/父退出、系统恢复，实际平台资格另列 |
| C：macOS helper、系统 route/DNS command、Windows reader | 原 fd/Child/SCM_RIGHTS/session，缺完整 transfer ACK、适用 join/后代责任 | 同实际 helper/control/原件，先安装再 ACK，真实 worker 进同 Scope | 删除全局 stream/丢失 launcher 责任；不能 EOF/PID/direct-child reaped 当完整关闭 |
| 控制（首波后复用）：Windows pipe、订阅、fault handoff/recovery | 原 NamedPipeServer 仍依赖隐式 Drop；部分 fault 已物理关闭未交 supervisor | M 原始 handle/provider；同 control server/subscription joins、实际故障交接 | 删除隐式 handle/永久 fail-closed 实现缺口；慢/断订阅、panic、最终 ACK、revision、身份撤权和流量结算 |
| 控制：TLS/quota/inspect/持久化剩余调用者 | 已迁同 Source/lock/worker，不重复实现；核全部独立入口及失败/退出边界 | 同 ConfigurationSource 和原 lease/worker/native；Engine-only/FFI 不启动 Proxy | 实际原件关闭、worker join、持久/临时、quota/计量回归；不动消费者 |
| 预算（首波后复用）：Mieru legacy/新全池、WG/TUN worker、Packet return、transport/process、SS replay | 现额度局部，缓存/常量不是总资源配置 | 实际 owner 稳定后由唯一 config 写入人按每实例/身份/共享/总体范围投影；pending/failed/retiring 同计 | 默认省略兼容、耗尽拒新、确认后回收、不驱逐活业务或有效安全历史；1c1g 独立资格 |

B→A、需要的 B→C 和公共依赖随精确前置提交更新；未给出合格 SHA 时不得将接口存在记为已迁完。
各域完成均须：全部实际调用者迁完 → 普通生产装配 → 原件/任务完整 → 旧链删除 → 必要回归。
最终统一检查只在五域稳定后排队，保留每次实际输入、失败、忽略、中断及未执行。

环境资格单列：同新内核的冻结 GUI/面板、特权 TUN/系统 route/DNS 恢复、真实 Windows/macOS、
全部适用互通与 1 CPU/1 GiB 实测。实现缺 provider/source/Scope 不得归为环境待验。
REALITY 保原目标/断言/期限，DNS 环境恢复后定向复验；不改网络或目标绕过。
TEMP-WG-OUTER-UDP-01 仅原 outer_udp_proxy/reconnect 范围保留未关闭、不纠正不扩大。
最终 artifact 必须可下载并含同一验证锁及原始证据，不只提供云端绝对路径。

## 续作共享前置（不是职责域完成）

- `1f47c32353d539df8d54ae95e558f9225a3dbc1f`：实际 TCP 控制与协商 UDP 原件使用同
  ControlledAssociationPool 总额度；关联责任不改物理 CarrierOrigin；原 Tx/Rx 转交保同 LoanOwner。
  平台 carrier 受影响组 53 PASS，额外关联/保留 view 三项 PASS；首次测试编译缺 Future import
  的失败留在外部证据，不将前置资格算整域完成。
- M 暂时唯一写 `component/stream_bridge/{network.rs,datagram.rs}`，按 B 的具体需求补
  同 actual Node resolver 的相关 peer 候选、真实 UDP 观察与 TCP establishment 分离，之后归还 B。
- DNS 的 Single Packet+Datagram 冷选择/实际 open_carrier 只准备现有 wire 形态，仍需 C 真 donor；
  A 只解析一次 selected target，未选 Datagram 时才使用同一次 resolved 的 Stream 路径。
- concrete Packet provider 扩展 Create/ExistingWindows，先留原件/issued identity，再配置；
  失败保同控制器，唯一 view 返回、同 Scope driver join 与 matching native receipt 仍分别确认。
  不因 provider 存在就开启未完成的普通 Packet/TUN source。

## 当前接续状态（五域均未结题）

再次 fetch 的 `FETCH_HEAD` 仍为完整远端 integration
`59615e69ea5a413d657a873157ea4adf8f204a90`；没有回退或强推。
本地依赖以普通 merge/追加提交累积，尚未作为最终候选发布。
此前组合实际验证锁为 `3e9fea043e217bb125dcfe6040e8e2b60757ab403fa66f1d3719db8e86f33214`，
其中只接入实际 Packet backend 的现有本地依赖；最终归档仍须携带相同锁本体。

| 实际调用者 / owner | 已有具体前置 | 尚须接通、删除与回归 |
| --- | --- | --- |
| DNS→SOCKS→Mieru / 原 Query Scope、同 SOCKS TCP+UDP 与 Mieru 池 | `f53`/`e5d6df9c` 的实际 pending 原件容量与唯一 view 先留责任（完整 SHA 以依赖图为准）；失败原始日志保留 | 原两个真实取消/复用回归复验；其余 resourceful/mixed/device donor 全迁完后才能删旧 DNS connector/exchange |
| 普通 TCP / canonical plain linked WG、原 FlowUsers | `b144b4c4` 实际 source wire origin；`e8be9c88` 普通 factory/endpoint-free dispatch；`9cb64a9c` 原 admitted facts/调用者 witness；`6c6ea7b2` 同用户表、Inner 计量、真实 SYN 与换代检查 | 真正普通生产装配回归、借入与直接用户关闭/Scope/native 分别确认；独立 WG provider、Direct Packet 和旧 executor 删除仍缺 |
| 普通 Datagram / 原 DatagramOpening 与设备用户表 | `ca308dd5` 同实际 Packet opener；`5c203509` 完整 OptionalEndpoint 消费与日志，保持网络 endpoint Some | 普通 WG wire/DNS 借入的真实取消、失败/换代与退出回归；不可将 None 填成假 server/port |
| 动态/配置 TUN / 同 inbound registry、canonical allocator、源原件 | `446d3687` 同唯一 ListenerObservers 消费动态源 Scope；`0834b21d` + `bd4d3dfb` + `b2019c70` 原 source 子任务 Scope 与实际声明 | TUN 全旧能力（DNS hijack/sniff/计数/ACK/readiness/错误码）与普通 start/stop 仍须完整切换；后删私有 executor；特权平台资格另验 |
| 无总期限的 gRPC/Mieru / 原 protocol 账本 | `e19371f1` 原 native 单次关闭；`c0acda54` 实际 view/调用者/逻辑归还快照；`51310bc8` Mieru 真实 ordered barrier；`bcc094e9` gRPC 同原件完整结算 | 定向原 wire 回归；所有兼容/跨执行方式与其余 transport/MUX/process 不能以此局部代替 |
| per-connection Service TCP / 原 caller 与组件 Scope | 已冻结错误移交点：仅登记 driver 不能证明长期共享池接受了责任 | B/M 接具体 noncached Service 声明与原 pending/Accepted finalizer；保同额度和 Scope，不建新清理任务/池 |
| IPC/HTTP/gRPC 故障恢复 / 原服务器、accepted/subscription 与预算 | `30ba9212` 物理确清才释放原 slot，始终交出原 typed fault | 四项实际服务器故障再起测试待 M 排队；初启 TLS、quota/inspect、持久化全入口继续核查 |
| Windows Named Pipe / 原 Win32 handle 与 std worker、同 Scope | `656f0d07` 真实 overlapped drain/std join/Scope join/handle close；`0fac8800` 未知关闭不重试可能复用的数字 handle；`ab7a07a5` 外部 waker 不在原锁内归还 | GNU 交叉编译及真实 Windows 回归待验；Linux 不能报告 Windows 运行资格 |
| SS replay 与剩余生产预算 / 复用 A 的预算 worktree | `af535bbd` recipe 绑定同实际安全历史；既有 51ad/DNS/control 不重复记成果 | policy/capacity/profile 替换与有效历史、总额度、WG/TUN/returns/transport/process 实际 owner 投影；保每实例/身份/共享/总体语义，pending/failed/retiring 不退款 |

A 完成控制增量后在等待 DNS donor 期间复用原 budget worktree；SS replay 文件由 B 明确唯一转交 A。
B 保留其他协议/transport/process，C 保留设备/source，M 仍唯一串行应用公共契约与装配。
所有上述提交只是前置与当前调用者状态，不是最终资格。开发编译失败逐次留档；修复只追加，
没有放宽断言、deadline、生产容量或栈。统一最终检查、续作 Draft PR 与可下载原始证据仍须完成。


## 续作组合记录：原件生产责任继续推进

远端再次核对仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`。
协调 Draft [#133](https://github.com/zerodenet/core/pull/133) 的 base 为 integration，
当前远端 head `ff7a1f7a69743701be63c9772f3f1fe571494205` 仅为阶段输入，
没有最终候选资格。本地普通 merge/追加提交仍在继续；下列事实不表示任一域结题。
该组合验证锁为 `3e9fea043e217bb125dcfe6040e8e2b60757ab403fa66f1d3719db8e86f33214`。

| 原调用者与真实 owner | 已组合的必要前置 | 剩余执行及删除门 |
| --- | --- | --- |
| DNS query → SOCKS 双原件 → Mieru 原池 | B `0ecf82c4662824700f817e1f47cf589a88814484`；A `dbe7a6586394c8f1fae0a5f97b96cf4a87163078` 改用普通 RunningProxy 同配置/同源 | 首次请求/取消 ACK 原失败未获修复资格；其余 resourceful/device/ECH caller 全迁完再删除旧 connector/exchange |
| 诊断、URLTest → 同 RuntimeHost 与 Direct 原 Scope/原 provider | M `0bbd922b` 真实 probe task、唯一全局额度、首 await 前 custody；`995c7ece` 候选额度与退出；配置 Direct 用其原 Scope 实际 initializer | 普通冷/热、coalescing、最后 waiter 取消及既有 V1 错误/计量必要回归；新真实用例 `a438b5bd` 待构建，不能拿票据测试当生产资格 |
| 无 observer 的取消 carrier | M `fc65d433` 同原 Slot 持有关闭 future，同原 host supervisor 轮询；固定首次清理期限，不增加 task/Scope/池 | 平台真实 I/O + actual Scope join 才退唯一额度的回归通过 1 项；Mieru/gRPC 普通 wire 及各合法调用者仍须验证 |
| VLESS gRPC Stream/Datagram 与原 gRPC pool | B `daac42b5`；M `a4f67381` 同 immutable recipe、`3f8e2417` 普通登记、原 root totals 配置 | CA/ECH/all backend、Relay/MUX 与其余合法 transport 仍有实现；不以局部 gate 关闭当全域完成 |
| TLS CA、初启 control TLS、父 stdin、inspect/quota | M `e8e76efe` 同授予 CA 原件的真实 TLS backend；先前 Source bounded read、parent std worker/Scope/native 均已组合 | 新 CA/backend 回归与最终统一输入；Windows GNU 类型检查仅代表相应历史 SHA，不代表 Windows 运行；quota/inspect 原职责继续审计 |
| System TUN ordinary source、route/monitor 与实际 Registrar | C `c141883d`；M `a132e704` 既有 UDP 构建 neutral source 闭包及原 native 计量，`ab50ab81` 实际 shared-Scope 总额 | 真实 privileged backend 资格另验；helper/Windows reader/独立 WG/Direct Packet 仍是实现职责。旧生产 runtime/route 执行链迁完后删除 |
| System TUN main exit、TunInfo/失败恢复投影 | C `7ee6d484`；M `1df7ab88` 同 exact original observer 投影和自动停依赖前真实 stopping 事实 | EOF/主动 stop 对照、错误身份隔离、所有原 Scope/native 清理及恢复回归待验；投影不发关闭凭据 |
| TUN 小总额度满额与恢复 | M `3c60b3c7` 保原每组 ceiling，同 Registrar Capacity 已知未登记拒绝仅归还冷 reservation | 真实 TCP Pending/main+child、actual join 后可再登记的必要回归待验；无 native TUN acquisition，不能作为原设备资格 |
| SS TCP/UDP/legacy replay 与同 adapter 原安全历史 | A `98f240d8` legacy 实际 bitmap bytes、`404dc5d2` UDP 五额度，B `0ecf82c4` SIP 原 Options 投影 | 新模块/可见性编译错误逐项留档并追加修复；SS 两测试三 case 的整体超时保留，Phase 诊断 `a183adab`/`1ee3e16b` 待实际运行 |
| SIP003 原进程/派生 loopback | B `02847eb0` 单一真实 actor 做 prepare→Ready→monitor | Linux/Windows/macOS 适用 process 原件、后代确认、原 actor join 与派生端点授权仍缺实现；不归环境待验或 TEMP |

开发仍只排必要构建/回归，原始失败日志留在仓库外。`4db6e4d5` 正构建所影响的 root/
proxy/runtime lib 产物；新的本地提交不得借用该产物声称被测。最终统一检查、精确候选 CI、
可下载原始归档及各域旧执行链归零检查尚未完成。TEMP-WG-OUTER-UDP-01 保原范围单列未关闭。


## 后续实际调用者与原件检查（实现继续，未冻结最终候选）

远端 integration 最近核对仍为完整 SHA
`59615e69ea5a413d657a873157ea4adf8f204a90`。协调 Draft #133 仍非最终候选。
现有 manifests 增加 zero-dns 对既有 zero-core 的直接依赖以匹配真实 Scope 身份，
实际验证 Cargo.lock SHA-256 已更新为
`9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`；
没有改变任何第三方包版本、来源或校验和。仓外锁差异证明与每次原始输入记录均保留。
最终仍须提供实际同锁本体及下载地址，不以本段替代归档或候选 CI。

| 职责域 / 真实调用者 | 已取得的必要证据与后续前置 | 尚缺实现、删除或复验 |
| --- | --- | --- |
| DNS / 原 Node Query、持久 DoH 和 detour | `4abc0a63d2e66dfb2c58b4ccffe10c7d3a6e74c1` 上 ECH Domain 与跨 Default/Node 同物理 DoH 通过；UDP65 夹具原错把 EDNS 尾部当 question，`3ea3d50f` 修真实 question 解析 | VLESS gRPC 四项首次真实生产调用均在 native 前报缺 controlled grant；B `6073acca`/`59508ba7`、M `f59dd581`/`5fbe371c` 接原 Node opener 与完整候选，待同测试复验。`f47b8e1e` 同代 N1/N2 实际 peer 原池修复也待验证。全部 mixed/device donor 到位再删 ProxyDnsOutboundConnector/占位业务 Session/旧 exchange |
| 出站 / taskless Service、Business、Relay、gRPC/其余 transport | 同 `4abc0a63` 的四协议 None-deadline N1 真 wire/cancel 通过，复用唯一原件与额度；gRPC CA/ECH/Business 六项均暴露同前置 grant 错误，原日志保留 | 所有 taskless Business 仍有 `inner.connect -> bare Node TCP` 旧链，SS 原件断言真实失败，B/M 正补同一个原件 owner 的严格双需求授权；全合法 Relay/MUX/transport 及跨执行方式继续迁移，不能放 gate 或增第二池代替 |
| Packet、设备/TUN / 同 Source Native、原 Direct Packet provider | C `a15a83ee`（M 单取 `f6d3a429`）核同 EndpointIo/Live/listen/active lease，修新虚拟 Link 错停真实 Source；`4abc0a63` 配置/网络替换及普通 literal/domain 三项通过。Direct Packet 两个真实原件负向、TUN 一主任务额度在 native 前拒绝通过 | Packet 第四项停在新增夹具错误 tag，C `5c51a9e9`/M `193ca853` 修后待原强断言复验。独立 WG provider、原生读取/解密/重组及分片真实计额、Windows reader/helper、适用进程 lookup 和旧 TUN/Packet 执行链删除仍有实现；特权 TUN 原生资格继续明确 ignored |
| 控制 / TLS、父输入、Named Pipe 与服务器 | 先前 Source TLS/父输入及真实 Linux 控制恢复证据保留；A `c704b97d` 增真实 Windows ping/订阅/失败再起原件测试，未运行 | 同原 ControlTask 登记窗口仍可能丢失已 spawn JoinHandle，A 正修唯一槽；Windows GNU 检查和真实 Windows 运行分别记录，不能互换资格 |
| 预算 / SS Source、UDP 安全历史、进程和 Packet | `4abc0a63` 的 SS pure Source 与双 UDP Source 两项实际预算换代通过；Source acquisition idle 由真实 peer cleanup 唤醒。SS Business 第三项失败保留。Linux 原进程树 `6ab90ae1a9071dfdcabb36b742db3d9d690092b1` 三项真实回归通过；Host 原件/实际 Scope/原 max_starts 两项在 `2e3f9565cb61653fe5ccd5331605c489161f4233` 通过 | Linux Process provider 已接 Host/BoundResources 冷 grant，B 实际 SIP 调用者仍须接入；全局生产 process/transport、原 Packet return/native-read、WG/TUN worker 等额度投影仍缺。进程树、实际 actor join、原 fd closes 各自核对，不把 body/EOF/端口重绑当关闭证明；Windows/macOS process 仍是实现缺口 |

以上为必要开发回归，不是统一候选资格；不同 SHA/feature 产物严格分开，失败/忽略均保留。
最终五域调用者归零、普通装配、真实原件任务责任、旧链删除和必要回归完成后，
才冻结统一输入做完整验收与精确 CI。TEMP-WG-OUTER-UDP-01 仍仅原 outer_udp_proxy 范围未关闭。

## 续作核对更新（先前表中待复验项以本节的新输入为准）

同一输入 `88c6bcea246f1a29c780d7be30ef7b4fc68cbdcf`、Rust 1.99.0、
Cargo.lock `9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`：
SS 三项真实业务/Source/安全历史预算、gRPC 六项实际 CA/ECH/网络/计量、VLESS gRPC
DNS 四项候选重试/原查询取消/换代、Mieru 两项原 Node 代次/嵌套真实原件定向回归通过。
这不包含剩余合法 transport、MUX、mixed/device 或完整 DNS detour 集合的资格。

同输入的实际 Relay 回归在最终读取出现 `unexpected end of file`；真实 wire 和计量
断言没有删除。B `b988036f` 改为实际认证流 half-close 后读取 reverse EOF，尚待复验。
同输入的 taskless ACK 回归在实际 unused retry 后仍占额度而失败；后续 `02d6116d`、
`4db80898` 修原件/view 在 IO 前持有与正面无原件退款，`21158565`、`a41e257d`
另外保留已获得原件直到真实 Host ACK，`fd82139a` 区分可见的 Native/User receipt
和总额度释放，均需在含这些提交的冻结输入验证。旧失败日志保留。

root 的 `--lib` 不编译 `zero` 二进制。首次补 `--bins` 在上述输入发现三处控制类型
错误；`38870732` 修正。仅将该修正加入原冻结源码的独立验证输入
`3b609770`：控制原始任务登记、并发命令故障、Unix 原件/无效 UTF-8、HTTP 与 gRPC
服务器退休、订阅任务及控制故障恢复的必要回归通过。第一次 HTTP filter 选错而
运行零项，单独保留记录；正确 `http_adapter::retirement_tests` 已执行通过。
随后 A `6190bb9e` 保留 status monitor 的原始 JoinError，待下一 root 输入验证。

`7e9c324d` 接同一个 Process job 的实际端点预留、匹配原件关闭、inline readiness
probe、真实 actor 及跨 Scope 直接依赖授权；`407bc7f8` 接 Windows Job 原件同一
预算/端点职责。Windows Job 先 suspended 创建并强存原始 handles，分配 Job 后才
resume；清理要求实际 root wait、Job 内零进程、原 actor join 和逐 handle 关闭。
`8e490d33` 的 Windows GNU platform all-features/tests 源码检查通过；真实 Windows
运行尚未执行。Linux 端点与 Packet 原件测试的第一次编译发现 fixture 私有字段访问，
`ecb001a8` 修实际 accessor，继续排队验证。macOS 任意后代进程责任仍有实现缺口，
不能用进程组、单 PID/EOF 或源码检查称完成；已在调查 XNU 实际可用机制。

C `0c3ebe93` 私有整合及 M `7ba21b4a`、`bd52371f` 接真实 Packet return MTU、
同原存储分片/重组、Direct/TUN 读取预算和 Windows 原 SDK reader 的实际 Scope。
WG 首 bind 前的成对 workspace、独立 provider、完整 Source 暂停/预算 preflight
与旧链删除仍在实现，未把新字段或 foundation 回归计为生产调用者已迁完。
M `22dbd013` 将全部现有 H2 client 入口的 connection/read/write 执行合并到同一
实际调用者，删除该入口旧三个 spawn 和无界写队列；真实 wire/backpressure/half-close/
原 loan 取消测试已接实际 `web_carriers` suite，尚待执行。这不关闭包裹它的旧 native
socket、其余 transport 或共享池迁移责任。

上述均为开发输入；所有五域仍按首节完整调用者清单持续收尾。当前 Draft PR #133
仍是早期进度记录，尚无统一最终候选或该候选 CI 资格。最终锁文件、源码清单和
原始日志必须以可下载归档交付；云端路径及分散输入结果均不替代该交付。


## 当前接续检查点（继续实施，不是最终候选）

源码输入 `ea953646d257ada18c0658e62ee5f69567b9cfcf`、Rust 1.99.0、上述同一
Cargo.lock：原 Host initializer 首借用取消后继续推进与清理两项、真实 ProcessEndpoint
预留/探测/原件/actor 四项，以及 Metadata 实际可写后同总体额度与 caller ACK 一项通过。
先前 `5905c97b` 的原件总账五项中四项、ProcessBudget 真 RawWaker panic/延后 ACK 一项、
Metadata driver/user graph 三项、双 reader SourcePause 一项通过；Metadata 非阻塞发送
夹具首次 WouldBlock/超时保留，由 `d62e58c4` 等待同原 socket 可写后在上述输入复验。
两次匹配零项的错误 filter 另记未执行，不能算通过。

H2 原期限/一字节窗口/256KiB 断言未改。`5905c97b`、`55de5a53` 均仍三通过一超时，
实际接收分别为 178701、183332 字节；继续定位，不提高期限、窗口或生产容量。
进程监控两项失败分别归因为 canonical 模式冲突的错误分类及主动撤权不同于进程退出；
B `f8c5ceec` 与 M `007a26dc` 已保留实际类型/真实 exit 23，尚待同输入复验。

Carrier/Metadata/Bound 同 Host 原件总额、实际 Source/Direct 暂停、异步 SDK 获取及
按需同进程启动已组合，仍须补完整普通生产投影、剩余 provider 与旧链删除。
A 的总体额度 StopOld/DNS 委托，B 的完整 SS UDP/全部剩余 transport 与共享池，C 的
独立 WG provider/前置 process lookup，以及控制末项与全域预算都继续实施。
五个完整职责域尚未关闭；远端 integration 最近核对仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`。
Draft #133 仅保存可审阅的接续源码，无最终候选 CI 资格。TEMP-WG-OUTER-UDP-01 原范围不变。
