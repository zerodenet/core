# 第二轮续作：剩余生产调用者冻结

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
实际验证锁现为 `3e9fea043e217bb125dcfe6040e8e2b60757ab403fa66f1d3719db8e86f33214`，
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
验证锁仍为 `3e9fea043e217bb125dcfe6040e8e2b60757ab403fa66f1d3719db8e86f33214`。

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
