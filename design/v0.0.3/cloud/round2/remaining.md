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
