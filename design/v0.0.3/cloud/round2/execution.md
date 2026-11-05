# 第二轮执行记录

本记录只描述本轮实际执行；任务卡的完成标准不变。所有实现 PR base 均为
`codex/v003-cloud-r2-integration`，只有 M 普通合入该分支。

## 冻结起点

- GitHub remote：`https://github.com/zerodenet/core.git`，fetch/push URL 已核对。
- 远端 r2-base 与初始 integration：`c940b727193f69752c66a9f49a7d64693ad74c1c`。
- 已确认上述包 head 包含 checkpoint `e4c3bc0e20628ce4e50bde4d829b80fba17266c5`。
- M 主 checkout 与 A DNS、B outbound、C device 的独立 worktree 位于同一环境；
  初始角色分支均为包 head。未改动 develop/main。
- 已安装 `rustc 1.99.0 (b940084d7 2026-09-28)`、匹配 rustfmt/Clippy、protoc 35.0。
  Cargo 构建/测试共享一个 target，由 M 串行授权；开发只做必要定向。
- 仓库忽略 Cargo.lock；环境原锁与新基线不匹配，首次 `fetch --locked` 失败。
  M 离线 materialize 当前源码锁并向各 worktree 分发同一锁后，`fetch --locked`
  成功。未升级协议上游，未将生成锁或原始日志加入 Git。
- 已校验并准备官方 Xray 26.3.27/d2758a0、Shadowsocks-rust 1.21.2/a03006a。
  初次 Mieru probe 准备失败：环境 `/usr/bin/go` 不是 Go 编译器；工具准备继续处理。

## 当前生产调用者冻结表

下表是开工时的真实调用及缺口，不表示已完成迁移。细分及精确提交随实际实现补充。

| 调用者 | 实际 owner / 缺少原件 | 唯一写入人及前置 | 迁完后的删除边界 / 必要回归 | 状态 |
| --- | --- | --- | --- | --- |
| System/name/wire/Node 查询 | 基线 OwnedDnsQueryTasks、原 blocking jobs/Scope | A | 保留有职责 standalone API；实际 Pending 取消、nested join | 继承，受影响时回归 |
| 直接 UDP/TC TCP/DoT/DoQ、DoH | 基线 native provider、原缓存及 H2 driver | A | 不建第二池；失败原件、复用、网络/配置换代 | 继承，受影响时回归 |
| taskless/default-dial Stream DNS detour | 基线 DNS Root 原件/Query Scope | A | 不推进额外选择；selector ABA、取消、原件 matching close | 继承，受影响时回归 |
| Mieru/MUX/HY2 resourceful Stream DNS detour | 旧 ProxyDnsOutboundConnector/Session 0；缺 donor loan 完整消费 | B 原件 → M helper/gate → A | 最后迁完才删裸 TCP 后备；同池、logical return/driver/native 分断言 | 实现中 |
| WireGuard DNS UDP exchange | 旧 prepare_datagram_exchange → 同一 RawIpDevicePool，尚无标准 role/provider | C/M → A；非 TEMP 路径 | Packet/Datagram 标准消费后删旧 exchange；实际 Pending、退出/恢复 | 实现中 |
| 普通 Mieru TCP/UDP/carrier | 实际协议池已有，普通登记尚未开启 | B；M register/catalog 前置 | 完整业务派发/计量、跨 owner 代次与 native close 后删兼容 executor | 实现中 |
| VLESS/VMess/Trojan MUX 及 gRPC/H2/QUIC | 同一实际池；普通登记仅限定 prefix 子集 | B；M 公共 carrier/transport/登记 | 全部已有合法调用者、认证隔离、半关闭/空包/顺序、无第二池 | 实现中 |
| Shadowsocks SIP003 出站及剩余 packet carrier | 兼容计划持有原 Child/安全状态；宿主生产闭环未齐 | B；M process/provider 装配 | reaped Child、转交 I/O、Scope 分别证明；有效 replay 保留 | 实现中 |
| Direct 非入站 Packet | HostPacketDevice 原 driver；Unix adopt 尚未用已有 retained owner | C；M 标准 provider | 失败 acquisition 仍留原件；借入 FD、实际 driver join/matching receipt | 实现中 |
| 非例外 WG raw-IP/linked stacks | 实际 RawIpDevicePool/EndpointStackOwner；独立 driver 尚未 Scope 化 | C；M host/register | 保留方向撤权/回滚旧 TCP 连续性，候选丢弃及原件清理 | 实现中 |
| owned/borrowed TUN、Windows 原件 | 已有 TUN 原件 API；生产调用与 Windows worker 边界仍需完整接通 | C/M | 系统网络恢复及跨平台实际资格另列；不能以 worker 退出充 native receipt | 实现中 |
| 控制 HTTP/IPC/gRPC、订阅、TLS/quota/storage | 真实原件/任务表待第二波冻结 | 首波空闲 agent，M 明确转交文件 | 慢接收者/超时 ACK/持久与临时/原 join；不改消费者 | 计划内，未派发 |
| 生产预算 | DNS 固定16等内部上限不算统一生产配置 | 首波空闲 agent，M 独占转交 config 投影 | 省略字段、按需/耗尽、身份/替换、活跃和安全历史 | 计划内，待 owner 稳定 |

## 共享前置与调度

- `5be118e2`：M 提供 `connect_carrier_stream`，复用标准 Stream 前缀准备循环，
  最终角色返回实际 StreamLoan；协议与 DNS 分别持有 donor 与消费控制，不制造业务身份。
  同提交的 `OwnedBiStream::retire_logical` 只终止原 logical send/receive，
  不声明 endpoint/native/driver 已关闭。消费者回归完成前不开放生产 gate。
- 第一组开发定向排给 C：Direct 原件 acquisition 与 HostPacketDevice matching-close 回归。
  A/B 源码并行，无并发 Cargo。

## 独立保留项

`TEMP-WG-OUTER-UDP-01` 仍未关闭，仅既有 `outer_udp_proxy`、
`ProxiedRawIpWireCarrier`、pool `legacy_outer` 及重连历史；不修复、不扩大。
它不豁免其他路径的原件/provider/Scope 关闭门槛。

FinalMask 并发 UDP 返回异常及早期 cold-spawn 夹具超时保留历史未归因状态。
特权 TUN/系统 route/DNS 恢复、真实 GUI/面板同产物、Windows/macOS 和 1c1g
资格尚未执行；实现缺口与环境待验分别记录，不能用后者隐藏前者。
