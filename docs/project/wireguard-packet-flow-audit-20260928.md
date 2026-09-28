# WireGuard Packet/Flow 分层审查（2026-09-28）

## 审查范围与依据

依据用户提供的《Packet Plane + Flow Plane + Capability-based routing + Translation
Adapter》原文以及根 `AGENTS.md`，审查当前 develop 工作区的 WireGuard 桥接、数据
平面选择、公共 raw-IP 栈、网络变化恢复和本次 ping 地址适配。审查结论针对这些
实际路径，不代表对整个项目或密码实现的全面安全审计。

## 责任映射

| 责任 | 当前所有者与入口 | 结论 |
| --- | --- | --- |
| 配置形状/模式 | `crates/config/src/model/route.rs` | config 定义 `translate`，转换行为不进配置层 |
| 规则顺序、出站及模式决策 | `zero-router` + `zero-engine::runtime::route` | 不解析 IP/Echo，不按 ping 重选出站 |
| 已选候选内部最短可执行路径 | `proxy/runtime/network_graph.rs` + inventory | Packet/Stream/Datagram 三节点；转换按协议过滤 |
| 原生 Packet 收发执行 | `proxy/runtime/raw_ip/packet.rs`、`device/` | 中性计划/设备，保留源地址并执行 L3 hop/MTU |
| TCP/UDP Packet↔Flow | `zero-stack` + raw-IP 公共执行器 | WireGuard 注册公共栈操作；非协议专属 TCP/UDP 栈 |
| 显式 Echo Packet 地址适配 | `stack/echo_translation.rs` + `stack/packet/icmp/translation.rs` | 地址/标识/校验和/关联均在栈层；不创建 ICMP Flow |
| 返回队列与超时驱动 | `proxy/runtime/raw_ip/device/{returns,translation,driver,endpoint}.rs` | runtime 只持有通道、调用公共栈；普通/关联端点都可接入 |
| WireGuard 配置投影 | `proxy/adapters/wireguard/{udp,packet,inbound,lifecycle}.rs` | 适配器调用协议构造器，提供中性操作 |
| 密钥、握手、peer demux、AllowedIPs | `protocols/wireguard` | generic runtime 不导入 WireGuard/GoTATun，不以外层地址选 peer |
| 物理网络变化后的设备更新 | `platform/tokio/egress.rs` + `proxy/runtime/orchestration/devices.rs` | neutral generation 通知，串行准备/发布，旧快照及失败重试 |

## ping 与原方案的关系

原生 Packet→Packet 不需要 Echo 状态。这条路径继续成立。普通主机 TUN 的源
地址与远端允许的 WireGuard 分配地址不同，需要额外、显式的地址转换；这个适配
器才需要 Echo 在途状态。它属于正常 IP/ICMP 栈，并没有把 ICMP 定义为第三种
Flow、在 WireGuard 里复制 ICMP 栈，或在 Router 中做 Echo 请求/响应关联。

`translate` 是原方案的显式 Translation Adapter 扩展：TCP/UDP 保持 Flow 转换，
Echo 使用选中 PacketSink 的可执行地址适配器。严格 `flow` 的语义保持 TCP/UDP。
强制选择适配路径没有相应能力时终止，不把失败当作绕过策略的理由。备用候选的
配置顺序保持不变，Block 和能力不兼容仍终止 Packet 选择。

分片继续复用既有 `zero-stack::FragmentReassembler`：TUN 和 raw-IP 入站先重组后
选路，普通设备和关联端点先重组解密包后进行返回关联。Echo 转换器未复制分片
重组状态机；出站分片仍调用 stack 的纯 Packet API。

## 已修正规范问题

- 旧 `stack/packet/icmp.rs` 内联测试移到 `icmp/tests.rs`；Echo builder 移到 sibling
  `icmp/echo.rs`，保持纯 packet API，根文件回到约 220 行。
- 旧 `proxy/runtime/raw_ip/device.rs` 超过 400 行；设备驱动、健康投影和地址适配
  分拆到 `device/`，根文件约 225 行。
- Packet 会话身份由 stack 的纯 `packet_conversation_key` 提供；runtime 路由锁定
  消费中性元数据，不自行解析 Echo identifier 或复制 IP/传输头布局。

- 原始 Packet 执行器曾直接读取 IPv4 分片标志/标识；现统一调用 stack 的纯
  `ipv4_fragmentation_allowed` / `fragment_forwarded_packet`，runtime 不持有 IP 字段偏移。

## 与原文示意尚有距离的部分

不能据此宣称原文的全部扩展设想已经完成。

1. 通用宿主 Direct PacketSink 尚未实现；已有 Direct Echo 是历史宿主 socket 操作，
   明确不伪装为 PacketSink。本次 translate 对 Direct 不支持，避免形成隐式旁路。
2. PacketSink/Endpoint 执行合同当前是 proxy 内部中性接口，尚未统一成原文示例中
   可供任意外部实现使用的完整公开 `PayloadSession`/`PacketEndpoint` API。增加新
   注册协议可复用现有公共 raw-IP 操作，但不等于已具备全部公开扩展接口。
3. 图当前计算数据平面转换路径，未实现跨任意网络拓扑、延迟/MTU/安全代价的
   综合最优化；实际适配能力由 prepare 操作确认，不能只看 metadata 声明。
4. translate 当前只覆盖 TCP/UDP 与单播 ICMP Echo 及其错误还原，不是 ESP、GRE 等
   任意协议的 NAT。它们可在符合远端源地址与回程要求时使用原生 Packet 路径。

上述差距不需要把 WireGuard 密钥、握手或宿主 NAT 策略挪进 Proxy/Engine 来解决。
未来 Packet 协议可通过同一 opt-in 能力提供地址适配，扩展转换器也应继续留在栈层。

## 验证状态

用户已确认实际数据库和 yt 网站可访问。本次增加纯栈边界/并发/错误/超时测试，
并使用只允许隧道分配地址的本地 WireGuard 服务设备验证 IPv4/IPv6 加密往返及
网络变化后的恢复；测试不依赖宿主 raw ICMP socket。

最终源码快照对应的全工作区测试在 UTC 08:23:17 至 08:52:40 完成，退出码 0：
312 个已完成测试目标、2201 项通过、0 失败、155 项按定义 ignored；没有把
ignored 的外部互操作或平台测试算作通过。格式与 diff 检查均退出码 0。
日志为桌面 `zero-wireguard-ping-test-20260928.log` 和
`zero-wireguard-ping-gates-20260928.log`。

`cargo check --workspace` 在 UTC 09:00:56 完成，退出码 0，耗时 495.6 秒。
`cargo clippy --workspace --all-targets` 在 UTC 09:11:32 完成，退出码 0，耗时
636.3 秒；告警位于未修改的 route ingress、listener、UDP tuple 和配置测试代码。
优化构建 `cargo build --release --all-features --locked` 在 UTC 09:49:14 完成，
退出码 0，耗时 2122.4 秒，复用 `target/wg-tun-build-20260928` 作为构建目录。
最终桌面产物签名验证退出码 0，版本启动耗时 1.70 秒、新 JSON 配置校验耗时
0.17 秒，均退出码 0。SHA-256 为
`1aaf5b23456622d62617f61896187503932ccb15567965cd0b1d43bc7abcfc38`。
源码快照的 50 个 Rust 文件与构建前一致；这是未提交、未推送的本地工作区测试
构建。A/B 真实网络 ping 仍待用户切换新内核验收。
