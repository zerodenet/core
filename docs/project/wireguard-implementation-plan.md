# WireGuard 实现计划

本文固定 Zero `v0.0.3-dev.*` 开发线中的 WireGuard 范围、分层、运行路径与验收门禁。实现以当前 Zero 能力边界、WireGuard 官方协议和固定互操作基线为准，不把 WireGuard 当成普通的流式代理协议。

## 1. 版本与基线

- Zero 开发线：`v0.0.3-dev.YYYYMMDDHHMM`，只从 `develop` 发布。
- 协议基线：WireGuard protocol v1，以[官方协议说明](https://www.wireguard.com/protocol/)和[官方白皮书](https://www.wireguard.com/papers/wireguard.pdf)为准。
- 行为与配置对照：Xray-core `v26.3.27`，commit `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`。该基线用于固定 TCP/UDP、IPv4/IPv6、peer、allowed IP、keepalive、MTU 和 endpoint 解析的互操作目标，不要求复制 Xray 的 Go/gVisor/kernel-TUN 内部结构。
- Rust 协议引擎候选：BoringTun `0.7.1`。只允许精确固定版本或审计后的仓库内补丁；不得跟随 `master`。在完成消息上限、重放、cookie、定时器、密钥擦除和畸形包审计前，不进入默认 `full` feature。
- `protocols/wireguard` 的 package version 跟随最终选定并固定的 Rust 协议引擎版本；Zero 产品版本仍由 workspace version 管理。

## 2. 范围决定

发布 `v0.0.2-rc.*` 后，仓库进入两条长期并行线：

- `main` 只维护 `0.0.2-rc.*`，接收 RC 缺陷修复、兼容性修正和优化；后续候选版本继续使用递增的 `v0.0.2-rc.YYYYMMDDHHMM`。
- `develop` 只推进 `0.0.3-dev.*`，WireGuard 及其所需的通用网络栈、trait 和 adapter 工作不得提前进入 `main`。
- 同时适用于两条线的修复先按发布风险选择落点；从 `main` 进入的通用修复必须受控 forward-port 到 `develop`。不得再次将浮动 `develop` 整体合入 `main`，也不得让未完成的 WireGuard 随修复反向进入 RC。
- 两条线分别执行版本契约、CI 和发布标签；`main` 的 RC 成功不代表 `develop` 的 WireGuard 已验证，`develop` 的 dev 成功也不改变 RC 的发布范围。

WireGuard 是承载 IPv4/IPv6 包的三层隧道，不是建立一条应用层 TCP 字节流的协议。因此实现分为两个方向：

1. **出站**：Zero 的 TCP/UDP 会话通过一个共享的 WireGuard peer/device 进入远端网络。必须具备可主动建立 TCP 和 UDP 会话的用户态客户端网络栈。
2. **入站**：UDP socket 收到的 WireGuard 包在鉴权解密后产生 raw IP 包，再交给现有 `zero-stack` 终止 TCP/UDP，并进入统一入站路由与会话观测流水线。

首个可合并里程碑先完成出站；入站在同一 `v0.0.3-dev.*` 开发线中作为独立里程碑实现和验收。任何里程碑都不得以“能握手”替代双向 payload、重协商、keepalive、MTU 和故障恢复证据。

首期明确不包含：

- 操作系统 kernel WireGuard 接口、`wg-quick`、路由表或 iptables 管理；
- 从 WireGuard 配置自动修改宿主机 DNS；
- 未经定义的 Xray 私有字段透传；
- 将 WireGuard 密钥、握手包或解密后的 raw IP 内容写入事件、状态快照或普通日志；
- 在没有 packet-path 证据时宣称 WireGuard 外层 UDP 可以经过任意 relay chain。

## 3. 分层与所有权

### `protocols/wireguard`

- 拥有 private/public/pre-shared key、peer、allowed IP、reserved bytes、握手、cookie、重放窗口、rekey、keepalive 和 WireGuard 包编解码。
- 提供 `validation` 与 `runtime` feature；配置层只依赖 `validation`。
- 接收抽象时钟、随机源和 UDP/raw-packet I/O，不直接创建 Tokio socket、TUN 设备、路由表或进程任务。
- 不依赖 `zero-config`、`zero-engine`、`zero-proxy` 或具体控制面。

### `crates/traits`

- 只在确有跨运行时需求时增加最小的客户端网络栈或 raw-IP device trait。
- trait 不出现 WireGuard、peer、key、allowed IP 等协议名或协议私有字段。
- 不把 Tokio 类型带入 `zero-traits`。

### `crates/stack`

- 现有 `UserNetworkStack` 保持 raw-IP 入站终止职责。
- 新增独立的客户端网络栈：接收 WireGuard 解密后的 IP 包，主动提供 TCP connect 与 UDP socket，并把生成的 IP 包送回 WireGuard device。
- TCP 状态机、UDP 端口分配、分片/MTU/ICMP 错误属于网络栈，不进入协议 crate 或 proxy adapter。
- 客户端栈与现有入站栈共享纯 packet 解析/构造代码，但不把两个方向的生命周期混成单一 facade。

### `crates/config`

- 增加 `OutboundProtocolConfig::Wireguard`，随后入站里程碑再增加对应 inbound variant。
- 只保存配置 ADT、结构/引用校验，并调用 `protocols/wireguard` 的 validation API 校验密钥和协议私有值。
- 不复制 base64/hex key parser，不产生运行态 peer/device，也不持有明文派生密钥。

### `crates/platform/tokio`

- 实现协议/栈所需的 UDP socket、timer、task 和网络解析适配。
- socket 的 egress/bind 行为继续走现有 platform/runtime 服务，不由协议 crate 直接 `UdpSocket::bind`。

### `crates/proxy`

- `WireguardAdapter` 只做 `OutboundProtocolConfig` 投影、共享 device 生命周期和显式 capability 实现。
- TCP 使用 `TcpOutboundCapability` 的同步 preparation 边界；执行期返回客户端网络栈建立的 stream，通用 runtime 继续负责 relay、accounting、结果归一和错误映射。
- UDP 使用 `UdpFlowCapability`；协议 adapter 不复制通用 UDP session/cache/manager。
- reload 时以配置身份复用或替换共享 device；密钥、peer、endpoint、allowed IP、MTU 变化必须构建新状态并原子切换，失败保留 last-known-good。
- 不在 runtime 根、`tcp_dispatch` 或 `udp_flow` 中增加对 `OutboundProtocolConfig::Wireguard` 的 match。

### `zero-engine` 与控制面

- 继续只接收通用 session、stats、health 和事件，不识别 WireGuard 密钥或包格式。
- 能力发现可以报告 TCP、UDP、IPv4、IPv6、inbound/outbound 和编译 feature；不得把一次握手成功解释为完整健康。

## 4. 配置契约

出站配置的目标形状如下，最终字段名以 config tests 固定：

```yaml
outbounds:
  - tag: wg-out
    protocol:
      type: wireguard
      private_key: BASE64_OR_HEX_32_BYTES
      addresses:
        - 172.16.0.2/32
        - fd00::2/128
      mtu: 1420
      peers:
        - public_key: BASE64_OR_HEX_32_BYTES
          pre_shared_key: OPTIONAL_BASE64_OR_HEX_32_BYTES
          endpoint: example.com:51820
          allowed_ips:
            - 0.0.0.0/0
            - ::/0
          keepalive_secs: 25
          reserved: [0, 0, 0]
```

校验要求：

- private/public/pre-shared key 解码后必须恰好 32 bytes；错误信息不得回显密钥。
- 至少一个本地 tunnel address 和一个 peer；地址必须带 prefix。
- outbound peer 必须有 endpoint；域名解析沿用 Zero DNS 与 egress 策略。
- `allowed_ips` 不能为空，peer 选择使用 longest-prefix match；相同优先级的冲突必须拒绝，不能依赖配置顺序静默覆盖。
- `reserved` 只能为空或 3 bytes；非零值是显式兼容扩展，不属于标准 WireGuard 能力。
- MTU 默认 1420；IPv6 启用时不得低于 1280。上限、封装开销与分片行为必须由测试固定。
- keepalive 使用秒，`0` 表示禁用；范围必须适配最终协议引擎的无损类型。
- 私钥和 peer 公钥不得相同；重复 peer、公钥或完全重复 allowed-IP 声明必须拒绝。

## 5. 运行路径

### 出站 TCP

```text
route decision
  -> ProtocolRegistry / WireguardAdapter prepare
  -> shared WireGuard device
  -> client network stack TCP connect
  -> encrypted IP packets over UDP carrier
  -> generic TCP relay/accounting
```

### 出站 UDP

```text
UDP session
  -> UdpFlowCapability prepare
  -> client network stack UDP socket
  -> raw IP packet
  -> WireGuard encrypt / peer selection
  -> UDP carrier
  -> response decrypt / client stack / generic UDP response path
```

### 入站

```text
UDP listener
  -> WireGuard cookie/handshake/auth/decrypt
  -> allowed-IP source validation
  -> raw IP packet
  -> zero-stack TCP/UDP termination
  -> common inbound route/session/accounting pipeline
```

外层 endpoint 的 DNS、socket 创建和 egress 选择属于 runtime/platform 服务。内层目标的 DNS 是否在 tunnel 内解析必须是明确配置与测试结论，不能因系统 resolver 行为偶然成立。

## 6. 生命周期与安全不变量

- 一个配置身份对应一个共享 device；不得为每条 TCP 连接重新握手或创建独立 peer 状态。
- 空闲时仍运行 rekey、cookie reset、keepalive 和 session expiry 定时器。
- 只在通过鉴权的包上接受 endpoint roaming；先校验 receiver index、MAC、AEAD、counter/replay window，再更新 endpoint。
- ingress raw IP 的 source 必须匹配已认证 peer 的 allowed IP；egress destination 必须通过 longest-prefix peer 选择。
- 遵守 WireGuard 的 message/time/key rejection limits；达到上限必须 rekey 或拒绝，不能继续复用旧 keypair。
- 所有 key material 使用可擦除容器；reload、失败和 shutdown 都要清除旧状态。
- 有界化 handshake queue、decrypt queue、fragment buffer、UDP association 和 per-peer 状态；不得让未认证包创建无界任务或会话。
- 配置、Debug、错误、metrics、snapshot、event 和 panic 路径都不得输出 private/pre-shared key。
- reload 先完整校验和构建新 device，再原子替换；失败不影响旧 device。旧 device 有界 drain 后终止。
- outbound health 至少区分：未启动、endpoint 未解析、未握手、最近握手、数据可达、退化和停止。

## 7. 实现里程碑

### M0：开发线与契约

- 开启 `v0.0.3-dev.*`；
- 固定本文、基线 commit、feature 名和配置样例；
- 完成 Rust 协议引擎依赖/许可证/安全审计结论。

### M1：协议与验证

- 新建 `protocols/wireguard`；
- key/peer/allowed-IP validation；
- handshake、transport data、cookie、replay、timers 的单元与向量测试；
- 私密字段 redaction 和 zeroize 测试。

### M2：客户端网络栈

- 在 `zero-stack` 增加主动 TCP/UDP 栈；
- IPv4/IPv6、ICMP、MTU、fragmentation、TCP close/reset、UDP error 的确定性测试；
- 使用纯内存 raw-IP device 测试，不依赖特权 TUN。

### M3：出站 adapter

- config variant、feature wiring、registry registration；
- prepared TCP/UDP operations、共享 device、reload/reconcile、health；
- direct outer UDP；如需 relay chain，必须通过独立 packet-path 设计与互操作证据后启用。

### M4：入站 adapter

- UDP bind/preparation 进入 `InboundListenerCapability`；
- neutral raw-IP peer operation 进入 runtime-owned listener/task 生命周期；
- 复用 `zero-stack` 与 common inbound route，不建立 WireGuard 专用路由流水线。

### M5：生产验收

- 全 workspace fmt、check、clippy、all-features tests 和 release build；
- 固定端口外部套件串行执行；
- 双向 TCP/UDP payload、IPv4/IPv6、DNS、MTU/大包、keepalive、rekey、endpoint roaming、多 peer、reload 和故障恢复；
- Linux/macOS/Windows 构建；运行态特权测试与非特权用户态测试分别报告；
- 产出 capability matrix，分开标记已实现、已互操作验证、仅单元验证、缺失和明确排除。

## 8. 外部互操作矩阵

最低接受矩阵：

| 方向 | 对端 | TCP | UDP | IPv4 | IPv6 | keepalive/rekey |
|---|---|---:|---:|---:|---:|---:|
| Zero outbound | wireguard-go 固定 commit | 必须 | 必须 | 必须 | 必须 | 必须 |
| Zero outbound | Xray `v26.3.27` | 必须 | 必须 | 必须 | 必须 | 必须 |
| Zero inbound | wireguard-go 固定 commit | 必须 | 必须 | 必须 | 必须 | 必须 |
| Zero ↔ Zero | 同版本 | 必须 | 必须 | 必须 | 必须 | 必须 |

每个通过项都必须验证请求与响应 payload，而不是只验证 handshake、socket 建立或进程存活。外部套件共享固定端口时串行运行，timeout/reset 必须隔离重跑后再归因。

## 9. 合并门禁

WireGuard feature 进入默认 `full` 之前必须同时满足：

1. 协议引擎安全审计项关闭，依赖精确固定；
2. config/runtime 边界测试确认 generic runtime 不匹配 WireGuard config variant；
3. 出站 TCP/UDP 的 wireguard-go 与 Xray 固定基线互操作通过；
4. reload/last-known-good、密钥 redaction、资源上限和 shutdown 测试通过；
5. 三平台编译与 workspace 全量门禁通过；
6. 文档明确列出尚未实现的 inbound、relay-chain 或 kernel-TUN 能力，未完成项不得由 capability discovery 报告为支持。
