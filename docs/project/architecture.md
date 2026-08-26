# 架构

本文档说明 Zero 当前稳定的分层、职责边界、主要运行路径和依赖方向。
协议支持程度、配置字段和控制面契约分别由对应文档描述，避免把易变化的实现细节堆积在架构总览中。

相关文档：

- [公开协议能力参考](https://docs.zerodenet.org/projects/core/reference/protocol-capabilities)
- [执行计划](./engine-plan.md)
- [生命周期](./lifecycle.md)
- [公开配置参考](https://docs.zerodenet.org/projects/core/configuration/)
- [控制面](./control-plane.md)
- [工程规则](./tooling.md)

## 设计原则

Zero 的架构遵循以下原则：

1. 配置、决策和网络执行相互分离。
2. 协议拥有握手、认证、加密和帧格式，通用运行时拥有任务、路由、会话和统计生命周期。
3. 通用传输能力不依赖具体协议，也不读取引擎内部的出站联合类型。
4. 协议通过聚焦的能力接口注册，运行时不使用一个覆盖所有角色的单体适配器。
5. 能在底层 crate 表达的中性抽象，不依赖上层运行时。
6. 根二进制只负责进程和命令入口，不承载领域逻辑。

## 总体分层

从外到内，仓库可以理解为以下几层：

| 层次 | 主要 crate | 职责 |
|------|------------|------|
| 应用入口 | `zero` | CLI、进程启动、日志初始化、命令执行和退出状态 |
| 外部契约 | `zero-api`、`zero-grpc`、`zero-ffi`、`zero-connector` | 控制、观测、事件和嵌入式接口 |
| 配置与决策 | `zero-config`、`zero-router`、`zero-engine` | 配置模型、规则匹配、执行计划、路由和运行状态 |
| 代理运行时 | `zero-proxy` | 入站监听、协议调度、TCP/UDP 执行、会话和任务生命周期 |
| 协议实现 | `protocols/*` | 协议握手、认证、加密、帧格式和协议会话 |
| 通用传输 | `zero-transport`、`zero-ztls` | TLS、WebSocket、HTTP/2、gRPC、HTTP Upgrade、XHTTP、QUIC 等载体 |
| 网络设备与栈 | `zero-tun`、`zero-stack` | TUN 抽象、原始 IP 数据包解析和 TCP/UDP 栈 |
| 平台实现 | `zero-platform-tokio` | 基于 Tokio 的 socket、listener、stream 和连接器 |
| 基础抽象 | `zero-traits`、`zero-core`、`zero-error` | 运行时无关特征、领域基础类型和共享错误 |

这些层次表示职责和依赖方向，不要求每个 crate 只对应一个概念。例如 `zero-proxy` 同时承担协议集成、运行时编排和平台桥接，但不应拥有协议私有语义。

## 应用入口

根 crate `zero` 是最外层应用适配器。

`src/main.rs` 只负责：

- 进程初始化；
- tracing 初始化；
- 调用应用执行器；
- 输出致命错误并设置退出状态。

命令语义位于 `src/application/`：

- `run`：启动代理；
- `inspect`：配置验证、状态和构建信息；
- `control`：通用控制面 IPC 命令；
- `tun`：TUN 相关 IPC 命令。

配置解析、路由决策、控制请求构造和 TUN 操作不应回到 `main.rs`。

## 外部契约与控制面

`zero-api` 定义控制、观测、事件和状态导出的稳定数据模型。它不是 HTTP 服务，也不绑定某一种传输。

本地 IPC、HTTP、gRPC、文件 sink、Rust API 和 FFI 都是同一能力模型的适配方式。Clash、sing-box 和 Xray 等外部生态可作为兼容参考，但外部兼容逻辑应位于网关、适配器或工具层，不应反向约束内核模型。

各支撑 crate 的职责如下：

| crate | 职责 |
|-------|------|
| `zero-api` | 控制面和观测数据类型 |
| `zero-connector` | 控制端中立的事件分发、JSONL sink 和 Webhook 投递 |
| `zero-grpc` | 可选的 gRPC 控制面适配器 |
| `zero-ffi` | C 兼容嵌入式接口 |
| `zero-logging` | 非阻塞结构化日志和事件桥接 |
| `zero-dns` | system、UDP、DoH、DoT、缓存、Fake IP 和有界 Real-IP 反向索引等 DNS 能力 |

## 配置、路由和引擎

### `zero-config`

`zero-config` 拥有：

- 配置数据类型和反序列化；
- 必填、范围和引用校验；
- 入站、出站、路由和传输组合校验；
- 对协议私有值的验证委托。

UUID、密码、cipher、密钥材料和协议身份等私有格式由协议 crate 的验证接口解析。配置层可以调用 validation-only feature，但不应复制协议解析器，也不应为验证而编译协议数据面。

### `zero-router`

`zero-router` 拥有规则集、匹配和路由选择所需的中性模型。它不建立连接，也不持有协议运行状态。

### `zero-engine`

`zero-engine` 是控制内核，主要拥有：

- 将 `RuntimeConfig` 编译为 `EnginePlan`；
- 模式语义：`direct`、`global`、`rule`；
- 出站组语义：`selector`、`url_test`、`fallback`；
- 路由决策和目标解析；
- 配置重载和持久化状态；
- selector、urltest 和健康状态；
- 会话、统计、事件和诊断；
- 控制面需要的状态投影。

`zero-engine` 不绑定 Tokio，不启动监听器，不执行协议握手，也不直接建立 socket 连接。

`direct` 和 `block` 是引擎可解析的内置目标语义。真正的 socket 直连或阻断响应由 `zero-proxy` 执行。

引擎运行时根模块保持为门面：

- `runtime/configuration.rs`：配置持久化、重载和订阅通知；
- `runtime/policy.rs`：模式、selector、urltest 和策略事件；
- `runtime/observability.rs`：统计、事件和 sink 投影；
- `runtime/diagnostics.rs`：诊断；
- `runtime/session.rs`：会话和流生命周期。

根模块保留引擎构造、路由和目标解析，不重新吸收上述状态领域。

## 代理运行时

`zero-proxy` 把引擎决策转换为网络执行，负责：

- 绑定和运行入站监听器；
- 调用协议实现完成 accept、握手和认证；
- 建立直连或上游连接；
- 执行 TCP 中继和 UDP 流；
- 执行 relay 链和传输包装；
- 管理任务、重载、关闭和连接生命周期；
- 记录会话、流量、事件和错误；
- 根据当前编译 feature 注册可用协议。

运行时可以依赖引擎、配置、协议、传输和 Tokio 平台实现，但不应重新定义模式、组和路由语义。

### 协议能力注册

运行时不存在覆盖所有职责的 `ProtocolAdapter`。协议集成按角色拆分为聚焦能力：

| 能力 | 职责 |
|------|------|
| `ProtocolSupportCapability` | 元数据、feature 和支持状态 |
| `InboundListenerCapability` | 入站绑定和监听操作准备 |
| `TcpOutboundCapability` | TCP 单跳和中继跳准备 |
| `UdpFlowCapability` | UDP 流和 UDP 中继准备 |
| `UdpPacketPathCapability` | UDP 数据包路径和 datagram 载体准备 |

`ProtocolRegistry` 保存已注册的能力对象，`ProtocolInventory` 是运行时使用的窄门面。运行时通过 inventory 请求绑定入站、连接 TCP 或启动 UDP，而不直接查找具体协议对象。

增加协议的一般步骤是：

1. 在 `protocols/*` 中实现协议语义；
2. 在 `zero-proxy::adapters` 中实现所需能力；
3. 在注册表构造阶段注册能力对象；
4. 增加配置、能力矩阵和运行路径测试。

运行时不通过匹配 `InboundProtocolConfig` 或 `ResolvedLeafOutbound` 来选择具体协议执行器。配置联合类型的解包和引擎出站叶到协议请求的投影属于对应适配器。

### 准备与执行分离

能力接口是准备边界，不是独立运行时。

- 能力实现同步校验并生成拥有所有必要状态的 prepared operation；
- `ProtocolInventory` 注入窄运行时上下文；
- 通用运行时执行 operation；
- 协议或传输对象只完成握手、认证、帧处理和载体包装。

监听循环、任务派生、路由、relay 顺序、会话生命周期、流量统计和错误归一化仍由运行时拥有。prepared operation 不持有 `Proxy`，适配器也不直接运行监听循环或自行派生长期任务。

### 运行时模块边界

`crates/proxy/src/runtime/` 按生命周期和方向拆分：

| 模块 | 职责 |
|------|------|
| `orchestration` | 顶层运行、重载和关闭编排 |
| `listeners`、`listener_loop`、`inbound_operation` | 绑定、TCP/QUIC accept 循环和入站 operation 执行 |
| `inbound_route` | 协议 accept 后的 stream、MUX 和录制流路由交接 |
| `tcp_ingress` | TCP 响应、路由、限速、中继、统计和会话生命周期 |
| `tcp_dispatch` | TCP 出站候选、单跳和 relay 前缀执行 |
| `udp_ingress` | 入站数据包进入单个 UDP 会话调度器 |
| `udp_dispatch` | 每个入站会话的路由、启动和转发状态机 |
| `udp_flow` | 持久 UDP 流模型、缓存和可恢复执行状态 |
| `udp_delivery` | UDP 响应投递与统计 |
| `udp_socket` | 直接 UDP socket 操作 |
| `datagram_udp` | datagram 型入站生命周期 |
| `stream_udp` | stream 承载的入站 UDP 生命周期 |
| `packet_session_udp` | packet-session 型入站 UDP 生命周期 |
| `udp_association` | SOCKS5 等关联型入站 UDP 生命周期 |
| `mux_session`、`mux_tcp`、`mux_udp` | MUX 会话及其 TCP/UDP 子流 |
| `pipe` | TCP/UDP 内核运行入口对象 |
| `path` | 中性的 TCP/UDP 路径和端点模型 |

门面模块只保留声明、重导出和窄委托。状态机和复杂执行逻辑应放在按职责命名的子模块中。

## TCP 运行路径

协议完成 accept、认证和目标解析后，如果结果可以归一化为 `Session + client stream`，就进入通用 TCP 生命周期：

```text
监听器
  -> 协议 accept / 认证
  -> inbound_route
  -> tcp_ingress::serve_inbound
  -> TcpPipe
  -> tcp_dispatch
  -> 直连或协议出站
  -> 双向中继
```

`InboundProtocol` 是 `tcp_ingress` 内部的客户端响应和中继边界。它允许运行时以中性方式发送成功、阻断或上游失败响应，并对特殊流执行协议感知的中继。

`serve_inbound` 负责通用 TCP 生命周期：

1. 应用 URL 重写；
2. 合并入站默认限速和用户级限速；
3. 准备并登记会话；
4. 通过 `TcpPipe` 路由并建立出站；
5. 发送协议响应；
6. 执行带空闲超时、计量和限速的中继；
7. 完成会话结果、事件和结构化日志。

并非所有入站都直接实现这一流式接口。UDP、datagram、MUX 子流、TUN 原始数据包和协议自有 packet session 先经过各自的生命周期，再在适当位置交给 TCP 或 UDP 通用入口。

## UDP 运行路径

UDP 不是 TCP 生命周期的附属分支。它有独立的会话调度和持久流状态：

```text
协议入站 / TUN / UDP association
  -> udp_ingress
  -> UdpDispatch
  -> 路由和候选选择
  -> direct / relay / datagram / stream-packet / packet-path
  -> udp_flow
  -> udp_delivery
```

主要职责划分：

- `udp_dispatch`：每个入站会话的路由、候选启动、转发和 fallback；
- `udp_flow`：跨数据包存在的出站流、缓存、恢复状态和 handler 注册；
- `udp_flow::managed`：可恢复 stream/datagram 流的通用执行机制；
- `udp_flow::registered`：由协议注册的中性上游 handler；
- `udp_delivery`：响应投递、响应统计和客户端回写。

通用 UDP 状态使用中性路径类别，不保存 VLESS、VMess、Trojan 等协议命名的流枚举。协议私有的 resume、codec、packet framing、cache key 和 manager 请求由协议 crate 或对应适配器构造，并以不透明状态交给运行时。

UDP 数据包路径通过 `UdpPacketPath`、`DatagramCodec` 等中性接口组合。运行时负责链路编排和生命周期，协议负责数据包编码、解码和认证状态。

## 协议层

外部协议实现位于 `protocols/`：

- `socks5`
- `http`
- `vless`
- `hysteria2`
- `shadowsocks`
- `trojan`
- `vmess`
- `mieru`

协议 crate 主要拥有：

- 握手和认证；
- 协议身份、密码、UUID、cipher 和密钥解析；
- 加密状态；
- TCP、UDP 和 MUX 帧格式；
- 协议请求、响应和会话对象；
- 协议私有的 UDP flow plan 和 resume 状态；
- 可以与代理运行时分离的 accept、connect 和 relay 语义。

协议 crate 不依赖 `zero-config`、`zero-engine`、`zero-router` 或 `zero-proxy` 的联合类型。部分协议的 runtime feature 可以依赖 `zero-transport` 和 `zero-platform-tokio`，用于复用通用载体。

协议 UDP 类型不从 crate 根目录集中重导出，应通过明确的 `protocols::<name>::udp` 模块访问。

## 通用传输层

`zero-transport` 提供可复用的载体和流包装能力，包括：

- TLS；
- WebSocket；
- gRPC；
- HTTP/2；
- HTTP Upgrade；
- XHTTP，配置字段仍为 `split_http`；
- QUIC；
- 录制流、计量流和限速中继；
- 协议集成所需的中性 transport bridge 契约。

`zero-transport` 不读取 `ResolvedLeafOutbound`，也不负责从引擎联合类型构造具体协议请求。

需要区分两个概念：

- 通用载体实现属于 `zero-transport`；
- 某协议如何选择、组合并使用这些载体，属于协议 crate 和对应代理适配器。

例如 TLS、WebSocket 和 XHTTP 的通用实现属于 transport；VLESS 的 transport plan、握手和接受结果属于 VLESS；从引擎出站叶投影为 VLESS 请求则属于 VLESS 代理适配器。

`zero-ztls` 提供支持自定义 ClientHello 的 TLS 1.3 客户端能力，供 transport 在单跳和 relay 最终跳中使用。

## 网络栈和 TUN

### `zero-stack`

`zero-stack` 在原始 IP 数据包与 TCP stream/UDP datagram 之间转换。

它提供：

- `UserTcpStack`：用户空间 TCP 终止；
- `UserUdpStack`：用户空间 UDP 数据包处理；
- `UserNetworkStack`：组合 TCP 和 UDP 栈；
- `SystemTcpStack`、`SystemUdpStack`：使用操作系统 socket 的实现。

`crates/stack/src/packet.rs` 只负责纯数据包解析和构造。异步连接状态、channel、timer 和任务生命周期位于相邻的 TCP/UDP 模块，不能在生命周期代码中重新实现 IP 解析。

### `zero-tun`

`zero-tun` 定义平台无关的 TUN 设备抽象、Linux/macOS/Windows 设备实现和事务化系统路由。平台驱动的部署仍由最终应用或安装器负责。自动路由默认同时用两组 `/1` 接管 IPv4/IPv6，并按地址族记录和绑定各自的物理出口（例如 Windows 的 IPv4 以太网与 IPv6 Teredo 可以不同）；macOS 还为绑定物理接口的 socket 维护 interface-scoped 默认路由，避免 `/1` 接管后物理出口查询直接返回不可达。路由事务持有跨进程 lease，并持久记录 Zero 安装的半默认路由、显式目的网络排除项和 scoped 绕行项；异常退出后的同名设备启动先恢复残留项。

TUN 反环路由两类互不替代的机制组成：捕获路由只负责让应用流量进入 Zero；运行时拥有的 TCP/UDP/QUIC socket 工厂负责在系统路由仍指向 Zero TUN 时把自身流量绑定到 underlay 出口。socket 工厂必须先以无发包的本地路由探测取得内核选择的源地址；命中 TUN 地址才强制物理出口，命中 LAN、企业 VPN 或其他更具体路由时必须保留系统选择。启动时必须先解析并发布 IPv4/IPv6 underlay，再安装对应 `/1` 捕获路由。代理节点地址不属于目的网络排除，不能在 TUN 启动或协调期间被枚举、解析或安装为 `/32`/`/128` host route；当前仅为尚未完全出口感知的 DNS bootstrap 保留显式排除。额外 carrier socket（例如 QUIC、split-HTTP 第二连接和 UDP packet path）必须复用同一出口工厂。

TUN UDP association 以原始客户端源 IP/端口为身份，同一源关联内再按目标复用 UDP flow；不同源端口不能仅因目标相同而合并，否则响应会写回错误客户端。关联建立有并发硬上限和滚动速率限制，单关联队列使用非阻塞投递，关闭的关联和失败的目标 flow 都应用有界指数退避。这样自捕获或 `WSAENOBUFS` 只能造成受控丢包，不能演化为无界 association/session 重建。TUN 会话必须记录原始源 IP/端口，不能用 loopback 占位地址代替。

TCP 入站身份包含完整源 IP/端口；平台 listener 不得把 peer 降级成只有 IP 的值。TUN 用户态 TCP 栈对排队待 accept 的连接附加单调代际，并在交付前确认同一四元组仍指向该代连接且处于 established 状态，避免 RST 后同四元组快速重连时交付过期连接。目标地址相同本身不构成重复连接，不能按目标合并独立 TCP stream。

本机进程归属属于 `zero-platform-tokio`：Linux、Windows 和 macOS 分别使用平台连接表与进程接口，proxy 只消费中性的 PID/name/path 结果。TCP 与 UDP 都以完整本地源端点查询；查询是尽力而为的观测补充，套接字竞态、权限或平台错误只产生空元数据，不得改变路由或使会话失败。系统表读取在阻塞任务池执行，不能阻塞异步转发工作线程。

`auto_route=true` 还会通过平台原生通知持续观察主机路由拓扑：Windows 使用 IP Helper 路由/接口回调，Linux 使用 `NETLINK_ROUTE`，macOS 使用 `PF_ROUTE`。通知只是有界的失效信号；`zero-proxy` 中单一的 TUN route reconciler 对突发事件防抖，重新解析非 TUN 默认出口和仍需保留的显式 DNS/bootstrap 排除集合，再调用 `zero-tun` 的事务化 guard 原地协调。同步平台命令在阻塞线程池执行，失败保留上一份可用状态并退避重试。提交成功后只替换新建 socket 读取的物理出口，既有连接和 TUN/用户态网络栈不重启；`auto_route=false` 不创建监听任务。这一运行期协调属于 #21；代理端点 host route 不得重新进入该流程。

`strict_route=true` 还要求 `zero-tun` 在捕获路由提交后安装平台泄露保护：Linux 使用独立 nftables table，macOS 使用 `com.apple/*` 下的独立 pf anchor，Windows 使用 Windows Firewall（WFP 执行层）的稳定 rule group 与 profile outbound policy。保护只允许 loopback、受管 TUN 接口、Zero 自身的 underlay 出站以及显式 bootstrap/端点地址；最后规则必须 fail closed。资源名由 TUN 入站 tag 稳定派生，热协调以原子替换或先放行后收紧的事务更新；更新失败继续保留旧保护。正常停止先恢复防火墙状态再删除路由，异常退出则保留保护并由同 tag 的下一实例接管。平台权限、工具或恢复日志不可用时严格模式启动失败，不能降级为仅安装路由。

TUN 入站的基本路径为：

```text
TunDevice
  -> 原始 IP 数据包
  -> NetworkStack
  -> TCP stream 或 UDP datagram
  -> zero-proxy 运行时
```

当前 TUN 入站使用 `UserNetworkStack`，TCP 和 UDP 都进入 `zero-proxy` 的统一入口生命周期。系统重定向入站使用 `SystemTcpStack`，它们不是运行时可随意互换的同一个配置对象。声明式 `runtime.tun` 由代理编排层随启动、配置重载、回滚和关闭统一管理；命令式 `tun start/stop` 只用于未声明 `runtime.tun` 时的临时控制。

## 基础抽象和平台实现

### `zero-traits`

`zero-traits` 是 `#![no_std]` 的运行时无关抽象层，包含：

- socket、listener 和 datagram 抽象；
- TCP/UDP/NetworkStack 抽象；
- 地址和传输类型；
- 协议元数据及 TCP/UDP 行为边界；
- DNS 和 TLS 等平台服务接口。

它不能依赖 Tokio、配置、引擎、代理或具体协议。

### `zero-core`

`zero-core` 在 traits 之上提供共享领域类型，例如：

- `Session`
- `Address`
- 协议类型
- 入站 TCP/UDP 中性请求和响应接口
- 共享错误桥接

### `zero-platform-tokio`

`zero-platform-tokio` 实现 `zero-traits` 中的运行时抽象，包括 Tokio socket、listener、datagram socket、relay stream 和 transport connector。

当前只有 Tokio 平台实现。未来增加其他运行时，应实现相同 traits，而不是把运行时分支散布到 engine 或协议 crate。

## 依赖方向

核心依赖方向如下：

```text
zero
├── zero-api
├── zero-config
├── zero-engine
├── zero-logging
├── zero-proxy
├── zero-connector（可选）
└── zero-grpc（可选）

zero-proxy
├── zero-config
├── zero-core
├── zero-engine
├── zero-platform-tokio
├── zero-stack
├── zero-traits
├── zero-transport
├── zero-tun
├── zero-dns
└── protocols/*（按 feature）

zero-engine
├── zero-api
├── zero-config
├── zero-core
├── zero-error
└── zero-router

zero-transport
├── zero-core
├── zero-error
├── zero-platform-tokio
├── zero-traits
└── zero-ztls

protocols/*
├── zero-core
├── zero-traits
├── zero-transport（部分 runtime feature）
└── zero-platform-tokio（部分 runtime feature）

zero-core -> zero-traits
zero-stack -> zero-traits
zero-tun -> zero-traits
```

补充规则：

- `zero-transport` 不依赖 `zero-config` 或 `zero-engine`；
- 协议 crate 不依赖 config、engine、router 或 proxy；
- `zero-engine` 不依赖 Tokio 和具体协议数据面；
- `zero-traits` 位于依赖底部；
- validation-only 协议依赖只用于协议私有值校验，不建立反向运行时依赖。

实际依赖以各 crate 的 `Cargo.toml` 为准。修改依赖关系时，应同步更新本文档并运行 workspace 架构测试。

## 架构约束的验证

架构不仅依赖文档约定，还通过测试约束关键边界，包括：

- crate 依赖方向；
- `main.rs` 与 `src/application/` 的职责分离；
- engine runtime 门面边界；
- proxy runtime 门面和子模块职责；
- 协议 registry/inventory 调度；
- TCP/UDP prepare/execute 分离；
- stack 数据包解析与异步生命周期分离；
- 禁止运行时重新引入协议命名的 UDP 状态。

文件可以在不破坏职责的前提下重组。架构测试应优先约束所有权、依赖和执行边界，而不是把偶然的目录形状永久固化为设计本身。
