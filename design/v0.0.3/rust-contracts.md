# Rust 契约表达与详细签名

[方案入口](README.md) · [通道与执行契约](contracts.md) · [生命周期与事务](lifecycle.md)

本文件保留全部目标 Rust 声明及擦除、所有权、取消规范；PR02 仍需编译与 no_std/feature 证据。

<!-- source-section: 11 -->
# Rust 契约表达规则

## 类型和可见性

开放的实现集合使用 trait，封闭业务状态使用 enum，关联类型表达协议流、响应器和会话之间的关系。Newtype 区分 SessionId、ComponentId、OutboundId、Generation 等身份，禁止用可互换的 String 或整数到处传递。

ValidatedConfig、PreparedUpdate 等只用于真实阶段约束。构造字段私有，提交消耗所有权；不为每个函数建立一个同义包装。类型系统防止非法调用顺序，运行时仍验证端口、网络、并发和外部副作用。

纯类型和运行时无关契约集中在 traits 与 core；Tokio 运行接口放 zero-platform-tokio::contract，运行错误复用 zero-error。新增依赖只允许 platform → core/error → traits，不能让 core 反向引入 Tokio，或让公共契约引用 config、engine、proxy 私有类型。协议通过可选 runtime feature 实现这些接口，validation 仍保持轻量。具体目标方法、所有权和行为见公共 Rust 契约详细规格，PR02 必须完成编译及依赖门禁。

## 静态与动态分发

协议实现内部采用具体类型、关联类型和泛型。异构组件集合与异构通道需要动态分发时，在清晰的边界擦除类型。组件实例擦除、异构流擦除和异步 Future 装箱是不同边界，不能宣称整条路径只发生一次分配。

公共装配辅助代码自动完成动态包装，协议作者不重复实现第二套私有接口。动态接口只保留实际操作，禁止重复的 claim、prepare、leaf 层层装箱。

Stream 原生接口摘要，相关数据类型、动态门面及其他角色见后文详细规格：

```rust
pub trait StreamOutbound: Send + Sync + 'static {
    type Stream: StreamIo + 'static;

    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> impl Future<Output = Result<Connected<Self::Stream>, ChannelError>> + Send;
}
```

ConnectRequest、OpenContext、Connected、StreamIo 和 ChannelError 的内容及行为在详细规格中定义，不能由各协议自行改变。动态门面采用统一 BoxFuture/poll 转换；原生 async fn 或返回 impl Trait 的方法不直接作为普通 dyn Trait 方法。PR02 需证明关联类型、非 Sync 接收器和 Pin 流的接入可行，不把文档声明当作已经编译验证。[Rust dyn 兼容规则](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility)。

不得为了消除一次虚调用把整个内核泛型化。性能验收同时测运行成本、二进制大小与构建成本。

## 所有权与异步

Send、Sync、Clone 各自按用途要求。构造器可共享不意味着接收器可并发；单消费者状态可以只要求 Send。网络等待期间不持有覆盖全组件的互斥锁；锁保护短暂的索引或状态交换，明确锁顺序。

任务可以拥有必要的 Arc 或值，但不能为取得 static 生命周期捕获整个 Proxy。协议构造只接收显式服务，单次调用通过借用或必要的拥有值取得依赖。共享资源的所有者必须能关闭并等待内部任务。

recv、accept、握手和 send 分别写明取消安全性。取消 Future 不等于撤回已发送的数据；已交付的连接或报文不能在再次调用时重复出现。

## 错误缓冲和不安全边界

协议内部保留具体错误，组件边界归一化为阶段、原因、影响范围、投递状态与重试条件，保存来源链。公共 API 错误码由适配器稳定映射；重试不解析错误字符串。

正常数据路径不通过 Any 和 downcast 恢复类型。UDP 会话保存并执行自身行为，私有协议状态保持在具体实现内。

缓冲使用借用、切片和必要的所有权转移，复用 PacketBuffer 等现有设施。Bytes 等共享缓冲可用于适合的只读分片，但加密、重组和载体差异仍可能复制，不承诺未经测量的零拷贝。

不为性能猜测增加 unsafe。FFI 和系统设备等已有边界需要独立说明指针有效性、生命周期和关闭顺序。内核内部错误不能通过 panic 跨越 ABI。
<!-- /source-section: 11 -->

<!-- source-section: 12 -->
# 公共 Rust 契约详细规格

本节规定目标接口和行为，不是现有导出的 API，也不表示运行实现已完成。协议作者实现有业务含义的角色；公共装配代码完成配置类型擦除、异构通道包装和运行时接入，不要求协议再实现 claim、leaf、prepared operation 或 handler 探测链。

## 模块与依赖

| 目标位置 | 内容 | 禁止依赖 |
| --- | --- | --- |
| zero-traits | no_std 地址、报文存储、拨号硬约束及必要纯契约 | Tokio、config、engine、proxy |
| zero-core | SessionId、ComponentId、RoleId、Generation、Destination、认证事实和纯领域结果 | Tokio、全局配置及外部 API DTO |
| zero-error | 标准运行错误、阶段、投递及来源链 | config、engine、proxy |
| zero-platform-tokio::contract | component、stream、datagram、packet、inbound、network、lifecycle 运行接口及 zero-error 的薄导出 | zero-config、zero-engine、zero-proxy、具体协议 |
| zero-config | V1 解码、公共结构和引用验证、编译期注册的协议配置解析校验 | 协议 runtime feature、设备或任务启动 |
| zero-runtime | 已验证配置到实例的装配、配置事务和作用域所有者 | 被协议运行接口反向依赖 |

zero-platform-tokio 可以增加对 zero-core 的单向依赖；zero-core 和 zero-traits 继续保持 no_std，不为放置 Tokio 运行接口打开默认运行时依赖。公共 contract 模块只定义协作接口和必要小工具，平台实现放其现有兄弟模块；不新增一个 crate 来承接每个 manager。zero-runtime 仍仅在 M4 提取。注册与验证的泛型装配位于上层，协议 factory trait 位于公共 contract，不形成 runtime 到协议再回到 runtime 的环。

## 输入类型与授权范围

| 类型 | 固定内容与限制 |
| --- | --- |
| Destination | zero-core 的 Address 与 u16 端口组成的业务或载体目标；使用处区分两种角色，不另造协议专用目标枚举 |
| RequestFacts | SessionId、入站 RoleId、原始及有效来源、可选认证主体、目标来源、必要 SNI 等不可变事实；不含 Engine、Proxy 或完整配置 |
| ConnectRequest | 一个 Stream 业务 Destination；协议服务器地址来自协议已验证 Options |
| DatagramRequest | 固定目标或多目标约束、逻辑关联身份；无目标的 UDP ASSOCIATE 不伪造地址 |
| PacketRequest | IP 家族、MTU、地址与返回关联约束、Endpoint 身份；不把原始 IP 当普通 UDP |
| OpenContext | 当前请求身份、单调截止时间、取消句柄、受约束 UpstreamTransport、可选标准建连事实观察器 |
| BuildContext | ComponentId、限定的文件加载入口、组件任务登记句柄及取消域；按实际角色提供监听、设备或声明的后台控制载体能力 |
| ComponentSpec | 预期角色、资源所有者身份、显式依赖、静态配置身份、监听及共享设备声明；纯描述，不创建资源 |
| ComponentInstance | 唯一生命周期所有者和实际角色句柄集合；多个角色可以共享同一协议内部资源 |
| ChannelError | 阶段、原因、影响范围、投递状态、重试许可及保留的错误来源链；外部错误码另由 API 适配 |
| CloseReport | 已退出的任务和 I/O、释放的自有资源、恢复结果及未完成项；存在未完成项不得宣称关闭完成 |

OpenContext 的 upstream 可按需克隆为受限的拥有句柄，支持协议池重连；它不能携带注册表、完整宿主或最终出站自身的池。BuildContext 的监听与设备能力限定于已声明的组件资源；普通出站只能通过调用时的 upstream 打开载体，不能通过另一个全局 Network 句柄绕过选定链路。限速和撤权控制保留在内核，不通过原始协议字段传递。

## Factory 与配置一次转换

协议包导出轻量 RawOptions、解析校验函数和运行 Factory。RawOptions 的反序列化与 validate 只启用 validation 表面；validate 返回协议拥有的强类型 Options。注册装配负责把已有 V1 形状适配成 RawOptions，不在通用运行层解读 UUID、密码、cipher、密钥或证书含义。注册时保存具体解析函数和 factory 的关联，不在运行时通过 Any 找回 Options。

```rust
pub trait ComponentFactory: Send + Sync + 'static {
    type Options: Send + Sync + 'static;

    fn describe(&self, options: &Self::Options) -> ComponentSpec;

    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError>;
}
```

注册表的公共泛型辅助把协议的 decode_validate、Options 与 Factory 组合成一个冷路径构建配方；类型擦除只在这个异构配置集合发生。构建配方持有已验证配置与构建函数，不持有候选 socket、池、设备或已启动任务，也不是每次 connect 再构造的执行 operation。旧 V1 配置枚举仅作为迁移适配，最终通用配置只保留协议 kind 和兼容的配置文档形状；协议私有字段类型由协议拥有。未知协议、未编译 feature、字段路径和未知字段的错误语义按基线保留。

describe 必须无副作用；build 只能由宿主在替换目标确认关闭后调用，同步创建实例状态和唯一生命周期所有者，不绑定 socket、打开池连接、启动设备或后台 I/O。宿主先持有 ComponentInstance，再调用其生命周期 start 完成有副作用的异步启动；start 失败时所有者仍存在，可执行 shutdown 并核对清理，不能因 factory 返回 Err 把半建成资源遗失。角色句柄不各自执行设备关闭；公共 erase_stream、erase_datagram 等辅助只完成真实角色的类型擦除，不要求空能力实现。

## Stream 与延迟确认

```rust
pub trait StreamIo: AsyncRead + AsyncWrite + Send {}
impl<T: AsyncRead + AsyncWrite + Send> StreamIo for T {}

pub type BoxStream = Pin<Box<dyn StreamIo>>;

pub enum PeerConfirmation {
    Confirmed,
    Deferred,
}

pub struct Connected<S> {
    pub io: S,
    pub peer_confirmation: PeerConfirmation,
}

pub trait StreamOutbound: Send + Sync + 'static {
    type Stream: StreamIo + 'static;

    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> impl Future<Output = Result<Connected<Self::Stream>, ChannelError>> + Send;
}
```

StreamIo 是 Rust 当前 trait object 组合所需的结构性聚合，操作来自 AsyncRead/AsyncWrite；它不是声明支持却没有行为的 capability 标记。具体流不统一要求 Sync 或 Unpin。公共异构包装采用 Pin<Box<dyn StreamIo>>，协议内部继续使用具体流类型与关联类型。

Connected 表示本地已取得可用逻辑通道；Confirmed 才表示远端已完成协议要求的确认。Deferred 由协议流继续验证响应，必要时通过 OpenContext 的标准观察器上报 PeerConfirmed 或 PeerRejected；无需为每个连接增加一个后台确认任务或 watch 通道。通道取得、请求提交、远端确认和业务成功是不同事实，不能提前记为同一成功。

读写包装器自己保留部分帧、加解密和未完成写状态。非零 write 返回表示接受了相应明文字节，不等于远端收妥；flush 和 shutdown 必须按协议发送剩余帧或关闭标记。单向 EOF 不等于整个连接完成，另一方向仍可继续。错误保留来源并上报标准阶段，不要求内核拆解私有响应或解析错误字符串。

## Datagram 与 Packet 收发

数据面采用 poll 接口，默认发送端和接收端各有一个独立所有者，以便并发上下行且不要求接收器 Sync。公共 async 辅助使用 poll_fn，不为每个报文强制分配 boxed Future。需要共享发送的协议可以在自己的发送器里完成有界序列化，不能产生多消费者争抢同一个接收队列。

```rust
pub trait DatagramTx: Send {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>>;
}

pub trait DatagramRx: Send {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>>;
}

pub trait ChannelControl: Send {
    fn cancel(&self, reason: CloseReason);

    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> impl Future<Output = Result<CloseReport, ChannelError>> + Send;
}

pub struct DatagramSession<Tx, Rx, Control> {
    pub tx: Tx,
    pub rx: Rx,
    pub control: Control,
    pub limits: DatagramLimits,
}

pub trait DatagramOutbound: Send + Sync + 'static {
    type Tx: DatagramTx + 'static;
    type Rx: DatagramRx + 'static;
    type Control: ChannelControl + 'static;

    fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> impl Future<
        Output = Result<
            DatagramSession<Self::Tx, Self::Rx, Self::Control>,
            ChannelError,
        >,
    > + Send;
}
```

Datagram 拥有目标或来源 Destination、逻辑关联身份和一个受预算约束的 PacketBuffer；发送视图中的地址是目标，接收视图中是来源。DatagramLimits 固定单包最大值、目标约束和排队行为，超长报文明报错误，不悄悄截断、不拆成多个业务数据报。零长度的有效 UDP 报文仍是 Some(Datagram)，不能作为 EOF；只有 None 表示逻辑接收结束。一次 pending send 保持同一 frame，取消后的投递不确定性按后文处理。协议原生分片只在协议内部执行并在接收交付前完整重组。

```rust
pub trait PacketTx: Send {
    fn poll_send(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>>;
}

pub trait PacketRx: Send {
    fn poll_recv(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<IpPacket>, ChannelError>>;
}

pub struct PacketSession<Tx, Rx, Control> {
    pub tx: Tx,
    pub rx: Rx,
    pub control: Control,
    pub limits: PacketLimits,
}

pub trait PacketOutbound: Send + Sync + 'static {
    type Tx: PacketTx + 'static;
    type Rx: PacketRx + 'static;
    type Control: ChannelControl + 'static;

    fn open(
        &self,
        request: PacketRequest,
        context: OpenContext,
    ) -> impl Future<
        Output = Result<
            PacketSession<Self::Tx, Self::Rx, Self::Control>,
            ChannelError,
        >,
    > + Send;
}
```

IpPacket 拥有一个完整 IPv4/IPv6 报文及返回关联，不能是零长度伪报文。Packet 与 Datagram 接口分开，不能因为方法形状相似就隐去报文层级。

PacketLimits 包含有效 MTU、地址家族、源地址适配条件和返回关联保证。是否能做 Flow 转换由具体已验证事实和共享 stack 设施决定，不能只凭一个 supports_packet 布尔值推断。一个逻辑 Flow 的关闭不能关闭共享 Endpoint。ICMP Echo 仍使用已有公共 Packet 地址适配及有限关联，不增加 ICMP Flow 平面。

## 动态门面与分配边界

```rust
pub type BoxFuture<'a, T> =
    Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait DynStreamOutbound: Send + Sync {
    fn connect<'a>(
        &'a self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'a, Result<Connected<BoxStream>, ChannelError>>;
}
```

ComponentRoles 的字段为可选的 stream、datagram、packet、inbound、control/observation 等实际角色；ComponentInstance 单独持有唯一生命周期所有者。动态生命周期的 start/shutdown 使用 BoxFuture，revoke 为同步调用。BoxDatagramSession 与 BoxPacketSession 只擦除各自 Tx、Rx、Control，公共辅助提供 pin 转发，不要求协议实现第二套角色。公共泛型适配器实现从 StreamOutbound 到 DynStreamOutbound 的转换；协议只实现前者。Datagram/Packet 的 open 同样在建会话时转换成动态门面，Tx/Rx 本身的 poll 方法可直接动态分发。单会话可能有多个真实角色的包装，不能宣称全路径只分配一次；禁止 claim 到 prepared 到 leaf 的同义转发。未来性能优化必须由吞吐、分配和峰值内存证据驱动，不能把全部应用泛型化。

## 接入交付和客户端响应

InboundSource 拥有监听或协议逻辑请求源，输出待握手对象。源只负责取得请求，不在 accept 内串行执行慢握手。每个待握手对象在有界的宿主任务范围内执行；普通 TCP、QUIC 子流和 frame MUX 可以复用设施，但不共享同一帧格式或强制 u16 流 ID。

| 接口 | 目标签名 | 责任 |
| --- | --- | --- |
| InboundSource | next(&mut self, context: AcceptContext) -> impl Future<Output = Result<Option<PendingInbound>, ChannelError>> + Send | 单消费者；PendingInbound 由公共泛型包装保存协议具体握手对象 |
| InboundHandshake | complete(self, context: HandshakeContext) -> impl Future<Output = Result<AcceptedRequest, ChannelError>> + Send | 消费一次；认证、握手及初始缓冲属于协议 |
| AcceptedStream | facts(&self) -> &RequestFacts；respond(self: Box<Self>, outcome: ReplyOutcome) -> BoxFuture<'static, Result<Option<BoxStream>, ChannelError>> | 包装中保持流和响应器的静态关联，不擦除后 downcast 配对 |
| AcceptedDatagram | facts(&self) -> &RequestFacts；respond(self: Box<Self>, outcome: ReplyOutcome) -> BoxFuture<'static, Result<Option<BoxDatagramSession>, ChannelError>> | UDP ASSOCIATE 控制连接和响应编码归协议 |
| RequestMultiplexer | next(&mut self, context: AcceptContext) -> BoxFuture<'_, Result<Option<PendingInbound>, ChannelError>> | 每个逻辑业务独立准入、路由、计量与关闭 |
| InboundControlSession | 沿用 auth 与 run 的现有中立契约 | 无业务目标的认证控制；接受内核准入和取消 |
| Fallback | 沿用有界 capture、replay 与 FallbackRoute | 协议选定回退及捕获范围，内核负责受约束的目的端执行 |

AcceptedRequest 是封闭枚举，变体为 Stream、Datagram、Multiplex、Packet、Control 和 Fallback；新协议只使用已定义的实际角色，不新增协议命名变体。ReplyOutcome 为 LocalChannelReady、Blocked 和 UpstreamFailed，响应能力按协议语义映射；None 表示拒绝响应已完成且不再交付业务通道。协议不得多发成功响应或在已有业务载荷后插入另一份握手响应。Deferred 出站只确认本地通道可用，不强制等待依赖首个业务字节才能产生的远端响应，以免双端等待死锁。

接受对象取消时关闭其仍拥有的连接；交付成功后，所有权转给会话执行器。协议预读字节必须随通道重放一次，不能遗失或重复。MUX 子流收到 END 与共享载体断开不同，UDP continuity 在协议会话内保留；协议网络保活不能伪造业务流量。原始 UDP、Packet 接入必须在握手前也执行有界的端点和任务限制。

## 生命周期与公共任务范围

ResourceLifecycle 的方法为 start(&mut self, deadline: Instant) -> impl Future<Output = Result<(), BuildError>> + Send、revoke(&self, reason: CloseReason)、shutdown(&mut self, deadline: Instant) -> impl Future<Output = Result<CloseReport, BuildError>> + Send。start 返回表示资源及所需驱动已就绪，不表示宿主已经允许新业务；revoke 同步撤销新调用资格，shutdown 负责等待自己拥有的任务和 I/O。

宿主持有 TaskScope 所有者，协议只取得可克隆的任务登记及取消句柄。spawn 接收 TaskClass 和 Future<Output = Result<(), ComponentError>>，返回 TaskId 或 NotStarted/Capacity/Closing/IdentityExhausted 错误。组件宿主的 suspended Scope 在真实所有者安装并进入 start 前拒绝登记；协议只持 registrar，不能开放或重新开放作用域。普通 TaskScope::new/empty 的直接登记语义保持。任务类别只区分关键驱动和可恢复辅助任务，错误进入所属组件状态；不自动添加无上限重启。关闭开始后禁止新登记，确认任务退出后才能关闭作用域。已完成任务记录也必须回收，不能只限制并发数而永久积累 JoinHandle。

握手和业务任务受对应连接范围管理；组件协议驱动受组件范围管理；控制面保留独立收尾范围。不把整个 Proxy 捕获成 static。关闭监督发生在作用域之外，避免关闭命令等待包括自身在内的任务造成死锁。这里只规定权限与生命周期，不要求额外 actor、线程池或资源调度框架。
<!-- /source-section: 12 -->
