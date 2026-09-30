# 通用网络端点管理实施规划

日期：2026-09-29。状态：P0/P1 第一阶段已实现；P2 独立启停切片此前通过
本地工作区门禁。2026-09-30 的后续工作增加运行中入站撤权和部分 P3 事件，
并通过单独的本地工作区门禁；运行中出站撤权及完整统计仍待开发。
代码核查基线：develop / `c71a7c22`，工作区另有既存 DNS/TCP 修改。

第一阶段已增加规范配置/旧配置目录、Engine 准入、注册观察器及只读查询。
已接入范围和明确缺口见 [端点目录 V1](network-endpoint-catalog-v1.md)。
本地门禁与外部场景状态见 [验证记录](network-endpoint-catalog-verification-20260929.md)。
后续独立控制范围见 [控制 V1](network-endpoint-control-v1.md)。
本轮控制验证见 [P2 验证记录](network-endpoint-control-verification-20260929.md)。
不宣称完整 P2、P3 统计/事件或 P5 生产场景已完成。

## 1. 目标与原方案关系

依据用户提供的 Packet Plane + Flow Plane + Capability-based routing +
Translation Adapter 原文、根 AGENTS.md、现有 WireGuard 计划及控制面规范。
本规划补充端点的身份、方向授权、生命周期和观测契约，复用原有数据平面。

- 数据平面继续采用 Packet / Stream / Datagram，以及可执行转换器。
- 入站和出站是端点的使用能力；启用和停用是资源状态。
- 一个双向端点共享物理设备、协议状态和外层载体，拥有一个管理身份。
- WireGuard 是首个完整接入者；未来注册协议复用同一控制面基础。
- 本次实施范围是 Zero 内核、控制接口、契约生成、文档和验收工具。

原文明确规划双平面、方向能力、转换器、最低转换成本与未来 Tailscale。
统一资源身份、期望/实际状态、独立启停和管理展示是本次补充需求。
sing-box 的 Tailscale 状态、peer、诊断及出口选择可作需求参考；Zero 使用自有
API，不复制第三方接口，也不将 Tailscale 的登录/退出动作当作通用启停。

## 2. 已有范围与本次新增范围

下表保留规划起点的源码核查；“已有”表示源码存在，并有历史验证记录。
首阶段的当前工作区本地门禁见验证记录；历史及本地通过均不证明所有平台或
生产场景已验收。

| 能力 | 当前核查 | 本规划处理 |
| --- | --- | --- |
| Packet/Flow 与最少转换选路 | 已有 network_graph / inventory；先选 target 再选择平面 | 复用，运行时方向和状态成为准入条件 |
| WireGuard 原生 Packet 与派生 TCP/UDP | 已有公共 raw-IP/stack 操作 | 复用，不另建协议专属 Flow 栈 |
| WireGuard 入站与双向共享设备 | 已有中性 listener；inbound_tag 关联共享端点 | 接入统一资源身份及生命周期 |
| 路由 auto/packet/flow/translate | 已有；Echo 地址适配位于 stack | 保持语义，不新增 ICMP Flow |
| 多配置、peer AllowedIPs、DNS detour | 已有，A/B 有历史实网记录 | 增加停用/方向变化时的行为验收 |
| peer 握手/认证收包健康查询 | HealthSnapshot.outbound_devices 已有 | 扩充为覆盖入站、出站和共享端点的观测 |
| reload、外层 UDP carrier 与故障恢复 | 已有实现与局部证据 | 纳入端点操作串行协调与回滚 |
| 独立端点列表、稳定 ID、方向授权和启停 | 尚无统一公开契约 | 本次核心开发 |
| 通用统计、事件及协议详情 | 部分数据已有，尚未组成统一端点契约 | 本次核心开发 |
| 通用宿主 Direct PacketSink | 尚未实现 | 独立后续工作，不作为本次前置条件 |
| 任意外部 PacketEndpoint 数据平面 SPI | 当前为内部中性操作 | 不在本次承诺；未来协议通过注册能力接入 |
| 跨任意拓扑、时延/MTU 的全局最优图 | 当前图只优化平面转换 | 独立扩展，不改变本次路由策略 |
| Tailscale 实际协议、认证、出口选择、文件/SSH | 尚未由本规划实现 | 后续协议接入；本次验收通用扩展边界 |

WireGuard 生产门禁继续沿用 wireguard-implementation-plan.md 的 M5，逐项核查
引擎补丁、安全、真实 TUN、故障恢复、长期运行和跨平台证据；管理能力完成
不会自动关闭这些门禁，也不自动改变 wireguard 的 opt-in 特性。

## 3. 资源、方向和状态模型

### 3.1 统一身份和配置

新增顶层 `endpoints` 作为物理/协议资源的规范配置入口，共有字段为 tag、
enabled、允许方向及协议配置。协议配置的 ADT 位于 config，私有校验与构造
继续由协议所有。规范配置与只读契约已按端点目录 V1 落地；独立启停和临时
意图已接线，以下完整生命周期仍按分阶段验收。

- `endpoint_id` 由配置 tag 形成稳定身份，不使用私钥或密钥派生值。
- 端点出站投影可作为既有 target 被引用；端点入站投影提供既有路由元数据。
- endpoint、outbound 和 policy 的 target tag 冲突必须在配置阶段拒绝。
- 内部设备实例用 core_instance_id + endpoint_id + generation 区分。
- 配置内容改变可增加 generation；列表仍保留同一个 endpoint_id。
- legacy inbound/outbound tag 保持可用，显式映射到同一资源及对应方向。
- 仅显式 inbound_tag 关联或明确 endpoint 引用允许合并；相同密钥不自动合并。
- 一个 peer 是端点的协议成员，不另创建一个管理端点。

现有 WireGuard inbound/outbound 配置继续被内核接受；规范化后进入同一中性
资源计划。新增配置与旧配置指向同一身份而定义不一致时提前拒绝。客户端
无需解析或自动转换 WireGuard conf。本次不要求旧客户端迁移配置。

### 3.2 三类能力信息

| 信息 | 含义 |
| --- | --- |
| supported | 已编译的协议能够提供哪些方向、数据平面和操作 |
| allowed | 当前配置/管理意图允许使用哪些方向 |
| effective | 当前设备与已发布配置实际可以执行哪些操作 |

方向为 inbound/outbound；平面为 Packet/Stream/Datagram，并标记原生或派生。
启用某方向要求注册能力与必要绑定配置齐备，缺失时明确报错。元数据宣称支持
不能替代 prepare 阶段返回的可执行操作。

只允许出站时，仍接收握手、keepalive、已建立出站路径的回包和认证漫游。
禁止的是远端发起的新业务进入 Zero 入站路由；不能直接关闭外层 UDP 接收。
方向准入复用中性 Packet 返回路径/会话所有权及生命周期，未关联的新业务
不得穿过禁止方向；无法安全分类的 Packet 用法明确返回 unsupported。

### 3.3 期望状态、实际状态与健康分开

- 期望状态：enabled/disabled，以及允许的 inbound/outbound 方向。
- 实际状态：stopped/starting/running/stopping/failed，包含最后一次结构化错误。
- 运行健康：unknown/healthy/degraded 等事实投影，以及协议提供的具体证据。
- running 只证明本地资源完成准备和发布，不证明远端握手或业务可达。
- WireGuard 空闲或从未握手不直接等同于离线；展示握手/认证收包年龄及状态。
- 单个 peer 无响应不停止其他 peer，也不自动阻断人为固定选择的端点。

配置提供初始意图。默认控制命令为 runtime_only，意图保存在 Engine 的通用
端点状态中，源文件不被修改；同一 ID 的普通 reload 与网络恢复保留该意图，
删除资源时清除，进程重启后读取配置。显式 source_file 操作复用现有配置持久化
事务，成功提交后清除对应临时覆盖。API 暴露 state_source；提供显式清除临时
覆盖的方法，避免客户端误判状态。配置修改与覆盖更新共用串行协调入口。

## 4. 操作语义与路由行为

### 4.1 关闭路径和停用端点

PacketRoute.Close() 关闭某个使用者持有的数据路径，释放队列、返回关联及引用。
同一端点的其他路径继续运行；它不修改端点管理意图，也不关闭共享协议资源。
路径可能承载一个 Packet 会话或多个报文，不强制与一条 TCP 连接一一对应。

Close 是当前路径对象的生命周期结束，不是停用分流规则。规则保持有效且
端点仍启用时，后续匹配流量可以重新建立路径；其他允许方向的业务仍可访问
该端点。因此不能把 PacketRoute.Close() 接到客户端的“停用端点”按钮上。

| 操作 | 端点及访问效果 |
| --- | --- |
| 关闭当前路径 | 结束该对象；端点仍运行，其他业务及后续新路径可继续使用 |
| 撤销入站方向，保留出站 | 禁止远端新业务进入入站路由；握手、keepalive 和出站回包仍接收 |
| 撤销出站方向，保留入站 | 禁止本机新业务选择其出站能力；入站业务及其必要响应仍可工作 |
| 停用整个端点 | 所有业务方向关闭，并结束该资源的协议收发和设备任务 |

endpoint stop 修改整个资源的意图并撤销所有方向的准入，终止该端点所属的
Packet 路径和 TCP/UDP 业务，停止 keepalive、定时器、载体及监听资源。
停止后保留可查询的配置身份和最后状态；控制 API 可查询不表示 WireGuard
数据端点仍在处理报文。其他独立端点继续运行。
v1 使用立即停止；有界排空作为以后独立操作，不混入默认停用语义。

### 4.2 启停、方向修改与重启

- start：校验、准备资源、发布有效状态；远端不可达体现在健康中。
- stop：先阻止新准入，再取消现有路径，关闭资源并等待结束确认。
- 方向收缩：结束被撤销方向的业务，保留另一方向的回包、协议状态和业务。
- 方向扩展：候选准备完成后发布；准备失败保持原资源和原权限。
- restart：显式重建设备，可中断该端点业务；其他端点不受影响。
- 重复 start/stop 幂等；操作与 reload/网络恢复串行协调。
- 停用意图优先于自动恢复，迟到的旧 generation 消息不能重新激活资源。
- 预备失败应回滚；部分清理失败则报告 failed/错误，不能伪报 stopped 或成功。
- 命令返回最终状态与 revision；仅排队不返回 applied=true。
- 超时与并发冲突明确报告，客户端通过操作关联 ID 和快照确认结果。

### 4.3 禁用资源不是修改路由规则

| 使用场景 | 规定行为 |
| --- | --- |
| 路由显式指向已停用端点 | 返回可识别的 endpoint_disabled 原因，不隐式绕去 Direct |
| 固定 selector 选择已停用端点 | 保留用户选择并报告不可用，不擅自替换 |
| 自动选择/明确允许故障转移的 policy | 按已有策略处理可用成员，保留既定顺序与 Block/unsupported 语义 |
| DNS server detour 指向已停用端点 | 查询按原策略失败；只有显式 DNS fallback 可继续，缓存策略保持既有定义 |
| TUN 开启时停用 A | A 的业务准入撤销；TUN 和 B 继续运行 |
| 手动探测停用端点 | 报告 disabled，不因测速启动资源 |

所有新准入路径，包括原生 Packet、派生 TCP/UDP、DNS detour 和外层载体引用，
消费同一端点状态。Engine 决定准入及策略，Proxy 执行；能力图只在已选 target
内部选择最低转换成本路径。不能用空候选或能力过滤暗中改变 fixed target。
还需验证 endpoint 外层代理依赖没有环路，停用依赖后报告具体原因。

## 5. 观测与管理 API

### 5.1 通用快照

EndpointSnapshot 包含 ID/tag、协议种类、别名引用、supported/allowed/effective、
desired/runtime/health、core_instance_id、config_revision、intent_revision、generation、
state_source、启动时间、最近采样时间、最后错误和可执行操作列表。

统计至少包含 Packet 路径数、Stream/Datagram flow 数、双向内层字节/包数、
外层字节/包数、丢弃和错误。原生 Packet 的统计不依赖 Flow 转换；不定义 ICMP
Flow。共享端点各别名展示同一统计身份，合计时去重。

- 内层统计在设备明文 Packet 边界计数，包含完整 IP 包，方向相对 Zero 端点。
- 外层统计在端点载体边界计数 UDP payload，包含握手/keepalive；代理自身开销
  不混入 WireGuard 统计。inner 与 outer 不相加为一个“总流量”。
- Flow 逻辑流量另有既有口径，重传/加密开销不能重复算入 Flow 计费统计。
- 未实现的计数标记 unavailable，不把缺失字段填成可信的零。
- 计数重置由 generation 标识；采样和事件有界，慢订阅者不阻塞收发。

### 5.2 WireGuard 详情与未来扩展

WireGuard 提供有版本的详情 schema：peer 稳定公开 ID/公钥指纹、AllowedIPs、
配置/已认证远端地址、来源是否已知、握手及认证收包年龄、双向统计、解析或
协议错误。peer_index 保留作兼容字段，peer 重排不改变稳定 ID。
未知来源载体不提供伪造的认证来源；握手也不伪装成业务连通性。
不暴露 private/pre-shared/session key 或明文报文内容。

公共 API 使用 schema_id + version 的详情信封和机器可读 schema；协议产生事实，
薄适配器投影，通用运行时不解析 WireGuard 私有字段。未来协议注册自己的详情
与诊断能力；Tailscale 的认证、peer Ping、出口选择等另定义显式、有类型的契约。
不添加任意字符串 action 的通用执行后门或新的外部协议适配 SPI。

### 5.3 拟定入口

| 接口 | 目标 |
| --- | --- |
| endpoints.list / endpoints.get | 分页列举与单端点通用快照 |
| endpoints.details | 已声明 schema 的协议详情 |
| endpoints.set_state | 启用/停用，显式 persistence，等待结果确认 |
| endpoints.set_directions | 设置允许方向，验证实际能力并协调现有业务 |
| endpoints.restart | 显式重建单端点 |
| endpoints.clear_overrides | 回到配置意图 |
| diagnostics.probe_endpoint | 可选端点/peer 诊断；未注册能力返回 unsupported |
| endpoint.state_changed / endpoint.stats_sampled | 结构化状态与统计事件 |

只读 list/get/details 已有端点目录 V1 契约与 HTTP 路径；四项修改命令已沿用
现有 confirmed executor 接线；入站收缩和状态事件已实现，出站收缩、完整
统计、诊断及其余事件仍待完善。endpoint.stats_sampled 按现有采样任务低频
批量发出，样本只含端点 ID、配置 revision、generation、采样时间和通用计数；未实现的计数为
null，不当作零。
沿用 zero-api 的 Query/Command/Event、鉴权、错误信封与 capabilities，按现有 HTTP/IPC/gRPC/
Rust/FFI 暴露方式挂载。修改操作沿用现有管理员控制约束；源配置持久化服从
已有 Config 权限及持久化入口。复用现有 SSE/事件回放，不新建观测服务器。
错误原因作为结构化 detail/code 发布，P0 决定是否需要提升现有错误契约版本。
旧 HealthSnapshot.outbound_devices 保留为兼容投影，不再作为资源身份来源。

## 6. 分层责任

| 层 | 本次责任 |
| --- | --- |
| zero-traits / zero-core | 必要的中性资源身份、操作和事实合同；保持 runtime-neutral |
| zero-config | endpoints ADT、引用/端口/方向校验、旧配置规范化；私有校验委托协议 |
| zero-engine | 端点计划、意图/准入、revision、统计/健康/事件投影；不启动 socket/task |
| zero-proxy | 注册生命周期/观测窄能力，准备/发布/关闭、引用与依赖协调、回滚 |
| zero-stack | 公共 Packet/Flow 转换与返回路径分类，不持有端点启停策略 |
| protocols/wireguard | peer、密钥、AllowedIPs、认证、漫游、定时器及协议事实 |
| zero-api / 控制适配器 | 统一查询/命令/事件和 schema，传输仅映射既有契约 |

Engine 的新增实现放入 runtime 的职责子模块；Proxy 的控制执行放在 handle 与
orchestration 的对应子模块。root facade 保持简短。新增能力通过 ProtocolRegistry
注册，并使用窄执行上下文；不恢复 ProtocolAdapter、具体协议访问器或 runtime
对 WireGuard/GotaTun 的分支。配置相关操作复用现有 acknowledged transaction，
不创建第二套 config API、状态库或 Connector 管理流程。

## 7. 实施顺序与交付

| 阶段 | 交付 | 阶段验收 |
| --- | --- | --- |
| P0 契约 | ID/方向/状态/统计/API/schema/兼容错误语义 | 模型无歧义，明确 Close 与 stop；文档和契约测试 |
| P1 资源接入 | 规范 endpoints 配置、旧配置规范化、WireGuard 共享身份和只读列表 | inbound/outbound 关联只显示一个资源；ID 稳定，无重复 socket/协议状态 |
| P2 管理闭环 | 启停/重启、方向限制、意图及回滚/依赖协调 | A/B 独立控制、出站回包正常、停止不被 reload/网络恢复唤醒 |
| P3 观测闭环 | 通用统计/事件、WireGuard peer 详情、兼容 health 投影 | Packet/Flow 均可计数，共享统计去重，事实与健康分离 |
| P4 控制与扩展 | 全部已支持控制载体、schema/capability 导出、诊断能力、第二种中性测试端点 | 各入口语义一致，假协议无需 WireGuard 分支即可接入；不据此宣称 Tailscale 已实现 |
| P5 场景验收 | 本地/参考互操作、A/B 实网、工作区与平台记录 | 逐项记录实现、验证、ignored 和待用户验收，不用握手替代 payload |

P0/P1 先交付可查询身份，随后 P2 形成可独立启停的垂直切片；不等待 Direct L3
或真实 Tailscale。每阶段保持旧配置可运行。运行时/配置/路由变化按 AGENTS.md
执行完整工作区门禁；纯文档规划不触发构建。

## 8. 场景验收清单

1. A 与 B 同时运行，地址和 AllowedIPs 独立；A 的 starmerx.com DNS detour、
   用户给定域名与 192.168.1.235:5432，B 的 16.10.1.2:8000 依既有配置验证。
2. 停用 A 后新旧 A 业务结束、A DNS 新查询失败且无隐式公共解析绕路；B 业务
   连续可用。启用 A 后恢复；停用 B 同样不影响 A。DNS 缓存命中单独记录。
3. inbound-only/outbound-only/bidirectional 的 Packet、Stream、Datagram 行为；
   outbound-only 阻止远端新业务，同时保留回复、keepalive 和正常协议重协商。
4. 共享设备 PacketRoute.Close 只结束对应路径；其他路径继续工作。端点 stop
   终止所有所属路径；query 保留 stopped 身份，引用不会重新创建同一设备。
5. start/stop/restart 与同端口 reload、绑定失败、DNS 失败、载体故障和网络变化
   并发；验证 revision、回滚、资源释放、迟到回包及 generation 隔离。
6. Packet 原样收发和显式 translate 的统计口径；peer 重排、离线单 peer、已知/
   未知外层来源；没有握手时不虚报 reachable。
7. 旧配置/新配置语义一致；旧 health 可用；HTTP/IPC/gRPC/FFI 等已支持入口的
   权限、状态、错误、重复操作及事件回放一致。
8. 端点详情和事件中无密钥或原始 payload，队列/历史/订阅资源有界；注册第二种
   中性测试端点，无新增 WireGuard/Tailscale 判断和重复 Packet/Flow 执行器。

代码门禁：cargo fmt --all；cargo check --workspace；
RUST_MIN_STACK=16777216 cargo test --workspace --all-features；
cargo clippy --workspace --all-targets；cargo build --release。
平台/特权/长期测试分别记录；超时和 ignored 不记为通过。

## 9. 对上层的交付边界

本次交付稳定 API、schema、示例与验收脚本。建议客户端独立展示端点列表和
详情页：状态、允许方向、启停、peer 及业务统计；节点组展示出站投影并引用
同一 endpoint_id。服务端可通过同一 API 管理，不要求桌面界面。
本对话不修改 GUI/ZBoard，不实现 WireGuard conf 客户端自动转换、宿主 NAT/
防火墙/路由表管理或第三方账户产品流程。真实 Tailscale 与 Direct PacketSink
进入后续独立计划，不能算作本次已完成能力。
