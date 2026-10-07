# 通用端点目录 V1（实施第一阶段）

目录提供内核配置和只读观测。独立启停、重启、临时意图与回滚已在后续
[控制 V1 切片](network-endpoint-control-v1.md) 接线；运行中入站收缩已实现，
运行中出站收缩通过独立客户端栈/Packet 返回路径撤权接线。
完整实施范围见 [管理规划](network-endpoint-management-plan.md)。
本地门禁与仍待验收的场景见 [验证记录](network-endpoint-catalog-verification-20260929.md)。

## 配置与资源身份

顶层 `endpoints` 定义资源，公共字段为 `tag`、`enabled`、`directions`、`listen`。
当前协议 ADT 为 `wireguard`；出站 tag 保持为资源 tag，入站别名为 `endpoint/<tag>`。
省略方向时默认仅允许出站；启用入站需要 listen。绑定 UDP listener 本身不等于
允许远端业务入站，出站回复和协议控制报文仍需要该 listener。

规范定义在解析时生成已有入站/出站投影，通过已有能力注册及 raw-IP 操作执行；
保存配置时移除生成视图，仅保存 `endpoints`。与显式角色或 policy 的 tag 冲突
提前拒绝。即使 enabled=false，引用及协议配置仍需合法。

旧 WireGuard 配置继续接受：仅显式 `inbound_tag` 关联的角色共享一个目录身份。
相同密钥不自动合并。peer 是资源成员，不独立创建管理身份。

| 配置来源 | 资源 ID |
| --- | --- |
| 规范端点 | `endpoint:<tag>` |
| 旧出站，无显式关联 | `legacy:outbound:<tag>` |
| 旧入站或显式关联角色 | `legacy:inbound:<inbound_tag>` |

ID 对同一配置资源的重新加载、密钥调整和 peer 重排保持稳定。控制端应将 ID 当作
不透明字符串；旧配置迁移到规范资源是一次显式身份迁移。

[示例](../../examples/v0.0.3/wireguard-endpoint.json) 默认停用，使用公开的虚构测试
密钥；实际部署必须替换密钥、地址和 peer。规范 WireGuard 资源需要本地
addresses；配置 listen 时 peer 可以省略 endpoint，由认证握手学习远端地址。
未配置 listen 的纯出站及 outer_udp_proxy 仍要求每个 peer 的配置地址。
省略地址的 peer 在握手前无法主动发送；详情 configured_endpoint=null。
资源 supported.peer_address_learning=true 表示该监听绑定支持认证地址学习；
无监听绑定时为 false。客户端按此能力决定是否允许省略地址。
旧 WireGuard inbound 配置继续进入同一目录。

## 准入与执行分层

- Config 提供规范资源定义、角色投影和中性绑定事实，协议私有校验继续委托 WireGuard。
- Engine 的 `EndpointAdmission` 决定准入、固定选择失败及显式 fallback 行为。
- Proxy 按该准入准备设备/监听器，并复用现有 Packet/Flow 路径和协议适配器。
- WireGuard 仅负责协议设备与 peer；公共 raw-IP 入站先交付已关联出站回复，再
  判断是否允许远端的新业务。未授权的新业务不会进入 TCP/UDP/Packet 入站路由。

现有原生 Packet 回程按 IP 关联，不能严格识别回复。因此仅允许出站的规范端点
在 auto 下对 TCP/UDP 使用已注册的 Flow 转换；强制 packet 及无法安全分类的
auto 报文返回 unsupported。显式 translate Echo 使用 stack 的有界请求关联，
仍可执行；ICMP 不新增 Flow 平面。双向端点及 legacy 配置保持既有原生 Packet
语义。未来 Packet 能力可显式声明安全的回程关联能力后参与原生路径选择。

配置 enabled=false 不准备设备或监听器，资源仍可查询。新出站请求报
`endpoint_disabled`，出站方向撤销报 `endpoint_outbound_disabled`；固定 selector
保留选择。只有已有显式 fallback/自动策略可以选择其他符合准入的成员。
配置 reload 和独立控制沿用现有事务；stop 的资源及业务结束确认语义见控制契约。

## 查询入口

| HTTP GET | JSON QueryRequest（IPC/gRPC/Rust/FFI） |
| --- | --- |
| `/api/v1/endpoints?offset=0&limit=100` | `{"endpoints":{"offset":0,"limit":100}}` |
| `/api/v1/endpoints/{id}` | `{"endpoint":{"endpoint_id":"..."}}` |
| `/api/v1/endpoints/{id}/details` | `{"endpoint_details":{"endpoint_id":"..."}}` |

HTTP 路径中的整个 ID 需按 URI 路径段编码。目录按资源 ID 排序，默认每页 100，
上限 1000；最后一页 next_offset=null。查询要求 Read 权限，复用既有错误信封。
只读能力标识为 `network_endpoint_catalog_v1`；注册控制能力后另宣告
`network_endpoint_control_v1`，并报告运行中方向收缩等限制。

Engine 单独查询只返回配置身份与意图，不宣称已启动数据设备。Proxy 通过注册的
`EndpointObservationCapability` 补充已存在设备及协议事实；运行时无需 WireGuard
分支。另有中性测试观察器验证注册边界，其结果不代表第二种真实协议已接入。

WireGuard 详情为 `schema_id=zero.endpoint.wireguard.v1`、schema_version=1；
[详情 schema](../protocols/wireguard-endpoint-details.schema.json) 定义公开 peer ID、
公钥、AllowedIPs、配置 endpoint 及兼容健康事实。peer ID 由协议将公开密钥规范化，
重排和 hex/base64 表示变化不改变 ID；不暴露私钥、PSK 或报文内容。

running 仅说明观察到本地资源，不证明远端握手或业务可达。握手和认证报文的健康
事实沿用旧 `HealthSnapshot.outbound_devices`；并非业务可达探测。
已应用资源提供 generation、启动时间、错误与状态事件；现有 Stream/Datagram
Flow 数和低频 endpoint.stats_sampled 事件可用。显式端点的内外层字节/包数、
本地已观察丢弃和活动 Packet 路径已有真实来源；缺少提供者时仍返回 null。
Raw-IP 设备的 Packet 路径观测在准备完成时声明，空闲时返回 `0`，无需先发
一个包才能区分零和不可用。这里的 Stream/Datagram 是当前活动业务 Flow，
不是累计连接数；Packet 是已执行的原生/转换 Packet 转发会话，派生 Flow
产生的 IP 包以及本机 Echo 应答不伪装成转发路径。共享端点在两个角色中只计一次。
统计口径、分页查询、采样和 stats.reset 见 [流量观测契约](traffic-observation-v1.md)。
统计周期 stats_epoch 与设备 generation 独立，inner/outer 不相加。

已有注册设备提供认证远端地址、来源已知状态和认证报文年龄；没有可用事实
时返回 null。入站独立设备的逐 peer 完整健康尚未接入该详情视图。
intent_revision 已生成并支持条件更新；state_source 反映 config/runtime_override。
详情响应同样带 core_instance_id、config_revision、observed_at_unix_ms；客户端按
实例关联结果，不把同一个 endpoint_id 或 generation 当作进程身份。
configuration 公布来源、基准方向和源文件条件；操作级支持见控制契约及
[客户端对接说明](network-endpoint-client-integration-v1.md)。

## 本机 Packet 地址

规范端点的 `protocol.addresses` 同时投影到入站，协议验证并提供精确的本机 IP
事实。中性 Raw-IP 监听运行时在认证、AllowedIPs 源检查和入站方向准入之后，
为这些 IP 处理 IPv4/IPv6 Echo Request，并通过原协议设备返回 Echo Reply。
不会为同一 CIDR 中的其他地址应答，不新增 ICMP Flow，不执行宿主地址/路由配置。
本机 Echo 属于本地交付，不执行转发路由规则；内层/外层照常计量，业务用量不增加。
关闭入站角色后新的本机 Echo 不再被接受，已建立出站的相关回包仍走原返回边界。
`raw_ip_local_echo_v1` 声明此运行时能力；它不代表自动开放宿主 TCP/UDP 服务。
legacy 独立 WireGuard 入站可选填 `addresses`，省略时保持原来的转发语义；
显式关联的 legacy 入站若声明地址，必须与同一出站的地址一致，否则配置验证
拒绝。省略地址仍保持转发语义，不从出站或 peer AllowedIPs 推测本机地址。

## 未完成开发

- 通用宿主 Direct PacketSink、仅出站的安全原生 Packet 回程分类、完整依赖停止协调及独立 PacketRoute 公共管理。
- 完整生命周期过渡；独立控制已发布 starting/stopping，启动、普通 reload、
  进程退出及意外失败的全流程仍待补齐。
- 独立入站 peer 的完整健康。
- 控制载体完整验收、运行时 schema 导出和端点诊断能力。
- A/B 实网及长时运行验收。目录完成不会自动关闭 WireGuard 生产门禁。
