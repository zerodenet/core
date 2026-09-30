# 通用端点目录 V1（实施第一阶段）

目录提供内核配置和只读观测。独立启停、重启、临时意图与回滚已在后续
[控制 V1 切片](network-endpoint-control-v1.md) 接线；运行中方向收缩仍未完成。
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
密钥；实际部署必须替换密钥、地址和 peer。当前规范 WireGuard 资源需要本地
addresses 和配置的 peer endpoint；仅监听并学习远端地址的服务端继续使用既有
WireGuard inbound 配置，该配置同样进入目录。这一配置形状的统一尚待开发。

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
尚未接入的实例 generation、计数、认证远端地址和来源已知状态使用 null，绝不
把缺失数据填成可信的 0/false。入站独立设备的逐 peer 健康尚未接入该详情视图。
intent_revision 已生成并支持条件更新；state_source 反映 config/runtime_override。

## 未完成开发

- 运行中方向收缩的独立撤权、完整依赖停止协调。
- 资源实例 generation、启动时间、错误和统计/事件投影。
- 规范配置中的仅监听 peer；入站健康、认证来源和实际 endpoint 事实。
- 控制载体完整验收、运行时 schema 导出和端点诊断能力。
- A/B 实网及长时运行验收。目录完成不会自动关闭 WireGuard 生产门禁。
