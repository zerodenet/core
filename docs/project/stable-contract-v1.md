# 稳定 V1 外部契约

本文定义首个稳定内核版本中由客户端、控制器和 SDK 消费的 V1 契约。它约束公开语义，不把 Rust 内部类型、Cargo 包布局或具体控制传输实现冻结为外部接口。

## 版本清单

`capabilities.get` 返回 `contracts`，分别发布四个可独立演进的兼容范围：

| 契约 | V1 含义 |
| --- | --- |
| `capabilities` | 能力清单字段、能力事实和限制码的解释规则 |
| `control_api` | Query、Command、响应和结构化错误的公开语义 |
| `config_schema` | `RuntimeConfig` 的可接受配置结构和验证语义 |
| `error_codes` | 稳定错误码目录和客户端分支规则 |

每个范围包含 `current` 和 `minimum_supported`。客户端只能在自身支持的版本区间与内核发布区间相交时启用对应能力。旧内核响应没有 `contracts` 时，客户端必须按“契约版本未知”处理并采用保守默认值，不能假定它等同于 V1。

## 配置模式

V1 配置顶层字段为：

```json
{
  "schema_version": 1
}
```

- 输入省略 `schema_version` 时按 V1 解析，兼容 V1 之前的配置文件。
- 内核导出的配置始终显式包含 `schema_version: 1`。
- 内核在构造运行时资源前拒绝未知版本，并返回包含 `found` 与 `supported` 的 `UnsupportedSchemaVersion` 配置错误。
- 客户端不得把更高版本配置静默降级为 V1，也不得通过删除未知字段绕过版本检查。

## 能力与限制

`features` 表示当前二进制已编译且实现的正向能力事实；`global_limitations` 表示跨协议或跨出站成立的机器可读限制。协议局部限制继续由 `protocols[*].limitations` 表示。

首个稳定清单覆盖：

- TUN 双栈接管、按地址族选择出口、direct 的可信域名跨地址族回退、网络变化后的出口重建和 strict route。
- direct TCP 的可信目标候选回退与逐候选拨号观测；direct UDP 保留原始目标，不用 TCP 的连接失败语义猜测式改投其他地址。
- DNS 劫持、TUN system DNS 自动发现、DNS 分流、双栈 FakeIP、FakeIP 持久化与事务化热更新、真实地址反向映射、DNS 上游出口绑定、地址族策略和 wire TTL 老化。
- 无 NAT64、裸 IPv6 缺少可信域名时无法转换、加密客户端 DNS 无法被普通 53 端口劫持、ECH 无法恢复主机名、DoQ detour 尚不支持等边界。

客户端生成配置时以正向能力事实为主要条件，并用限制码关闭不安全的组合或给出针对性说明。未知 feature 和 limitation 必须忽略；已知 limitation 消失表示该限制可能已解除，但客户端仍需依据对应正向能力决定是否启用功能。

## 错误契约

`error_codes` 返回当前错误契约的完整稳定目录：

- `not_found`
- `invalid_argument`
- `permission_denied`
- `insufficient_os_privilege`
- `feature_disabled`
- `conflict`
- `unsupported`
- `internal`

客户端只按 `code` 分支；`message`、`cause` 和诊断上下文用于展示与排障，不作为稳定匹配文本。字段校验失败时优先使用结构化字段路径；内部错误不得泄露协议凭据或其他秘密。

## 兼容与演进规则

以下变更可在当前版本内追加：

- 增加可选字段，并为旧消费者提供缺省语义；
- 增加 feature、limitation 或协议能力条目；
- 增加诊断文本或不改变机器语义的观测字段；
- 修复实现，使其符合已经发布的能力语义。

以下变更必须提高对应契约的 `current`，并保留可声明的兼容下限或提供迁移路径：

- 删除或重命名公开字段、错误码或能力码；
- 改变既有字段、命令、错误码或能力码的机器语义；
- 让原本可接受的 V1 配置在没有明确版本变化时失效；
- 改变请求成功、失败或回滚的事务边界。

弃用项至少跨一个稳定发布周期保留，同时在能力响应或文档中给出替代项。正式移除时提高相应契约版本；配置迁移由显式工具或上层客户端完成，内核不对未知版本做猜测式改写。
