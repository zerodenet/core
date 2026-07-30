# VLESS 公共约定

VLESS 入站和出站共享用户标识、flow 和地址编解码语义。这些协议私有值由 `protocols/vless` 解析和校验，通用配置层只调用协议提供的验证入口。

## 关键字段

| 字段 | 作用 |
| --- | --- |
| `uuid` / 用户列表 | 入站鉴权和出站身份 |
| `flow` | 选择协议流程；可用值受传输组合约束 |
| 目标地址 | 按 VLESS 地址格式编码和解码 |
| 传输配置 | 由通用传输层构建，不在 VLESS 协议模块内重复实现 |

完整字段形状和组合约束见[公开配置参考](https://docs.zerodenet.org/projects/core/configuration/)。
