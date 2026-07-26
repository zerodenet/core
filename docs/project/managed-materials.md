# 通用受管材料事务设计

## 目的与边界

证书链、私钥、CA、规则文件等运行材料可以由外部控制端投递，节点不应要求操作者先登录 VPS 写文件。该能力属于 Zero application 的配置事务，不属于 Connector：Connector 只投递事件，不能接收材料、解释协议配置或写入任意文件。

首期仍让协议配置使用现有文件路径。受管材料层负责在配置热重建前把声明的内容安全落到节点本地，使协议和 transport 继续只消费路径，不引入中心、用户或面板语义。

## 目标合同

在现有 `config.apply` 参数中增加可选 `materials`，与完整候选配置一起提交：

```json
{
  "method": "config.apply",
  "params": {
    "config": {
      "inbounds": [{
        "tag": "vless-in",
        "protocol": {
          "type": "vless",
          "tls": {
            "cert_path": "managed/tls/example/fullchain.pem",
            "key_path": "managed/tls/example/private-key.pem"
          }
        }
      }]
    },
    "materials": [
      {
        "path": "managed/tls/example/fullchain.pem",
        "kind": "certificate_chain_pem",
        "content_base64": "...",
        "sha256": "..."
      },
      {
        "path": "managed/tls/example/private-key.pem",
        "kind": "private_key_pem",
        "content_base64": "...",
        "sha256": "..."
      }
    ]
  }
}
```

这是 `config.apply` 的通用事务扩展，不新增 Connector 命令或第二套配置 API。HTTP、IPC 和 gRPC 承载同一 `CommandRequest`。

## 安全约束

- 所有 `path` 都是相对于配置目录下受管材料根目录的规范化相对路径；拒绝绝对路径、`..`、符号链接逃逸和保留设备名。
- 先校验 Base64、大小上限、SHA-256、材料类型和 PEM 结构，再接触正式路径。
- 私钥文件使用平台可提供的最小权限；日志、事件、诊断和错误不得包含材料正文。
- 候选配置只能引用本事务提交的材料或已存在且通过校验的受管材料。
- 远程调用继续要求 `Permission::Config`；gRPC/HTTP 传输安全不因材料事务而放宽。
- 首期不下载材料 URL，避免节点成为通用网络取回器；外部控制端负责取得内容并提交。

## 原子应用顺序

1. 读取当前配置 revision 与 last-known-good 文件清单。
2. 在同一文件系统的临时目录写入候选材料并同步落盘。
3. 校验候选配置及其全部材料引用，构造 proxy/application 重建计划。
4. 原子替换材料文件，再执行现有 `apply_config_and_wait` reconciliation。
5. listener、transport 或 application service 任一步失败时，恢复旧材料和旧配置。
6. 全部成功后持久化配置并清理不再引用的旧材料；清理必须延迟到事务确认之后。

配置 revision/CAS 与材料清单必须共同参与并发检查，避免旧候选配置覆盖较新的本地配置或证书轮换。

## 生命周期与可观测性

- 查询只返回路径、类型、SHA-256、字节数和生效 revision，不返回正文。
- 成功与失败使用通用配置事务事件；事件中只记录材料元数据和错误阶段。
- 证书过期时间可以作为解析后的非敏感诊断字段，但不由 Connector 解释或决定续期。
- 同内容 SHA-256 的重复提交应幂等；私钥轮换失败不得破坏仍在运行的 last-known-good listener。

## 实施阶段

1. 在 `zero-api` 定义材料 DTO，并在 `zero-config` 定义路径、大小与引用验证。
2. 在 application 配置事务中实现 staging、权限、原子替换和回滚。
3. 为证书链、私钥和 CA 增加成功轮换、校验失败、bind/reconcile 失败回滚、并发 revision 冲突测试。
4. 完成 Windows 与 Unix 文件权限和崩溃恢复验证后，再把该能力列为生产可用。

当前代码尚未实现上述材料 DTO 和事务；本文件是后续实现必须遵守的边界，不能把临时的 Webhook 文件投递塞进 Connector。
