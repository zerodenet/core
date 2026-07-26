# Connector 能力提交分析报告

> 日期：2026-07-26
>
> 基线：`3bca87aee feat(connector): add production release gate`
>
> 范围：本地累计的 Connector、原生管理能力、gRPC 安全、发布门禁及配套文档变更

## 结论

本轮变更已经按可独立审查的能力分成五个提交。当前 Zero 具备初期外部系统对接所需的基础闭环：

- 外部控制端通过 Zero HTTP API、IPC 或 gRPC 调用通用管理与配置方法；
- Connector 仅把 `zero.event.v1` 事件可靠投递到完整注册地址，不规定接收端路径、认证方案或业务流程；
- 单节点可注册多个 sink 并按事件类型分流，单个 sink 故障不会串行阻塞其他 sink；
- outbox、重试、dead letter、磁盘保护和投递状态已进入生产代码及测试；
- gRPC 可选择 Bearer、TLS、mTLS 或外部 TLS 终止，未强制所有部署采用同一种安全方案；
- 协议认证采用单一凭证语义，受管身份、配额、速率、设备与会话状态属于原生运行能力，不属于 Connector 的面板模型。

因此，可以把当前状态定义为“初期能力实现完成，可进入真实控制端联调和受控部署验收”，但不能仅凭本地测试宣称已经完成无条件生产认证。真实接收端、真实网络故障、升级回滚和长期运行仍需部署侧证据。

## 提交划分

| 提交 | 能力 | 主要作用 | 规模 |
|------|------|----------|------|
| `7707842d3` | 原生运行时与 Connector 投递 | 多凭证认证、访问策略、热配置事务、事件投递、outbox、故障隔离及互操作测试 | 300 files；+23,591/-5,274 |
| `4eba1f4b9` | gRPC 可选安全 | Bearer、TLS、mTLS、远程明文显式许可及真实握手测试 | 5 files；+445/-18 |
| `673cd436b` | CI 与发布产物 | 全 feature 门禁、musl 静态产物检查、稳定版发布签核 | 3 files；+124/-30 |
| `4aa27749b` | 控制面与 Connector 契约 | 移除固定面板工作流，明确直接管理 API、Webhook、热配置和材料边界 | 31 files；+825/-1,230 |
| `0cccd5d1e` | 协议文档与示例 | 单凭证语义、受管身份、MUX/互操作说明及 VLESS 示例修正 | 18 files；+102/-30 |

第一个提交规模仍然较大。这批累计代码在 `zero-api`、`zero-config`、application、engine、proxy、协议实现与 Connector 之间共享合同，继续机械拆分会产生不能独立编译或语义不完整的中间状态。后四个提交已经把可独立审查的安全、发布和文档能力从中剥离。

## 能力边界复核

### 原生管理能力

身份认证、协议凭证、配额、速率、设备限制、会话取消、配置校验和运行时热重建由内核/application 提供通用方法。外部系统决定如何组织用户、套餐、计费和节点业务，但不能把这些业务语义反向固化进 Zero。

### Connector

Connector 是可裁剪的主动事件投递能力，不是第二套配置 API，也不是中心访问节点的入口。接收端地址由 `api.event_sinks` 配置提供；Zero 不拼接固定路径，不注册节点，不同步用户，不定义 presence/traffic 等中心端点，也不要求一个 URL 绑定节点或协议。

### 控制与配置

中心新增或修改凭证时，通过 HTTP API、IPC 或 gRPC 提交 Zero 自身的配置合同。所有入口最终进入 application 持有的 acknowledged config transaction，并等待 listener reconcile 成功；Connector 不解释 inbound，也不负责下发配置。

### 传输安全

TLS/mTLS 是 gRPC 传输能力，不是 Connector 事件正文的强制二次加密。部署可选择节点原生 TLS、mTLS、可信反向代理/TLS 终止，或在显式许可下使用受信网络明文；敏感内容是否需要应用层加密仍由具体合同与部署威胁模型决定。

## 已撤销的错误方向

- 固定 `/register`、`/sync`、`/traffic`、`/presence` 路径；
- 节点注册后才能同步或上报的中心工作流；
- Zero 规定 Bearer、`node_id` 路径参数或中心 API 根地址；
- panel、access profile、用户套餐等外部业务抽象进入内核合同；
- Connector 接收或投影 inbound desired state；
- 以 XrayR、Xboard 或其他面板方言作为 Zero 内部抽象。

保留的互操作测试只证明 Zero 作为代理节点时的线协议兼容，不代表内核适配第三方面板。

## 尚未闭环

1. 通用受管材料目前只有事务设计；证书、私钥、CA 等内容的 DTO、staging、原子替换和崩溃回滚尚未实现。
2. 本地 outage/soak 和假接收端测试不等于真实控制端联调；仍需在目标部署中验证超时、代理、证书轮换、接收端升级和长期故障恢复。
3. outbox 低磁盘水位会 fail-closed 并暴露 replay gap，而不是承诺无限零丢失；外部系统仍需以业务账本进行最终对账。
4. 外部 Xray/sing-box 等 ignored 互操作测试未包含在本轮全量自动测试中；它们属于代理线协议资格，不属于 Connector 合同。
5. 发布工作流已增加门禁，但本轮没有推送、创建发布或验证远端 CI 产物。

## 最终验证

在 `0cccd5d1e` 上执行并通过：

- `cargo fmt --all --check`
- `cargo check --workspace --all-features`
- `cargo clippy --workspace --all-targets --all-features --no-deps -- -D warnings`
- `cargo test --workspace --all-features`
- `node docs/scripts/check-docs.mjs`（89 个 Markdown 文件）

每个提交还通过 pre-commit 的 `cargo fmt --all --check` 与 `cargo clippy --workspace --all-targets --features full`。两个文档提交在临时 worktree 的候选提交快照上通过文档检查，协议示例同时通过 JSON 语法解析。

## 建议验收顺序

1. 用一个真实控制端通过 HTTP API 或 gRPC 应用凭证变更，并确认热重建与回滚。
2. 注册两个不同事件过滤条件的 Webhook，验证分流、ACK、故障隔离与重启恢复。
3. 在目标节点磁盘和代理环境中执行 24–72 小时 outage/soak。
4. 完成受管证书材料事务后，再把“无需登录 VPS 的证书投递”列为已实现能力。
5. 远端 CI 与目标平台产物通过后，再进行生产发布签核。
