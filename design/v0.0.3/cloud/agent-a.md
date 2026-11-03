# Agent A：配置资源准备与宿主事务

从 [云端任务入口](README.md) 指定的独立分支开工。目标是整体关闭 Z003-10 和 Z003-09，对应原 PR08B / PR07B；只改内核，不处理协议池、WireGuard 临时债务或消费者项目。

## 现状与目标

`crates/config/src/compile.rs`、`rule_sets.rs` 仍执行 GeoIP/规则资源读取和运行构建；`crates/engine/src/runtime/configuration.rs` 仍编译路由、持久化并安装配置。现有 RuntimeHost 已有唯一配置操作 owner、串行锁和最终等待，不能另建第二套事务。

先定义并实现一次准备的资源/计划输入，将资源 I/O 与运行构建迁入宿主准备链；Config 保留 ADT、结构/引用和协议委托验证，Engine 保留领域计划的纯构造、快照安装、策略和 revision。随后把持久化及跨组件准备/提交/补偿接入同一宿主操作。Config 不依赖 runtime/proxy/engine，不将具体协议或 Engine 类型塞进通用 runtime；由 Proxy 的实际装配桥注入所需操作。公共 Proxy、Engine/FFI 入口保留既定功能范围，不为 Engine-only 嵌入入口自动启动完整 Proxy。

## 实施顺序与删除边界

1. 列出初始构建、reload、bypass、DNS dispatch、临时 overlay、适用控制/FFI 入口的全部调用者，确定准备值及失败映射。准备值表达真实资源，不做新 capability/provider 接口族。
2. 准备并复用实际规则资源，保持相对路径、自动路由、模式/bypass/DNS 顺序和原错误来源；业务路径不加载资源，准备失败不发布候选。
3. 让 Engine 消费准备好的领域输入，迁出文件读取/持久化和跨组件编排；删除旧 I/O/编排调用链，不仅保留另一组带新名字的方法。
4. 在同一配置操作内串行完成停止、确认旧清理、启动、Ready、持久化、提交和最终 ACK。持久候选在资源 Ready 前不得覆盖唯一 last-good；使用冻结方案要求的最小恢复记录/原子文件替换，不增加数据库或通用工作流框架。
5. 取消等待者不取消已受理操作的收尾；失败候选清理未确认不得重启旧资源。持久/临时配置、revision、选择状态与 Faulted/恢复结果保持既有契约。

以“资源准备迁出”和“配置事务收尾”两个逻辑提交组织同一 PR；第二步依赖第一步，但不等待 Agent B。不要依赖 B 尚未提交的类型。

## 必须验证

- 原有 route/bypass/DNS dispatch、资源相对路径与加载错误不变；准备失败保留当前配置、文件和健康业务。
- 持久与临时配置区别、幂等更新、同一操作最终 ACK、失败时 revision/领域快照/选择状态一致。
- 候选 Ready 前磁盘 last-good 不丢失；持久化失败、恢复记录对应的提交前/后重启及错误可恢复。不能用文件落盘顺序测试代替全部事务行为。
- 真实 listener/DNS/application service 的失败补偿、取消等待者、宿主停止时已受理事务收尾；原件关闭与 TaskScope joins 独立断言。
- 确认旧原件清理先于替代构建；超时/Unknown 保留原 owner；失败候选清理未确认阻止恢复。
- 适用 CLI/HTTP/IPC/gRPC/FFI 路径使用同一准备与提交语义，保持 engine-only 功能范围和 ABI；不更改外部 schema 掩盖内部迁移。

优先必要定向回归，使用真实待完成操作和原始错误；完整检查按 [验收标准](acceptance.md) 在冻结候选执行。PR 明确旧调用归零位置、所需后续事项及当前未运行平台，不写“全部重构完成”。
