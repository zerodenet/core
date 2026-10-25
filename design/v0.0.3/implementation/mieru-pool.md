# Mieru 共享池与标准 TCP：本地所有权接线

产品版本为 v0.0.3，分支为本地 develop；本批不提交、不推送。继续遵循 [runtime 依赖方向修订](runtime-extraction.md)，协议实现不依赖 zero-runtime，宿主不识别 Mieru 密码、帧、池键或 engine leaf。

## 实际实现

- PoolFactory 仍引用调用方同一个 Arc<ClientPool>，不新建替代池。新增显式的固定根载体 recipe，通过既有 ControlledOutboundStream / BackgroundControl 声明实际 endpoint、完整 upstream 绑定、有限载体数和同数 task 上限。域名、非空前缀及不一致的 TCP 配置在 I/O 前拒绝；没有静默回退。
- lifecycle start 先将原 CarrierOwner 登记到真实 HostResources 的声明 ordinal 0 和同一个 TaskScope，再取得池所有权。池的真实 driver 仍只有一个；没有额外 supervisor、辅助 caller task 或第二个内核。宿主消费实际退出，原始载体 provider 保留并确认关闭凭据。
- BackgroundControl 使用唯一 driver-custody view：在原生 preparation 前取得，在同一认证流程中移动到实际 driver，退出后仍需真实 Scope join 和原 FD 显式关闭。没有独立 caller 时不伪造 caller exit；该路径不能代替 Business / Service 原 caller 的义务。
- 每个 pending native open 取得轻量逻辑用户计数；成功后由返回的 NativeTcpStream 持有，读写拆分继续保留同一个使用权。FD 关闭、driver join 或 write shutdown 都不能证明尚持有的逻辑 read half 已交回。
- 同一个池只有在显式 shutdown 确认原代际 native/users 凭据、全部实际 driver joins 及逻辑用户归还后，才允许不同实例接管。缓存值在锁外释放并完成后才发布可交接状态。legacy-used 池仍不能被接管；native 代际之间也不会重开 legacy acquisition。超时、取消、Drop、cache clear、空池快照都不开放下一代。
- 宿主并行关闭时，I/O provider 的早期暂态快照单独保存在同一个持久 cleanup owner 中。收齐协议、任务和用户的执行结果后，再读取原 provider 的实际声明覆盖证明；只有准入已关闭且全部原始物理凭据确认，才清除该早期 I/O 快照的未完成标记。协议/任务/用户的未完成状态、原始已完成报告和失败均保留，不通过忽略 unfinished 或放宽替换条件修复竞态。

## 验证边界

runtime 新增测试使用真正的 ComponentCatalog 冷装配、规范共享 ID、生产 TaskScope/HostResources/provider 和替换事务。测试装配只向 BuildContext 注入真实 literal-root NativeCarrierOpener，不替代协议握手、driver、资源 owner 或关闭接收者。验证两项独立 TCP 角色共用一个 pool owner、tag 隔离的真实加密载体、修改绑定时旧流阻止新 owner 构建、取消清理后保留原 owner，以及取消真实认证时没有虚构 driver join。

协议侧另行验证：实际原 FD 和 driver 都关闭后，保留一个逻辑 read half 仍使池关闭不完整。平台侧验证：driver-custody 凭据依赖原始 native close 与实际 scope join，未登记、Business 和 Service 声明不能取得这种能力。原来的 unaccounted acquisition 拒绝证明继续保留。

实际命令、通过/失败、源码摘要和消费者检查以本批 [验证记录](local-mieru-20261024/validation.json) 为准。历史全量和旧二进制不覆盖本批源码。

首次完整回归为 3770 通过、2 失败、175 跳过；两项新宿主测试暴露上述旧 I/O 快照问题。原始结果及[根因记录](local-mieru-20261024/failure-analysis.json)保留，修复前后源码、二进制和检查分别标识。未调整断言、超时或跳过条件。

最终同一源码在 Rust 1.99 的完整 workspace/all-features 回归为 **3772 通过、0 失败、175 跳过**，155 个目标，837 秒；Rust 1.98 的严格 workspace/all-targets/all-features Clippy（`-D warnings`）通过，格式检查通过。GUI 和面板使用同一固定内核副本 `9a36ad0834130705924b7e598685172fff7aa30c2f31b0e8d33e1de28705700c`：GUI 原有 IPC/apply/回滚检查通过，内核正常退出 0，无 terminate/kill；面板原有发布回归通过，84.10 秒，同一 PID 的 TCP/UDP 跨三个发布进程保持可用。消费者源码未改，部署/SSH/service 为本地替身，未验证真实 GUI/TUN 设备、生产部署或 1 CPU/1 GiB 压力。

## 尚未开启生产登记

MieruAdapter / register.rs 仍使用原来的同一个兼容池，未将有限的固定根载体资格扩大为全协议生产支持，未删除 BackgroundCarrier 的 Unsupported 栅栏。生产装配还需要一起处理：

1. 同一个池的多 endpoint / 多 upstream 绑定，以及配置中完整凭据集合变化时的规范资源身份和整池替换。
2. 实际 DNS 选择、网络代际变化和 relay prefix 的原始资源依赖及关闭证明；不能伪造 literal peer、绑定到另一份 egress 控制器或直接绕过前缀。
3. 同一个 adapter 池中 TCP / UDP-underlay / UDP-role 的实际所有权；不能创建并行的 legacy/native 池，也不能将旧 detached driver 包装成已监督的任务。
4. 接入上述真实 provider 后，再统一启用生产 ComponentFactory 登记和完整端到端协议/兼容资格。

现有配置、Zero API/V1 命令事件、Proxy 入口、FFI/gRPC schema、客户端和面板源码不改变；新的声明和凭据是内部协作契约。不能把不改变 schema 等同于实际 GUI/TUN/生产面板资格。

## 后续进展

多 endpoint / binding、完整凭据集合和 TCP 载体上的标准 UDP 角色继续见 [整池 profile 阶段](mieru-profiles.md)。本页的历史验证结果不覆盖后续源码；DNS、relay prefix、UDP underlay 和生产登记仍需逐项验收。
