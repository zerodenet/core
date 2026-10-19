# 实际执行记录

完整规格及原任务卡保留原始来源；本文件记录后续代码和实际验证。所有检查点继续留在同一分支及 Draft PR124，整体尚未完成，不逐项合并。

## PR00 / PR01

PR00 方案提交为 `122eb04b806da6a31495345d79d3e4f591f027b3`。同一 PR 的交付规则提交为 `3abbde91a1e8a6dae3d9391ad254ff8eded64d9c`。

PR01 基线提交为 `f90f012689dce16cdbe4fa7616492b8fa8e5cc9e`，运行源码仍为 `develop@2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`。原始输入、回执、FFI/消费者及真实 1 CPU/1 GiB 资源证据见 [baseline](../baseline/README.md)。没有把零长 UDP 当 EOF，没有把 OS 线程数当 Tokio 任务数，也没有凭空设置新的生产容量默认值。

[f90 远端资格记录](../baseline/ci-evidence-f90.json) 区分了实际结果：静态、最小 feature、三平台编译及 musl 成功；Linux workspace 有一个既有 Hysteria 认证测试在 10 秒总截止时间超时，结果为 2634 通过、1 失败、159 忽略，根因尚待定位。三个 hosted TUN job 执行了主要特权场景，独立 smoke 步骤仍跳过；Windows 出口切换明确 `executed=false`，缺少合适的第二接口。Optional surface job 未执行。这些结果不适用于后续提交，不能称为全绿或完整真实设备验收。

## PR02：公共契约与集中擦除

代码提交：`4d76d1663d610cf110fc4feeb972600a2055b5ce`。验证前冻结的 Git tree：`99c60f08224c6baafed63a0cd03bc95ba3b34b86`，与该提交的 tree 完全相同。详细命令、耗时、原始输出压缩件和 SHA-256 在 [pr02-validation.json](pr02-validation.json)。本地构建时 HEAD 仍为 f90，不能把它的 build-info 标签描述成新提交的发布二进制。

- [zero-core](../../../crates/core/src/component.rs) 保持 no_std，新增有区别的资源/角色/会话身份、检查溢出的代际、纯请求事实、完整 IP 报文所有权及投递分类。认证主体 Debug 隐去协议身份值。
- [zero-error](../../../crates/error/src/channel.rs) 保留阶段、原因、范围、投递与重试分类及来源链。未能证明未提交时，默认状态为 Unknown。
- [运行契约](../../../crates/platform/tokio/src/contract/mod.rs) 位于 platform-tokio，使用关联具体类型和公共动态包装。没有将 Tokio 引入 traits/core，也没有提前提取 zero-runtime。
- Stream 支持 `!Unpin`、`!Sync` 具体流和延迟确认；Datagram/Packet 独立，接收器及控制器仅要求 Send。poll 辅助没有为每个报文分配 boxed Future。部分成功、超长、错误目标和投递不确定均有明确处理；丢弃 pending send 会隔离逻辑会话。
- Packet 有独立端点事实；不要求伪造业务 SessionId。握手仅在识别出业务请求后申请会话身份，Control 不会自动消耗业务身份。上游绑定含前缀角色的资源代际、硬拨号约束和网络代际，重建前缀不会复用旧池身份。
- 工厂 describe 只返回角色及相对资源归属；实际 ID 由宿主分配后传入 build。build 同步构造状态和唯一生命周期所有者；有副作用的启动在 start。角色不必实现未声明的空接口。
- [TaskScope](../../../crates/platform/tokio/src/contract/scope.rs) 复用提供的 Handle，限制活动任务及未消费完成记录；区分关键/辅助任务，回收完成记录，阻止关闭后的登记。关闭超时保留待清理任务；报告保留任务失败及来源链。Drop/abort 不会产生关闭成功报告。

本地冻结源码的检查：fmt、workspace check、workspace/all-target/all-feature 严格 Clippy、完整 workspace 测试及默认 release build 均成功；测试共 **2665 通过、0 失败、159 忽略**。其中 platform 公共契约 suite 为 70 项，覆盖单 Stream 工厂、动态调用、非 Sync Rx/Control、Pin 流、空数据报、部分发送取消、半关闭、Packet 无业务身份、任务容量、登记/关闭竞争、跨 scope ID 和关闭失败来源链。

实际 `thumbv7em-none-eabi` 检查通过。依赖守卫检查实际 Cargo 图：traits/core 无 Tokio，platform 无 config/engine/proxy/具体协议，config validation 不引入 Tokio/platform/transport；守卫及 no_std 目标检查已接入 CI。当前提交的远端 CI/平台结果仍待产生，不能引用 f90 的结果充当本提交资格。

## 尚未完成的生产迁移与资格

PR02 按任务卡保留原生产执行者。尚未将现有协议改为 Factory/实例角色登记，没有切换 Stream/Datagram/入站/Packet 生产路径；尚未完成 runtime 提取、单实例配置事务、DNS/Network/路由装配、旧桥清除及最终消费者回归。不能以本次新增接口和测试宣布 G1–G10 或整体完成。

下一检查点是 PR03 的强类型冷路径登记、一次 Options 转换、实际实例/角色身份及旧 facade 的单处适配，随后继续全部任务卡。已在实际基线核对：transport 的 Cargo 依赖已不含 config/engine/具体协议，engine 的 ResolvedLeafOutbound 已是 Direct/Block/中立 Proxy identity，协议计划也已存在于 owning protocol；这些属于起点已有能力。后续只迁移仍在使用的 claim/prepared-operation/handler/Any 和生命周期调用，不重做已经消失的旧层。

最终仍需在真实迁移后的精确提交执行完整 CI、受影响 immutable reference 互通、三平台资源/停止/恢复、相同负载的资源对照及固定消费者/FFI 交叉验证。Packet/协议专项资源、完整 CLI 任务归属、容量预算、完整 GUI/物理设备及 Windows 真实出口切换缺项均保留；不得用 hosted job 成功或现有 baseline 代替。
