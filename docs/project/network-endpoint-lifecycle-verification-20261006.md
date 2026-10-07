# 普通运行端点生命周期验证

日期：2026-10-06 至 2026-10-07。接续端点控制与监听 peer 的工作区实现，范围仅为 Zero 内核。
本记录与 [上一阶段门禁](network-endpoint-control-verification-20261006.md) 分开，
上一阶段通过不能代替本次源码的验证。

## 范围与分层

- 普通启动发布 starting，设备和监听准备成功后发布实际 running。
- 普通配置启停与删除发布过渡和最终事实；确认删除后释放对应 Engine 运行
  事实，再创建同一 ID 获取新的 generation。未更改端点保持代际。
- 普通退出先发布 stopping 并发送关闭信号，等待监听/服务与出站设备的既有
  任务完成确认，之后发布 stopped。配置 enabled、方向、统计周期保持原意图。
- 启动失败、监听/服务任务异常退出或 panic 统一执行清理，再发布 failed 和错误。
- 协调任务取消/panic 的同步析构保护发布明确“清理未确认”的 failed；不会把
  发出 abort 请求冒充确认 stopped。进程被 OS 强制终止不能补写旧事件。
- 配置候选失败回滚后，已恢复资源仍按真实状态投影；变更端点记录操作错误，
  未变更端点不产生假的故障或重建。
- 通用监听协调先批量预留可独立绑定的候选 socket，再切换实际监听。
  后序绑定失败不会暴露前序候选 WireGuard peer 会话。复用现有监听端口
  （含被删除资源的端口）的候选继续采用停用/绑定/回滚，不强行预绑定。

Engine 仍拥有运行事实、generation 和事件。中性协调器拥有准备、重载、
任务结束和清理；注册设备能力返回异步结束确认。raw-IP 设备复用现有
wait_stopped，WireGuard 适配器只返回已有设备结束确认，不新增协议专属
生命周期体系。大的 state.rs 拆为 startup/reconcile/completion，根模块保持门面。

## 客户端

读取 capabilities.features 中 network_endpoint_orchestration_lifecycle_v1，
复用 endpoints 查询、分页查询、endpoint.state_changed 事件与回放。
断线或事件缺口后重新查询，以 core_instance_id / endpoint_id / generation
区分实例；删除后查询 not_found。同一 generation 的 stopping 不等于重建。
资源 running 只代表本地运行，不证明 peer 或目标业务可达。

详见 [客户端对接](network-endpoint-client-integration-v1.md) 与
[控制契约](network-endpoint-control-v1.md)。

## 本轮尚未关闭的范围

- 同一资源 ID 内协议原地更新/真实重建及网络恢复的全部代际/过渡观测。
  endpoint_transitional_lifecycle_facts_incomplete 继续保留，新增能力是上述
  普通运行路径的精确声明。
- 单独 PacketRoute 公共管理、通用宿主 Direct PacketSink、仅出站端点的
  通用安全原生 Packet 返回分类、独立入站完整 peer 健康。
- M5 的固定外部实现、特权真实 TUN、长期恢复、安全/性能及跨平台验证。

## 验证

最终源码五项门禁全部通过。新增 6 个生命周期单测、5 个端点生命周期集成
用例均在完整门禁中执行通过。原先失败的关联 WireGuard 端点绑定回滚用例
及其候选 socket 释放断言通过。此前中断与失败记录在下文保留。

| 门禁 | 退出码 | 耗时 |
| --- | ---: | ---: |
| cargo fmt --all -- --check | 0 | 6.76 s |
| cargo check --workspace | 0 | 24.50 s |
| cargo check --workspace --all-features | 0 | 64.80 s |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | 0 | 35.43 s |
| bash scripts/test-workspace.sh --jobs 2 | 0 | 2186.97 s |

完整门禁覆盖 workspace/all-features 的单测、集成、文档测试和布局检查，
最终 146 个目标结果为 **2379 通过、0 失败、155 忽略**。
忽略项包括未配置的固定外部参考程序、特权/隔离平台场景和长期 soak；
不能把忽略解释为外部互操作或 M5 通过。
完整测试时间：2026-10-07 04:47:52 至 05:24:19 CST。
最终记录的 2623 个 Rust/Cargo/脚本源码文件指纹一致，git diff --check 通过。
本轮未提交、推送或构建发布版本。

测试使用当前 TUN 环境，不修改系统配置；
ZERO_TEST_HOST_IPV4=192.168.50.138 已通过本机实际 UDP 绑定核对。
每项命令记录起止时间、退出码与完整日志，编译并发 2、测试线程 4、栈 16 MiB。
日志目录：/Volumes/tool/tmp/zero-endpoint-lifecycle-gates-20261006/。

第一次完整运行：2026-10-07 00:04:38 至 00:40:09 CST，退出码 130
（主动中断）。已完成的 65 个目标结果为 1201 通过、0 失败、20 忽略；
Proxy 单测目标仍有两个新增用例未结束，因此这些部分结果不证明完整门禁通过。
问题位于测试的暂停时钟与真实网络/栈 std::time::Instant 时钟组合；两个
用例已改用真实时钟，内核 5 秒超时策略保持原样。此前单包过滤命令因
不同特性图重复编译而停止，也不计为通过。初次定向 23 个端点与 34 个
控制用例通过，但最终源码仍以重新运行的完整门禁为准。
归档日志：/Volumes/tool/tmp/zero-endpoint-lifecycle-gates-20261006-attempt1/。

第二次完整运行：2026-10-07 00:49:26 至 02:45:23 CST，退出码 101。
146 个目标结果为 2377 通过、1 失败、155 忽略；唯一失败是关联端点在后序
监听绑定失败回滚后的 UDP Echo 超时。之前的逐个预绑定允许前序 WireGuard
候选监听提前运行，影响远端认证会话。已改为通用批量 socket 预留，并补充
候选 socket 释放断言和删除资源端口复用回归；最终完整门禁已通过。
归档日志：/Volumes/tool/tmp/zero-endpoint-lifecycle-gates-20261006-attempt2/。
期间定向复测尚在编译时，为合并端口复用兼容修正而主动中断（退出 -2），
没有执行测试，不计为通过。

最终重跑编译阶段排查到宿主反复 Maintenance Sleep：电源日志显示每次
DarkWake 约 45 秒、随后睡眠约 3 分钟。2026-10-07 04:17 CST 使用
caffeinate -is -w 15671，为该次 Cargo 进程建立临时保持唤醒断言；不修改
系统电源配置，Cargo 结束自动释放。曾临时暂停一个自有编译进程以降低
并发，已恢复；编译参数、特性和测试范围未更改。不能把包含宿主睡眠的
外层墙钟耗时视为持续 CPU 编译或测试执行时间。短采样的符号解析未结束，
已停止该诊断，不作为原因证据；电源日志才是确认的睡眠证据。

第三次运行在端口复用新用例失败后主动中断：2026-10-07 03:25:32 至
04:45:04 CST，退出 130；71 个完成目标为 1522 通过、1 失败、47 忽略。
该用例错误地修改已物化配置的端点 tag，仍携带旧端点的派生角色，未真正
删除原监听。改为编辑规范配置源再解析，并为删除场景增加真实 socket
释放断言。运行时不会依据标签前缀猜测并删除显式保留的旧角色。
归档日志：/Volumes/tool/tmp/zero-endpoint-lifecycle-gates-20261006-attempt3/。
最终重跑从进程启动即建立临时保持唤醒断言，绑定门禁 runner 的生命周期。
最终 runner 已正常结束，临时保持唤醒断言已释放，无遗留编译/测试进程。
