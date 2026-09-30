# 本地测试流程整理记录

日期：2026-09-30。范围为 Zero 内核仓库的本地验证流程与端点测试组织。

## 已确认的开销

当前工作区通过 Cargo metadata 发现 270 个 integration test target，其中
zero-proxy 占 78 个。每个 target 都需要独立链接和启动；源码按领域拆成
短文件，不要求同时拆成独立 executable。

本机为 16 GiB 内存、4 个物理核心/8 个逻辑核心；项目卷位于内部 PCIe SSD，
检查时项目卷剩余约 205 GiB、系统卷约 52 GiB。当前瓶颈不能归因于磁盘已满。
冻结源码的完整测试通过：328 个目标，2280 passed、0 failed、155 ignored，
总计 7562.03 秒（约 126 分钟），编译阶段为 17 分 15 秒。所有 test harness
报告的执行时间之和为 242.81 秒；剩余时间包含启动、文档准备、调度等开销，
不能全部归为某一个 macOS 系统进程。

一个约 184 MiB 的端点测试 executable 仅执行 `--list`，两次实测耗时分别为
12.23 秒和 7.78 秒，而 CPU 时间接近零。同期观察到 syspolicyd 占用较高，
这支持“存在启动等待”的判断；是否由 macOS 安全检查直接导致尚未完整证实。
临时副本签名实验第一次 26.93 秒、热启动第二次 0.03 秒，不能据此把签名
认定为稳定解决方案。临时副本已删除，没有调整系统安全策略或 Cargo profile。

## 流程调整

- 提供 `scripts/test-workspace.sh`：默认完整 workspace/all-features，固定
  16 MiB 测试栈；显式 `--test` 为编辑阶段定向回归，不能冒充全量验收。
- 沿用现有 Cargo test profile 和 feature 策略，不在每轮验证前清空缓存。
- `--jobs` 只限制编译 worker；保留完整测试范围，不强制所有设备使用同一值。
- 与 CI 共用起止时间、总耗时、退出码和失败汇总；汇总器失败不覆盖命令失败码。
- 运行时、路由、配置或协议变更仍需完整工作区验收。完整门禁已经包含的
  测试不再单独重复执行；发布构建按实际制品需求单独进行。

入口脚本通过 macOS `/bin/bash` 3.2 回归，包含完整/定向参数、栈、worker、
日志、参数错误、命令失败、日志写入失败及汇总器失败传播；并通过 Bash 语法检查。

## 端点目标聚合试点

完整源码门禁完成后，将七个端点 integration target 汇入
`endpoint_contracts`，保留独立短模块及全部 18 个用例。共享 fixture 使用一次
Rustls 初始化与同一端口分配器，避免聚合后多份全局初始化或端口冲突。

- 用例名称集合核对完全一致，默认并发运行 18 passed、0 failed、0 ignored。
- 新入口实际定向回归耗时 40 秒，其中编译 21.02 秒，harness 运行 1.14 秒。
  这不是同条件的完整工作区耗时对比，不能推出全量提速百分比。
- workspace integration target 从 270 降至 264；zero-proxy 从 78 降至 72。
- 七个旧 executable 合计 1,334,901,600 字节，新入口为 196,786,600 字节。
  该组所需测试 executable 总体积减少 1,138,115,000 字节（85.26%）。旧缓存
  未清空，因此这不是已经释放的磁盘空间。
- 聚合目标 `cargo clippy --workspace --all-features --test endpoint_contracts --
  -D warnings` 通过，耗时 7.40 秒；fmt、diff 和脚本语法检查通过。
- 完整源码门禁先于布局迁移，迁移后只执行受影响目标回归及 Clippy，不把这次
  定向结果声明为再次完整跑完工作区。

证据：/Volumes/tool/tmp/zero-endpoint-suite-before-20260930.json、
zero-endpoint-suite-after-20260930.log、zero-endpoint-suite-comparison-20260930.json、
zero-endpoint-suite-after-targets-20260930.json 以及
zero-endpoint-suite-clippy-20260930.log，均位于 /Volumes/tool/tmp/。

这次仅覆盖端点领域。没有声称整个工作区已经完成聚合，也没有将减少目标数
等同于未经同条件测量的总耗时提升。

## 后续整理顺序

优先检查 zero-proxy 中重复链接成本较高的能力、路由、reload 和 Packet 路径
测试；按责任领域聚合，每轮保存用例清单，核对断言与运行结果。外部互操作、
特权设备和不同 feature 的目标保留明确执行边界。其他领域尚未完成整理。
没有引入自动依赖影响分析或用定向结果替代运行时变更的完整门禁。

使用方式见 [工程规则](tooling.md#本地测试入口)。
