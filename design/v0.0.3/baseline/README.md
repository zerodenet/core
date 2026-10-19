# PR01 行为与资源基线

本目录保存真实旧代码的输入、输出、依赖解析和验证证据。可重复的二进制行为及固定 loopback 资源基线已采集，其他资格缺项继续明确记录；这些产物不表示完整重构、协议迁移或全部消费者验收完成。

## 固定来源

| 来源 | 冻结身份 |
| --- | --- |
| 旧 Core 运行代码 | `2eedc2b8d8e2b0e16dffe32d3f83d2f0f2dbcc8a`，独立 detached 工作树 |
| GUI | `zerodenet/znet-sink@d6520b50df646f01d69f9e64eabffec26c1554c4`，云端干净检出 |
| 面板 | `zerodenet/zboard@7c7283fb34840883fccaba1de6e0a98edb24dfb1`，云端干净检出 |
| v0.0.2 官方包 | `v0.0.2-rc.202610142218` Linux x86_64；archive SHA-256 `2bdf2d662ca2702c945d912f5756728d3cd4e4d8e48db17b98c0034c34ac9720` |
| Core 工具链 | Rust/Cargo 1.99.1、protoc 35.0、CMake 4.4.4；Go 参考工具链 1.25.14，面板按 go.mod 使用 1.26.8 |

Cargo.lock 原先被忽略且未存在。`resolved-dependencies.toml` 是实际首次解析的证据副本，不是根目录生产 lockfile，也没有改变追踪政策或执行 cargo update。其 SHA-256 为 `0275dc0ac51fad8d380056defb4e4fb651edff477d0f8e6cb0166c05ca720cd7`。

## 已取得证据

- 完整 workspace/all-features/unit/integration/doctest：2635 passed、0 failed、159 ignored。未将 ignored 协议互通或特权项目算作通过。
- `thumbv7em-none-eabi` 上实际编译 zero-traits、zero-core 成功；记录了 traits/core/platform 正常依赖和 config feature 树。
- GUI 的 client-core/engine-client 纯 Rust 测试：59 passed；内核能力前端测试：2 passed。它们不替代完整 Tauri 或真实设备资格。
- 面板 `internal/adapters/zero` 测试通过；未修改消费者源码。
- GUI 自带 `check-kernel-compatibility.py` 对真实 v0.0.2 官方二进制和旧 v0.0.3 二进制分别通过查询、临时应用、无效 schema 拒绝、修订/实例关系及 route/WireGuard 验证。没有启动 WireGuard 设备或 TUN。
- 旧二进制捕获：29 个原仓库 example 输入中 28 个接受、1 个拒绝；22 条真实 IPC 回执覆盖配置临时/持久应用、无效 schema、明确身份 stats.reset、端口冲突及状态查询；53 条事件帧包含 snapshot、flow.started/routed/completed、config.changed、stats.reset 与采样。

完整输入、输出和摘要保存在 fixtures；未取得的数据保持待采集，不伪造预期响应。`python3 design/v0.0.3/baseline/check-baseline.py` 只核对存储证据一致性，不冒充重新执行。

## 既有缺陷与采集修正

| 项目 | 实际观察与处理 |
| --- | --- |
| 旧示例 | `examples/v0.0.3/vless-udp-chained.json` 在 SOCKS5 入站配置了未知 `udp` 字段；旧内核返回 unknown field，作为既有缺陷保存，未通过放宽解析掩盖 |
| build-info 身份 | 当前字段报告短 SHA。捕获工具初次错误地直接比较 8 位与 40 位值；现同时核对完整源码提交及已报告 SHA 前缀，未修改内核 |
| 持久化比较 | 工具初次假定持久化 JSON 与省略默认值的输入字面相等；实际会补齐默认字段。已保存完整持久化输出并检查请求规则语义，未把该假设错误归为运行缺陷 |
| 面板工具链下载 | Go 自动下载 1.26.8 的存储重定向被拒绝；改用官方 go.dev 发布归档并验证官方 SHA-256 后运行原测试成功，没有修改 go.mod 或安全设置 |
| FFI 文字与实现 | 现有头文件将 last_error 描述为线程本地借用指针；实际 Rust 使用全局 Mutex、取走错误并返回应释放的 CString。先保存头文件与实际行为，不能只凭注释声称 ABI/行为已验收 |
| 资源采样器 | 首轮退出瞬间 VmRSS 缺失使采样线程报错；数据未用于冻结。修正为核对进程退出并限制存活进程的缺失重试后，最终五轮完成且无异常 |

## 资源测量范围

宿主约束是 4 CPU/16 GiB，不能冒充 RS01。已验证普通 Docker 容器可实际设置 `cpu.max=100000 100000`、`memory.max=1073741824`、禁用额外 swap；官方 Python 镜像固定 digest 为 `sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1`。

`resource_probe.py` 只接受实际 1 CPU/1 GiB cgroup，测量构建后的二进制。编译、参考进程与其他负载必须先结束；容器内 Python/echo 计入 cgroup 峰值，内核 RSS/线程/FD 单独采样。TCP profile、UDP/Packet、容量拒绝、任务数及阶段故障注入分别记录，不能把 TCP loopback 样本当作全部 RS01 场景。

固定 profile 的五轮原始轨迹以可重复 gzip 保存，并在 `resource-baseline.json` 冻结实测范围及对照规则：内核峰值 RSS 40,706,048–41,996,288 字节，线程峰值 6–8，FD 峰值 124；含探针和 echo 的 cgroup 峰值 73,912,320 字节，OOM 为零。吞吐和 p95 的完整原始数字及 profile 均在该文件，不能外推到任意协议和业务规模。

当前 CLI 没有权威异步任务数接口，不能拿 Linux 线程数替代 Tokio 任务数。独立公开 Rust 库入口测得任务数 `0 → 3 → 35 → 3 → 0`，范围排除 CLI/IPC/Connector 应用任务。`task-observation.json` 保存原始采样；`task-allocation-observation.json` 另外记录整个观察器进程的 Rust 分配，包含原生 echo 和探针，不能称为单独内核堆或 RSS。分配观测只在独立工具内按已有测试方法委托 System，不进入生产库。

`futex-observation.json` 及原始栈轨迹保存 strace 6.13 的原生协调等待诊断：包含 park/条件变量，不能把所有 futex 等待都称为锁竞争。默认容器能力可完成追踪，没有修改宿主或容器安全策略。追踪包装器的 RSS 不作为内核资源数据，时间也不混入 release 吞吐基线。

Packet/具体加密协议资源 profile、新容量拒绝、完整应用任务归属及真实设备资格仍待对应阶段验证；现有 loopback 数字不能替代这些证据。新增生产资源默认值必须绑定已验证 profile，不能由这些部分测量猜出任意协议的全局内存上限。

## 重放入口

`capture.py --binary <旧二进制> --source <固定旧源码> --output <捕获目录> --expected-commit <完整SHA>` 读取固定 example 和既有管理测试输入形状，真实运行旧进程并保存响应。所有端点、身份与业务负载均为合成 loopback 数据，不接触安装的内核或用户配置。

[整体方案](../README.md) · [完整验收门禁](../validation.md) · [同一 PR 交付规则](../delivery-policy.md)
