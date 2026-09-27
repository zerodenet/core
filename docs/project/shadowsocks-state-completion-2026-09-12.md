# Shadowsocks 五项状态实现收口

> 后续开发状态以 [固定官方版本实现矩阵](../protocols/shadowsocks/parity.md) 为准；下文保留本次后续开发之前的审计或阶段记录。

固定对标 shadowsocks-rust v1.21.2 / `a03006a753486e64717d6e3afa91e0c6d043c557`。
本次在已有 SS 入站回收、XHTTP 和 VMess 工作区改动上继续实现；不改变协议
参考版本，不提交、不推送、不部署。

| 原缺口 | 实现结果 |
| --- | --- |
| UDP 逐包随机 session/packet ID | 协议 codec 持有稳定 client ID 和递增序号；入站按 client ID 保存独立响应 sender；计数器溢出失败关闭，不能回绕或重放 |
| UDP 回包身份与防重放 | 客户端强制响应方向、匹配 echoed client ID，再按 server ID 执行有界重放窗口；服务端拒绝响应方向报文 |
| 出站 UDP 接收任务未随 flow 释放 | flow 持有 abort handle；drop 取消静默接收任务并释放 socket，不依赖上游再来一个包 |
| 4096 字节接收缓冲截断 | 接收缓冲覆盖完整 UDP 数据报，并拒绝非配置上游的报文 |
| TCP2022 salt 池无容量上限和闲置清理 | 每池 262144 上限、61 秒保留、FIFO 过期队列、30 秒维护任务；满时拒绝新盐，空集合释放分配，pool drop 取消任务 |

`ShadowsocksDatagramCodec::new` 创建 association；clone 共享该 association。
底层无状态 framing API 仅用于线格式，运行时工厂使用有状态 codec。
旧 Zero 对端如果仍逐包更换会话，会继续消耗短会话限额，应升级对应对端，
不能通过提前删除有效重放记录兼容这种行为。

协议状态与线格式保持在 `protocols/shadowsocks`；通用 proxy runtime 没有
新增 SS 密钥、会话 ID、序号或帧解析知识。新目录职责已写入 AGENTS.md。
详细阈值和生命周期见 [协议状态说明](../protocols/shadowsocks-udp-state.md)。

## 本地证据

- SS 定向回归已通过；覆盖三种 2022 cipher 连续 2050 包、重复/乱序/越窗、
  错误方向/回显 ID、SIP023 EIH、计数器耗尽、60000 字节 codec 数据、8000 字节实际
  UDP 数据、静默 flow 释放，以及 TCP 池容量/闲置内存释放。
- 代理 SS 回归 5/5 通过：六 cipher 直连、SOCKS5/SS/HY2 中继；SS→SS 已覆盖
  三种 2022 cipher，并使用不同跳密码验证。
- 固定官方 SIP023 互通 5/5 通过：TCP/UDP 双向，新增连续收发用例对两种
  AES2022 方法各执行 1100 包，含 8000 字节回包及重复回包拒绝。
- 官方 macOS x86_64 压缩包 SHA256：
  `d47509328f0f267889154dd207e658f92f8b43c34c31800069a223847ff113cc`。
  准备脚本已实际执行，核对发布摘要与二进制版本。专用 CI 固定同一版本。
- 官方六 cipher 运行时 UDP 互通通过，本轮官方互通共 6/6。
- 格式检查和工作区全特性、全目标 clippy（`-D warnings`）通过。
- 完整工作区全特性测试最终通过：1688 passed、0 failed、112 ignored；
  ignored 不计为通过。首次运行 1687 passed、1 failed、112 ignored，失败为
  `reload_reconcile::timed_out_reload_waits_for_last_known_good_rollback`
  等待监听端口可达超时；未修改代码或放宽阈值，单独重跑该目标 11/11、
  随后完整工作区复跑均通过，首次超时原因尚未确定。
- `cargo check -p shadowsocks --no-default-features --features runtime` 与
  `cargo check -p zero-proxy --no-default-features --features shadowsocks`
  均通过；精简特性仍有 unused/dead-code 警告，不表述为无警告构建。
- 本机门禁日志目录：`/tmp/ss-five-items-20260912/`，其中 `workspace.log`
  保留首次结果，`workspace-rerun.log` 为最终完整复跑结果。

生产验证是部署后的最后步骤，不计入这五项实现完成度。
