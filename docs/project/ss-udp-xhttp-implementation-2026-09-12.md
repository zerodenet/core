# SS UDP 与 VLESS XHTTP 实现收口

本次处理对标审计中的两项实现欠账。基于工作区 HEAD
`de97b051d97b15fd148c304d4a830bb4e85e549c`，保留已有 SS replay-window 和
VMess AuthID 修复；不把它们计为本次新增实现。不提交、不推送、不操作生产。

## SS UDP

- 将长期累积的响应映射合并为协议所有的有界 bindings，300 秒空闲过期；
  多用户响应索引随映射和用户生命周期清理。
- 将 SIP022 replay session 与业务响应映射分开回收，至少保留 61 秒；
  满容量时拒绝新会话，不提前驱逐仍有防重放意义的记录。
- 协议 responder 使用持久的 30 秒维护期限，无新数据时仍执行回收，
  取消一次接收不会重新计时。
- 代码留在 `protocols/shadowsocks/src/udp/inbound/`，未向通用运行时加入
  SS 密钥、帧格式或会话窗口知识。

策略阈值与测试边界见 [SS UDP 状态](../protocols/shadowsocks-udp-state.md)。

## VLESS XHTTP

- 移除旧 `X-Session-Id` 私有配对，采用 path/session/sequence packet-up
  和独立 streaming POST/GET 的 stream-up；保留 stream-one。
- `auto` 通常选 packet-up，REALITY 下选 stream-one；入站 auto 接受三种模式。
- HTTP/1.1、HTTP/2 请求执行与会话配对在 `zero-transport`；
  VLESS 逐流鉴权在协议包；通用运行时通过 `InboundTransportMultiplexer`
  管理并发路由任务。adapter 未增加监听循环、spawn 或 JoinSet。
- 补充序号窗口、会话与请求数量限制、共享上传内存预算、断连取消、
  POST 确认和下载排空，失败后不重放已发送业务数据。
- VLESS 包、当前生成的 lockfile、文档、准备脚本和专用 CI 固定到
  Xray-core v26.3.27 / `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`。
  VMess 包版本没有跟随变更。版本一致不等于全部功能完成。

旧私有配对两端需一起升级；依赖单载体中继的配置应显式设为 stream-one。
HTTP/3、高级 XMUX/downloadSettings 和可配置元数据位置仍不在已实现范围，
详见 [XHTTP 契约](../protocols/vless/xhttp.md)。

## 验证记录

固定官方二进制的新增互通矩阵：9/9 通过。覆盖 packet-up、stream-up、auto
双向 TCP/UDP；每例包含四路并发 TCP，并覆盖 TLS 下的 HTTP/2 入站。
外部进程先执行配置检查，TLS 测试使用生成证书的指纹校验。

原有 stream-one 官方回归 7/7 通过：双向 TCP/UDP、TCP MUX、XUDP、
SOCKS 首跳后的最终跳 TCP/UDP。加上新增矩阵，本次 XHTTP 官方互通合计 16/16。

格式检查和全特性、全目标 clippy（`-D warnings`）已通过。
首轮全量门禁中，架构扫描把 XHTTP 错误文案中的 mixed 误判为协议名；
改为 inconsistent 后，原有 26 项架构约束测试全部通过，未修改门禁规则。
最终 `RUST_MIN_STACK=16777216 cargo test --workspace --all-features --no-fail-fast`
通过：1678 passed、0 failed、111 ignored。Ignored 未计为通过。

以下构建均通过：

- `cargo check -p zero-proxy --no-default-features`
- `cargo check -p zero-proxy --no-default-features --features socks5,vless`
- `cargo check -p shadowsocks --no-default-features --features runtime`

最小 feature 构建仍输出 unused/dead-code 等警告；全特性 clippy 的
`-D warnings` 门禁通过。`git diff --check` 和固定参考版本一致性检查通过。
日志保存在 `/tmp/zero-implementation-checks-20260912/`。

生产部署和生产观察仅是后续最后步骤，不计入实现完成度。
