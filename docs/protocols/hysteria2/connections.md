# HY2 共享认证连接与 UDP 会话隔离

本批消除 TCP、普通 UDP 流、UDP packet-path 各自建连和认证的差异。
同一出站身份共用一条认证连接，带宽协商、Brutal/BBR、连接流控与保活均属于该连接。
不新增用户配置；沿用 Zero 的出站与传输字段，由协议拥有连接身份与生命周期。

## 参考与分层

参考固定发布标签 Hysteria `app/v2.12.2`，提交
`619a6f856b69fb7ee6a7a379e810e68b84004605` 的
[client.go](https://github.com/HyNetworks/hysteria/blob/619a6f856b69fb7ee6a7a379e810e68b84004605/core/client/client.go)、
[udp.go](https://github.com/HyNetworks/hysteria/blob/619a6f856b69fb7ee6a7a379e810e68b84004605/core/client/udp.go) 和
[reconnect.go](https://github.com/HyNetworks/hysteria/blob/619a6f856b69fb7ee6a7a379e810e68b84004605/core/client/reconnect.go)。
目标是共享认证连接、按会话分发和关闭/新建行为一致，不复制官方应用的配置或 Go 对象结构。

- `protocols/hysteria2::transport::pool` 拥有共享身份、并发建连、认证与连接退役。
- `protocols/hysteria2::udp::{dispatch,channel,pump}` 拥有单一接收入口、会话注册、分发、分片重组和流任务。
- `zero-transport` 继续拥有 QUIC 拨号、TLS 和通用拥塞/窗口执行；本批无需修改 Quinn。
- proxy 只传递已有运行输入并桥接协议拥有的 channel；通用 runtime 不解析 HY2 session ID 或重建缓存身份。

## 生命周期

连接身份包括出站 tag、服务端地址/端口、凭据、SNI、TLS 校验开关、指纹、传输设置和出口代次。
不同身份不能共享。每个身份在并发首次访问时只拨号、认证一次；取消建连会释放空槽，其他请求仍可继续。
TCP 请求和各 UDP 会话持有认证 guard，单个 UDP 会话退出不会关闭其他使用者的连接。

池的空闲回收只作用于没有活跃借用的连接；256 个缓存身份达到上限后只淘汰空闲项，
全部活跃时拒绝新身份，而不是移除活跃项后为相同身份重复建连。
重载立即退役缓存，已有使用者持有旧连接直至结束，新请求使用新缓存。

故障后旧 UDP 会话关闭，新建请求与 TCP 走同一个恢复入口。TCP 只在代理请求发送前重试建流；
不重放已经发送的业务数据，不承诺把断开的 TCP 字节流或 UDP 会话无缝迁移到新连接。
服务端拒绝 UDP 能力时，UDP 创建报错，TCP 仍可使用共享认证连接。

## UDP 分发与资源边界

每条认证连接按需启动一个 datagram reader。每个 UDP channel 注册独立 session ID，
普通流与 packet-path 使用同一分配器。普通 UDP 流的逻辑缓存身份独立于物理连接身份，
同一目标的不同逻辑流也不会共享响应队列。运行时保留的 opaque resume 结束时通知协议注销会话，
通用缓存仅通过中立的关闭状态清理失效句柄，不读取 HY2 私有字段。
ID 在同一连接内不复用，耗尽后拒绝新会话，避免迟到包进入新会话。
每连接最多 4096 个活跃 UDP channel，每会话有 64 个入站 datagram 队列槽；
慢消费者只丢弃自身溢出包，未知 ID 被忽略，均不阻塞其他会话。
这些是内部资源边界，不增加第二套配置字段，也不声称与官方内部缓冲大小相同。

分片重组沿用现有协议实现，由每个 channel 独立持有；关闭 channel 删除注册及其重组状态。
连接关闭清空注册并唤醒等待者，认证 guard 析构中止 reader，避免保活留下孤立接收任务。
packet-path 现在也使用分片/重组，不再用固定会话 ID 和独立 QUIC reader。

同时修复入站 session ID 丢失：将其映射到已有中立 `client_session_id`，
内核按 `(target, port, client_session_id)` 区分流。否则同连接内两个 UDP 会话访问同一目标时，
可能复用同一内核流并覆盖响应映射。该映射由 HY2 拥有，内核不认识 HY2 私有报文。
通用 direct UDP 出口对显式带 `client_session_id` 的逻辑流使用独立 socket，
回复携带内核 session ID，而不是仅凭目标地址反查归属。取消或失败会释放该流 socket，
出口代次变化后仍保留隔离要求；普通无会话标识的 UDP 继续使用原共享 socket 策略。

## 验收

- 并发 TCP、普通 UDP 与多个 packet-path 只完成一次认证，分片回复正确分发。
- 同一目标的两个入站 UDP 会话在倒序响应下仍保持隔离。
- 连接故障后并发请求只重建一次，旧会话关闭；重载后旧活跃会话仍能完成。
- TLS/凭据/出站/传输设置/出口代次隔离；活跃连接跨空闲检查仍复用，无用户后可回收。
- 队列溢出与未知会话不影响其他会话，ID 不回绕，连接析构唤醒接收者。
- 官方 v2.12.2 发布程序对照并发 TCP/UDP 的流量内容与单次认证。

长期运行、任意丢包/网络切换、全部 UDP 超时与多跳故障组合仍需单独验收；
基础互通和上述边界回归不作为这些场景的通过证据。

2026-09-09，本地 macOS 的 HY2 运行时 23 项回归通过；真实代理两项同目标隔离测试通过，
分别覆盖 HY2 入站直连和 HY2→HY2→直连路径。每项连续收发两轮 1600 字节数据，
目标倒序回复，并检查两个逻辑会话使用不同且各自稳定的直连出口 socket。
validation-only 构建通过，仍有该特性组合的 unused/dead-code 告警，不记为严格无告警构建。

最终代码 `6c3778dd` 的[工作区 CI](https://github.com/zerodenet/core/actions/runs/34319436056)
通过 1524 项测试（91 项显式忽略）、严格检查与代表性最小特性检查；
[官方互通](https://github.com/zerodenet/core/actions/runs/34319435875)通过官方 10 项、sing-box 6 项、
Quinn 268 项。新增官方用例同时运行 4 路 TCP 和 4 路 1600 字节 UDP，
校验全部回复，并断言官方服务端仅记录一次成功认证。完整验收边界见[对齐清单](parity.md#tcpudp-共享认证连接与回复隔离)。
