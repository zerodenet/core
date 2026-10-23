# 原生 Stream relay 与 HY2/QUIC 进度证据

本检查点仍属于进行中的 PR03/PR04A，不代表全部 17 项完成。代码依次为
`acc5410a34a41562a6012ca718cc99ce2ec6c2c4`、
`bae5bfe785baa057ea9d3d2e23703ddab9bc21cd`、
`6a1d8bcb9cc870a9e772904716a85a089c557139`。
最终冻结 tree 为 `a9ada0f1b7514175e65051df3b859ade3ddff715`。
[验证记录](pr04-relay-validation.json)和[原始日志及摘要清单](pr04-relay-ci-root-causes.json)
保留实际命令、失败、对照、源码/压缩件 SHA-256 和二进制身份。

## 原生 relay 的实际范围

已登记的网络 Stream 节点组成的链路使用同一 Stream.connect 入口，逐层构造严格更短的上游前缀。
当前真实协议为 SOCKS5；两跳、三跳均用实际 TCP 转发器与旧执行者直接对照。
测试覆盖预读、请求、EOF/半关闭、物理 socket 元数据、链路标签和逐层 Outer 字节。
旧 relay 将配置节点名作为 Domain 发送，即使其内容是 IP 字面量；新路径保留这一 wire 表达。
旧 relay 的握手不进入业务 Flow 计数，新路径保持该行为，而不是把每层握手加到业务会话。

业务事实始终保留真实 Engine SessionId；前缀只改变本层 ConnectRequest 的节点目标。
绑定检查实际角色及资源代际、端点、拨号策略、网络代际和会话；父取消不能被无关子 token 替换。
已经建立前缀后再发生网络变化或取消，投递仍为 Unknown，不能重放。
中间角色撤权在物理拨号前失败；最终认证拒绝的 V1 code/message/stage 与旧路径相同，
同时保留实际组件/业务身份。混合旧协议链仍使用既有执行者，等待对应协议迁移。

首次冻结 `bae5bfe7` 的完整测试为 **2708 / 1 / 159**；唯一失败是新模块越过了既有
engine-leaf 投影边界。没有修改守卫白名单。最终链路组装只消费中立的 prepared candidate，
engine leaf 仍只在原有 inventory 边界投影，拨号策略由冷路径组件保存。
原守卫随后通过，最终完整测试为 **2709 / 0 / 159**。

## HY2 低带宽时延积停滞

`a0a012` 的 [CI39317685498](https://github.com/zerodenet/core/actions/runs/39317685498)
完整执行后为 **2698 / 1 / 159**。原 Browser 生命周期用例通过；唯一失败是 HY2 的原十秒截止：
客户端停在读 pong/EOF，服务端停在 finish/确认。此前原用例额外 800 次并发、100 次串行均通过，
这些成功不消除 CI 失败，也不是根因已修复的证据。

可确定复现的问题有两个。carrier 原来的最小窗口为一个 MTU，而 Quinn 发送普通包之前要求
flight 加完整包预留严格小于窗口。小带宽时延积下，窗口等于一个 MTU，即使没有在途数据也不能
发送后续响应。真实 QUIC 回归使用原协议实现、协商速率和十秒截止，旧配置实测
window=1326、MTU=1326，随后超时。最小窗口改为两个包；认证、协商速率和原用例断言未放宽。

只改两个包仍然失败，日志也保留。响应及 FIN 已送达后，客户端关闭时仍有待发应用帧，
旧 Quinn 从这些 pending 帧推断 ack-eliciting，并在实际 close-only 编码之前执行数据拥塞检查，
使 CONNECTION_CLOSE 也无法送达。close 路径实际只编码 ACK/CONNECTION_CLOSE，按
[RFC 9002 §7.2](https://www.rfc-editor.org/rfc/rfc9002.html#section-7.2)
不应受数据拥塞检查限制。修正仅豁免该 close 路径；地址验证、防放大和关闭重传限制保留。
确定性的 Quinn 状态机回归先完成握手，再设置耗尽窗口并保留待发数据：旧代码对端得不到关闭，
新代码收到原 42/done 关闭原因。完整 dependency suite **269 / 0 / 0**。

两项修正合用后，真实 workspace 中 13 项 HY2 相关用例通过。相同实际库构建的公共 API 探针中，
保留关闭修正、只将实际 Factory.minimum_packets 恢复为 1，仍于十秒失败；同一探针二进制设为 2
或使用实际 Profile 默认值均通过，并抓到 CONNECTION_CLOSE。探针只改变本地 fixture 的工厂参数，
不修改生产配置或参考客户端。真实 1 CPU/1 GiB 容器中，原用例和新用例各 20 次，合计 **40 / 0**。

旧 CI 没有记录当时 RTT/cwnd，不能倒推出其精确窗口；其停点与可复现机制一致属于推断。
原 f90 历史失败也保留这一取证限制。standalone dependency suite 使用自己的忽略锁，解析到
rustls 0.23.46；主工作区仍使用原审查锁中的 patched rustls 0.23.40 和 Tokio 1.54.0。
两个依赖图分别记录，真实 carrier 回归和完整 workspace 验证生产依赖图。

## 资格及未完成项

冻结 6a1d8bcb 的 fmt、workspace check、严格 Clippy、完整 workspace、Quinn suite、默认 release、
实际 thumb no_std、最小 proxy feature 和实际依赖边界全部通过。默认 release 的 build-info 为
6a1d8bcb，SHA-256 为 `947313e4622f3a7d6e231109045bbfa7ff5dc1ac749baf3c3442adb7647703c4`；
不包含可选 connector/grpc/WireGuard 的默认 release 资格。

a0 的七项协议 workflow 和 hosted TUN workflow 成功，VLESS 实际为 250/0/1；
三个独立 TUN smoke、macOS offline 跳过。a0 Windows 原始报告明确 `executed=false`，
缺少合适的第二接口。它们不是 6a 的远端资格，也不是用户物理设备验收。
新 head 的 CI 需另记实际结果。[既有 Browser/XHTTP 取证](pr04-ci-root-causes.md)中的官方参考
20 轮 19/1、40 轮 39/1 和 pinned 客户端字节计数反例继续保留；单次 VLESS workflow 成功不抹去它们。

这里只授予零共享 I/O/驱动 Stream 的临时宿主。被中止 start 的所有者保留、真实任务预算和非空
驱动的确认清理仍需补齐，再接入 replay 池、plugin、MUX/QUIC 等资源协议。
其他 Stream/carrier、混合链、内部 DNS/UDP、Datagram、入站、Packet、RuntimeHost、单实例事务、
旧桥清除及最终消费者/FFI/平台/同负载资源验收继续在同一分支和 Draft PR124 实施。
