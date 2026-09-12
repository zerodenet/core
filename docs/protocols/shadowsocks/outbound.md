# Shadowsocks 出站

`outbound.rs` 和 `outbound/tcp.rs` 构造 TCP 请求及协议会话；
`udp/outbound/` 提供 UDP flow、codec、packet-path 和配置入口。
协议根目录不重新导出 UDP 类型。

`ShadowsocksTcpConnectConfig` 解析 cipher/密码，发送目标请求，再创建持有
上下行密钥、nonce 和读写缓冲的协议 stream。SIP023 使用 iPSK 链构造 EIH，
最终 uPSK 加密数据并验证响应。新连接共享所属客户端的 replay guard。

`ShadowsocksUdpFlowResume` 为每个真正的关联创建 stateful codec，克隆共享
sender/session/replay 状态。缓存身份由协议以长度前缀字段的 SHA-256 构造，
不把密码或插件选项明文交给通用 runtime。

`transport/` 为 adapter 提供 leaf/flow 计划。adapter 只投影 engine leaf，
runtime 执行拨号和路由。插件租约在此打开本地 carrier，失败不能直连绕过。
没有插件时保留 SS/SOCKS5 UDP packet-path 组合；外部插件不支持包装已有
relay stream 或嵌套 UDP codec，见 [原生配置](configuration.md)。

完整方法的 TCP、UDP 验证与固定官方基线见 [实现矩阵](parity.md)。
