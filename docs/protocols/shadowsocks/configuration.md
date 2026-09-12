# Shadowsocks 原生配置

固定协议基线为 `shadowsocks-rust 1.21.2`。配置使用 Zero 的
`inbounds[].protocol` / `outbounds[].protocol`，不导入官方 CLI 或 manager 协议。

## 密码和用户

`cipher` 支持完整[方法目录](../../../protocols/shadowsocks/README.md)。
v1 密码按原始字节处理，包括空串和空格；2022 必须使用对应长度的 base64 PSK。
`none`/`plain` 可省略密码。为保留 Zero 空用户注册表的禁用语义，v1 入站
如需空密码，必须显式配置 `users: [{"password": ""}]`。
流密码无法逐用户认证，同一 listener 只允许一个用户。

AES 2022 单用户入站使用 `password`；SIP023 多用户入站使用
`identity_password` 加 `users[].password`，两者分别为 iPSK 和 uPSK，不能混填。
AES 2022 出站可配置 `iPSK[:iPSK...]:uPSK`。ChaCha 2022 没有 EIH key chain。
`principal_key`、限速、额度和用户替换沿用 Zero 原生策略。

## 重放与状态

入站和出站均接受：

```json
{
  "replay_attack": "default",
  "state_limits": {
    "udp_timeout_secs": 300,
    "udp_capacity": null,
    "tcp_replay_capacity": null
  }
}
```

v1 支持 `default`、`ignore`、`detect`、`reject`；前两者接收放行，
`detect` 记录重放，`reject` 拒绝重放。2022 始终拒绝重放，不受放宽策略影响。
默认没有隐藏容量限制。显式容量是 Zero 原生资源准入策略，满时拒绝新状态，
不会淘汰尚有效的重放条目。超时和显式容量必须大于零。
`runtime.udp_upstream_idle_timeout_seconds` 控制通用上游 socket 的生命周期；
协议回收与 socket 回收是不同层的责任。细节见[状态文档](../shadowsocks-udp-state.md)。

## SIP003 / SIP003u

SS 入站、出站的 `protocol` 均可增加：

```json
{
  "plugin": {
    "command": "/path/to/plugin",
    "args": [],
    "options": "server;host=example.com",
    "mode": "tcp_only"
  }
}
```

`mode` 为 `tcp_only`（默认）、`udp_only` 或 `tcp_and_udp`，表示由插件承载的
流量类别，不是关闭另一类 SS 流量。未被插件承载的类别按官方方式直连 SS。
`command` 与 `args` 直接启动进程，不经过 shell；`options` 原样写入
`SS_PLUGIN_OPTIONS`，不在 Zero 中解析其插件私有语法。
`SS_REMOTE_HOST/PORT`、`SS_LOCAL_HOST/PORT` 按客户端/服务端角色配置。
`obfsproxy` 保留参考实现的专用命令参数形式。

插件启动失败返回错误，不能静默绕过插件。入站插件退出停止其所属 listener；
出站插件退出使对应流失败，UDP 空闲订阅也会关闭。配置重载清理旧计划，
已有流持有的进程租约保留至流结束；新计划启动新进程。关闭服务回收子进程。
TCP 做本地就绪探测。SIP003u 没有 UDP 就绪握手，启动期间 UDP 可能丢包；
Zero 不抢占插件端口探测，也不自动重发业务报文。

外部插件自己建立连接，不能包装已有 Zero relay stream，也不是嵌套 UDP
packet-path 编解码器；此类 Zero 扩展组合不会绕过已配置插件。插件具体实现、
证书和自身选项由插件负责，Zero 不把它们变成另一套配置/控制 API。

## 官方服务能力到 Zero 的映射

| 官方责任 | Zero 承接位置 |
| --- | --- |
| listener、server 地址和凭据 | 原生 inbound/outbound 配置 |
| ACL、DNS、目的地选择 | `route`、`dns` 和 engine/runtime 既有策略 |
| 服务启停与配置替换 | 原生 Zero API、HTTP/IPC/gRPC 配置与生命周期接口 |
| manager 用户变更和统计 | 原生用户注册表、会话、统计、事件 |
| SS UDP 超时/容量、重放策略 | 协议 `state_limits`、`replay_attack` 加通用 socket 生命周期 |
| 日志、进程服务化、升级 | Zero logging、宿主服务管理与外部部署工具 |
| 订阅下载/转换和其他本地代理入口 | 外部配置生成器及 Zero 已有 SOCKS5/HTTP/TUN 入口 |

此映射不承诺复刻官方独立工具的参数名、端口暴露方式、平台 socket 调优开关、
manager 报文或订阅 API。它们使用 Zero 原生接口与平台策略，协议线路对齐单独核验。
生产资格验证是最后的运行步骤，不计入协议实现完成度。
