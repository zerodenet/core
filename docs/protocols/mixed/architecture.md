# Mixed 实现边界

`mixed` 通过 `MixedAdapter` 注册为入站能力，通用运行时不对 Mixed 配置做协议特判。

| 责任 | 所有者 |
| --- | --- |
| 绑定和准备 Mixed 入站 | `MixedAdapter` |
| SOCKS5 / HTTP 首字节识别 | Mixed 入站处理器 |
| 协议握手、HTTP 消息解析与规范化 | SOCKS5 或 HTTP 协议模块及适配器 |
| CONNECT 隧道路由生命周期 | 通用 TCP ingress runtime |
| 普通 HTTP 每请求路由生命周期 | 通用 message-ingress capability |
| 监听循环、关闭和连接任务 | 通用入站运行时 |
| 路由、出站、流量和 Session 记录 | 通用代理运行时 |

这种边界使 HTTP 与 Mixed 的 HTTP 分支共用同一协议实现，同时保证内核只依赖中性的消息型入站契约，不按具体协议名称分支。
