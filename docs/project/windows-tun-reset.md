# Windows TUN 连接重置跟踪

状态：未结。2026-09-09 带取证的重跑通过，历史失败尚无根因结论；没有修改内核转发行为。

## 两次失败分别记录

代码提交 `7de4579d` 的 [Windows 特权验收](https://github.com/zerodenet/core/actions/runs/34306685919)：

| 运行 | 用例和观察 | 尚缺证据 |
| --- | --- | --- |
| attempt 1 | `privileged_windows_ipv4_only_tun_falls_back_trusted_ipv6_domains` 在读取 TLS 响应时遇到 10054；直连目标为测试机自身物理 IPv4 的 8443 端口，Zero 日志上行 245 字节、下行 0 字节，并出现上游读取 10053 | 失败时服务端实际收到的字节数、回包及 TCP 关闭顺序 |
| attempt 2 | `privileged_tun_ipv4_smoke_tcp_dns_and_crash_recovery` 读取 `104.20.23.154:80` HTTP 响应时遇到 10054，未执行到首次失败用例 | 失败流的物理连接与 TUN 连接抓包，确认首个 RST 来源 |

两条路径均为 direct，未经过 HY2。相同错误码不足以证明根因相同，也不能将 Windows 的错误描述
直接当作真实源站主动断开的证据：应用与 Zero 用户态 TCP 栈、Zero 与源站是两条 TCP 连接。

## 已完成的取证

`68bd996e` 增加测试端口、实际读取长度日志和 Windows PktMon 抓包。
后续 `c1b095ae` / `238dbd3b` 保持 CI 的严格失败传播：不使用 `continue-on-error`；
只在失败后继续独立 TLS 取证步骤和抓包收集，不把失败改成成功。

`238dbd3b` 的[特权验收](https://github.com/zerodenet/core/actions/runs/34308359851)
在 Windows、Linux、macOS 实际运行并通过；[工作区 CI](https://github.com/zerodenet/core/actions/runs/34308359835)
的全量测试和静态检查通过。与文档提交 `42bba3d3` 的工作流绿色不同，后者实际平台用例被跳过。

Windows job `102329691635` 留存 artifact `10087557291`，名称 `windows-tun-packets-1`，
包含 `tun.etl`、`tun.pcapng`、UTF-16 的 `tun.txt`，保留七天。
抓包仅选择 TCP 80/8443 端口，每包截取 160 字节；后续若测试回退到 443 端口，需要补对应取证范围。
PktMon 同一包可出现在多个组件，分析时须按五元组、序号和时间去重，不能把组件副本当成重传。

本次已核对：

- HTTP：`10.66.0.1:53235` 经物理连接 `10.1.0.101:53236` 访问 `104.20.23.154:80`。
  Zero 向 TUN 客户端送入 869 字节响应后，测试只读取 32 字节便关闭；随后 RST 来自
  `10.66.0.1:53235`，ACK 已覆盖全部 869 字节。这解释了成功用例中的 `connection reset by local client`
  日志，不能用来解释历史失败中尚未成功读取响应的 10054。
- TLS：应用生成 245 字节 ClientHello；受控服务端 `10.1.0.101:8443` 从对端端口 54048
  一次读取完整 245 字节，记录的 TLS record 总长度也是 245，写出 7 字节 TLS alert，用例通过。
  “服务端单次 read 没读完整就关闭”是待验证假设，本次没有复现该条件。
  本次 PktMon 文本对该同机物理 IPv4 TLS 连接仅包含 ACK，以上读取/回包长度取自应用日志；
  不能声称已取得完整 TLS 路径抓包，下一次失败需视缺口补 TCPIP 事件或端点取证。

## 后续闭环条件

保持问题未结，跟踪新的实际 Windows 运行及其 artifact，跳过文档触发的空跑结果。
有新的失败时，先按测试客户端端口找到 TUN 流，再关联物理出站连接，核对响应、ACK、FIN、RST 的方向和顺序。
若服务端请求读取不完整，补确定性的分段用例验证；若源站先重置，区分公网端点可用性与内核传播；
若 Zero 在完整响应送达前产生错误关闭，再为共享 TCP 生命周期补回归并修复。

只有根因得到证据支持、相应修复通过回归和实际 Windows 验收后才关闭问题。
一次或多次重跑成功、减少断言、忽略重置、不断重跑直至绿色都不作为关闭依据。
