# Windows TUN 连接重置跟踪

状态：部分定位。2026-09-09 新的 Windows 失败已确认 TLS 测试服务端的非阻塞读取错误；
TLS 测试修复已通过 Windows 特权用例。历史 HTTP 重置仍未定位，受控对照继续验证；没有修改内核转发行为。

## TLS 测试服务端缺陷已复现

提交 `6d8e32b5` 的[运行 34310417863](https://github.com/zerodenet/core/actions/runs/34310417863/job/102335780811)
再次在 TLS 回退用例失败，HTTP 冒烟通过。新的日志和 artifact `10088237749` 给出直接证据：

- TUN 客户端 `[fd66::1]:52974` 发送 245 字节 ClientHello；物理上游为 `10.1.0.23:8443`，
  Zero 的物理源端口为 `52975`。
- 测试服务端记录 `first_read=Err(...10035, WouldBlock...)`，随后仍打印
  `wrote 7-byte TLS alert; closing`。读取错误被忽略，没有等到请求便回包关闭。
- PktMon 文本记录 `10.1.0.23.8443 > 10.1.0.23.52975: Flags [R.]`，
  `seq 1132345542, ack 2248355800`，方向为测试服务端到 Zero 的物理连接。
  Zero 随后记录上游读取 10053、上行 245 / 下行 0，TUN 客户端读取报 10054。
  同机流仍没有完整握手抓包；以上结论结合端点日志和这条 RST，不推断缺失的数据包。

`MockTcpResponder` 的监听器使用非阻塞模式，但接受的流没有恢复阻塞模式。
[Winsock accept 文档](https://learn.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-accept)
明确接受的套接字继承监听器属性。`set_read_timeout` 不改变非阻塞模式，
所以请求与 accept 的调度先后决定是正常读取还是立即返回 WouldBlock。
这解释了本次 TLS 失败及此前重跑时好时坏；首次历史 TLS 失败签名一致，
但缺少当时服务端日志，不能追认其 first_read 的具体结果。

修复仅在 `tests/tun_privileged/tls_responder.rs`：接受流恢复阻塞，读写均设超时，
按 record 长度读取完整请求，读取失败不发送 alert，成功后关闭写方向。
跨平台回归显式把接受流置为非阻塞，覆盖延迟请求、分段 header/body、请求中途 EOF；
本地 macOS 上旧逻辑两项失败，修复后 `cargo test --test tun_privileged_e2e tls_responder::`
两项通过。Windows 用例复用同一个 helper；提交 `01e37b8e` 的
[Windows job 102348778324](https://github.com/zerodenet/core/actions/runs/34314825863/job/102348778324)
中 TLS 回退通过，服务端记录 `complete_record_response=Ok(245)`。
HTTP 测试仍保持原断言，不将其未定位的 10054 忽略或改成重试。

## HTTP 受控对照

新增 `http_control::windows::privileged_windows_http_direct_and_tun_control`，
固定同机物理 IPv4 的 8080 端口作为 HTTP 服务端；不请求公网 HTTP 站点。
依次执行 TUN 启动前物理地址直连、TUN 转发、TUN 停止后直连。
TUN 运行时另检查物理旁路连接被严格防泄漏策略以 10013 拒绝，不关闭防泄漏来迁就测试。
每组固定 16 轮，每轮四条新连接：

- 一次写出较小响应，完整读取到 EOF 并逐字节校验。
- 请求头与 64 KiB 响应分别分段发送，完整读取到 EOF 并逐字节校验。
- 刻意读取 32 字节就关闭，仅这一条允许服务端出现连接关闭类错误。
- 提前关闭后立即重新连接，完整校验另一条 64 KiB 响应。

每组 64 条连接，共 192 条；144 条完整响应和 48 条刻意提前关闭，不重试失败请求。
请求和响应均包含唯一 case 编号，客户端记录原始源/目标端口，服务端记录实际物理源端口和结果，
并检查接收数量、编号唯一性及所有非提前关闭请求均成功。读取截断、额外数据、内容错误或 RST 都会失败。

本机物理 IP 存在系统本地路由，直接访问它不足以证明进入 TUN。
因此 TUN 组通过受控 DNS 分配 Fake-IP，并断言 Windows 选路命中 TUN；Zero 反查后仍以 direct
连接相同 HTTP 服务端。物理直连组绑定物理源 IP，TUN 组绑定 TUN 源 IP。
该对照覆盖共享 TCP 生命周期及 Fake-IP 目标恢复；不完全等同于历史公网真实 IP 的 HTTP 冒烟路径，
通过也不能单独排除后者的所有缺陷。

本地 macOS 的 loopback 服务端自测完成 64 次连接，另以同一工具通过本机物理 IPv4 地址
直连运行 64 次。每组 48 条完整校验通过，16 条提前关闭后重连通过；
提前关闭在服务端产生 BrokenPipe，完整请求没有失败。项目内两项回归通过，
其中另一项确认截断、内容损坏及多余字节均会被拒绝。这些不是 Windows/TUN 验收。
Windows 特权对照已加入 `Privileged TUN E2E`，捕获范围补充 TCP 8080。
首轮 `01e37b8e` 的 Windows job 在 direct-before 64 条通过后，direct-during 首次连接被 10013 拒绝，
尚未进入 TUN 组。这是对照测试原先要求严格模式允许物理旁路的设计错误，不是 HTTP 读取重置。
现已将该步骤改为严格验证旁路被拒绝，保留前后直连基线及全部 TUN 内容/关闭断言，等待新一轮执行：

```sh
cargo test --test tun_privileged_e2e http_control::windows::privileged_windows_http_direct_and_tun_control -- --ignored --exact --nocapture
```

判定时优先比较同一 case 的客户端、服务端日志及抓包：直连也失败则先查服务端/宿主环境；
只有 TUN 完整读取失败则进一步定位 TUN 路径；仅刻意提前关闭产生 RST 属于该用例的预期现象。
所有组通过只能说明本轮条件未复现，历史 HTTP 故障仍保持未定位。

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
