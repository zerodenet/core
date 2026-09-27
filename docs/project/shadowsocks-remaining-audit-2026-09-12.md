# SS 剩余实现复核：不能收口为三类扩展

> 后续开发状态以 [固定官方版本实现矩阵](../protocols/shadowsocks/parity.md) 为准；下文保留本次后续开发之前的审计或阶段记录。

本轮针对“除了三类能力是否已经全部”继续检查当前未提交工作区。
对标仍为 shadowsocks-rust v1.21.2 / `a03006a753486e64717d6e3afa91e0c6d043c557`。
此前完成的五项状态修复不撤销，但“其余只剩三类扩展”的范围判断不成立。
本次没有修改运行时代码、提交、推送或部署。

## 新确认的四项实现欠账

| 项目 | 代码与定向证据 | 影响 |
| --- | --- | --- |
| 旧 AEAD UDP 客户端/用户隔离不完整 | datagram read 向 dispatch 传入 source=None；flow key 只有 target、port、client_session_id，而旧 AEAD 的 ID 为 None；两客户端先后请求同一目标，延迟返回 A 的响应，实际被发给 B；不同密码用户也复现 | 回包串流、跨用户数据隔离失效；复用分支也不重新建立用户对应的会话归属 |
| SS2022 TCP 可变长度头不能分段续读 | inbound.rs:718、865 对 variable header 单次 read；相同合法请求完整读取接受，第二次读取只返回 1 字节则报 variable header too short | 合法 TCP 分段可能被错误拒绝；salt+固定头的单次读取规则不能直接扩大到可变头 |
| 用户撤销误停共享 SS UDP relay | dispatch.rs 的 principal cancellation 标记 close_association；datagram loop 收到后 break；两个 SIP023 用户预先都能往返，撤销 A 后 B 的请求不再到达目标 | 单用户生命周期影响整个共享 UDP 入口，不是该用户一条流 |
| 失败握手 drain 没有声明的总时限 | inbound.rs:958 每次 read 新建 2 秒 timeout；受控时钟下每次读取耗时 1 秒的无效握手总计 7 秒才结束 | 实际只有单次读取空闲超时，不能把注释中的 2 秒当作总资源占用上限；仍存在 1 MiB 字节上限 |

官方旧 UDP association 按 peer 地址划分，2022 按 client session 划分：
[udprelay.rs](https://github.com/shadowsocks/shadowsocks-rust/blob/a03006a753486e64717d6e3afa91e0c6d043c557/crates/shadowsocks-service/src/server/udprelay.rs#L321)。
官方可变头进入数据块读取，使用可续读的 poll_read_exact：
[aead_2022.rs](https://github.com/shadowsocks/shadowsocks-rust/blob/a03006a753486e64717d6e3afa91e0c6d043c557/crates/shadowsocks/src/relay/tcprelay/aead_2022.rs#L424)。
本地官方源码缓存的这两个文件已重新下载固定 commit 文件并逐字节比对一致。

## 范围与证据边界

- 之前三类差异（旧 AEAD Detect/Reject 策略、SIP003/SIP003u 插件、额外 cipher）仍成立。
- 四个探针均成功复现当前缺陷；这里“测试通过”表示缺陷被复现，不表示实现已修复。
- TCP 分段探针走真实协议 acceptor 和合法加密报文，用自定义 AsyncSocket 控制读取分段；drain 使用 Tokio 受控时钟，不称为生产测量。
- 两个 UDP 探针走真实 Proxy、监听 socket 和本地 UDP 目标。取消探针通过本地 close_principal_flows 触发，不是部署或远程管理操作。
- 本次只执行定向探针，没有重复运行完整工作区测试。临时 tests 文件已移除，原样归档在本报告同名目录。
- 新清单同样不是“所有缺陷已经穷尽”的证明。多用户相同 2022 session ID 的命名空间、同会话跨目标/NAT 迁移语义、更多取消/重载边界仍需单独核对，不能自动写成已经缺失或已经完成。
- 官方空 UDP payload 的 padding 策略与 Zero 固定零 padding 存在源码行为差异；尚不把这项等同于必需协议机制缺失。
- 生产验证只作为最后步骤，不计入实现完成度。

## 复现

将 [协议探针](shadowsocks-remaining-audit-2026-09-12/protocol-probe.rs) 复制到
`protocols/shadowsocks/tests/audit_remaining_20260912.rs`，将
[代理探针](shadowsocks-remaining-audit-2026-09-12/proxy-probe.rs) 复制到
`crates/proxy/tests/audit_ss_remaining_20260912.rs`，在仓库根执行：

```sh
RUST_MIN_STACK=16777216 cargo test -p shadowsocks --all-features --test audit_remaining_20260912 -- --nocapture
RUST_MIN_STACK=16777216 cargo test -p zero-proxy --all-features --test audit_ss_remaining_20260912 -- --nocapture
```

原始结果：[协议日志](shadowsocks-remaining-audit-2026-09-12/protocol.log)、
[代理日志](shadowsocks-remaining-audit-2026-09-12/proxy.log)。各 2/2，均为缺陷复现断言。
