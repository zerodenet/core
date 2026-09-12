# Shadowsocks 状态与回收

协议状态归 `protocols/shadowsocks/src/udp/`。runtime 仅持有不透明关联 ID，
不解析密钥、wire session ID 或重放窗口。

## UDP 关联

历史 SS 关联由凭据和客户端 endpoint 隔离；2022 由凭据和 wire session ID
隔离。协议生成不碰撞的本地关联 ID。不同用户即使使用相同 wire ID 也不共享
出站 socket、重放窗口或响应密钥。同一关联的多个目标共享可用出站 socket，
已认证的新 endpoint 会迁移该用户整个关联的响应地址，包括其他目标的待回包。
撤销一位用户只关闭其流，共享 UDP listener 继续服务其他用户。

默认 UDP 关联空闲期为 300 秒，无隐藏容量上限，与固定官方默认值一致。
`state_limits.udp_capacity` 是可选原生准入上限；不按目标数限制单个关联。
开启上限时不驱逐仍有效的重放窗口来接纳新会话。UDP 重放保留期至少为
`max(udp_timeout_secs, 61)`，覆盖未来时间戳的完整有效期。
有效请求与响应更新活动；重放或无效报文不能延长状态寿命。
持久 30 秒维护定时器在没有新流量时仍清理过期状态；取消 receive 不重置定时器。

每个 outbound codec clone 家族共享一个随机客户端 session ID 和递增 packet ID。
新 socket/packet-path 关联使用新 codec。客户端校验服务端方向、回显客户端 ID、
服务端 session 和 8128 范围的重放窗口；接收 ID 0，拒绝终止 ID `u64::MAX`。
终止包不能占用容量。发送计数耗尽失败，不回绕，不自动重发业务数据。
响应发送端为每个客户端会话保留独立稳定 server ID 和计数器。
空 payload 是有效 UDP 数据报；2022 空包包含 0..899 字节随机 padding。

`transport/udp_socket` 的接收任务随 flow drop 取消，并只接受配置的上游 peer。
65535 字节缓冲保留完整 wire 数据报；实际单包大小仍受平台 UDP 限制。
Zero 的 `runtime.udp_upstream_idle_timeout_seconds` 另行管理通用上游 socket 寿命。

## TCP 重放

2022 精确 salt 池保留 61 秒，默认无隐藏容量上限；可通过
`state_limits.tcp_replay_capacity` 开启准入限制。FIFO 到期队列和 30 秒维护任务
回收过期条目，池销毁会取消维护任务。上下行均检查已认证 salt，响应还绑定
当前请求 salt。显式 `ignore` 也不能关闭 2022 重放拒绝。

v1 的 `default`/`ignore`/`detect`/`reject` 与参考实现一致；接收默认放行，
检测模式记录重复，拒绝模式返回错误。生成的 TCP IV/salt 除 ignore 外也注册，
阻止本端生成 nonce 的反射重放。实现使用参考参数的两个轮换 Bloom filter。

测试覆盖跨用户同 ID、迁移、多目标 socket、用户撤销、2050 包连续会话、乱序、
方向/回显、计数边界、空包、容量与空闲回收，以及完整固定版本互通。
生产持续运行验证是单列的最终步骤，不是未实现能力。
