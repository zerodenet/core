# QUIC 接收窗口与自动增长

Zero 继续使用统一的窗口字段。旧配置含义不变：`stream_receive_window` 和
`connection_receive_window` 单独使用时是固定窗口。可选的 `max_stream_receive_window`、
`max_connection_receive_window` 声明增长上限；对应旧字段同时作为初始值。
最大值省略、null 或等于初始值时，该窗口不增长。默认仍为固定流窗口 8 MiB、连接窗口 20 MiB。

```json
"quic": {
  "stream_receive_window": 32768,
  "max_stream_receive_window": 8388608,
  "connection_receive_window": 65536,
  "max_connection_receive_window": 20971520
}
```

该片段放在 HY2 的 `transport` 内。所有值均为字节；初始值至少 16,384，最大值不得小于初始值，
且不得超过 2^60。入站和出站均控制本端接收方向。参数变化沿用已有配置重载与连接缓存退役机制，
不会原地缩小已经通知对端的额度。

## 字段与责任

| Zero 字段 | 官方协议实现的配置映射 | 作用 |
| --- | --- | --- |
| stream_receive_window | InitialStreamReceiveWindow | 每条流初始窗口，未提供上限时固定 |
| max_stream_receive_window | MaxStreamReceiveWindow | 每条流增长上限 |
| connection_receive_window | InitialConnectionReceiveWindow | 整条 QUIC 连接初始窗口，未提供上限时固定 |
| max_connection_receive_window | MaxConnectionReceiveWindow | 所有流共享的连接窗口增长上限 |

`send_window` 是发送缓冲约束，`bbr_initial_window` 是初始拥塞窗口，
`up_bps/down_bps` 是连接带宽声明。这些字段不作为接收窗口的别名，也不自动推导接收窗口大小。
QUIC DATAGRAM 不使用流级 MAX_STREAM_DATA/MAX_DATA，沿用已有 datagram 缓冲限制。

配置层负责 ADT 和边界验证，HY2 负责参数映射；`zero-transport::quic::receive_window`
执行共用的增长策略，通用 engine/proxy 不参与窗口计算。
Quinn 的扩展只有中立策略接口和事件桥接：收到数据、消费/归还额度、生成窗口更新、关闭流。
偏移合法性、重传和流生命周期继续由 Quinn 管理。未安装策略时完全保留原固定窗口路径。

## 对照规则

参考 Hysteria `app/v2.12.2` 固定依赖 quic-go
`v0.61.1-0.20260806010916-184d081eef3e` 的
[接收流控实现](https://github.com/apernet/quic-go/blob/184d081eef3e/flow_controller_base.go)。

- 消耗约四分之一窗口后可以发送新额度；增长判断要求当前周期消耗严格超过半个窗口。
- 有实际 RTT 测量，且消耗用时小于 `4 × 消耗比例 × 平滑 RTT` 时，窗口翻倍，上限截断。
- 流窗口增长时，连接窗口尝试达到该流窗口的 1.5 倍，仍不得突破连接上限。
- 慢速读取或没有 RTT 样本不会触发增长；每条流独立采样，连接负责总额度。
- 初始周期从首次数据到达开始；窗口不会自动缩小，流关闭时删除其采样状态。

Quinn 与 quic-go 的帧排列、调度和读取批次仍有差异，不能声称逐包发送时间相同。
这里对齐增长策略及可观察窗口行为，不引入官方应用的第二套配置树。

## 验收条件

1. 同一份输入由固定 quic-go 源码生成结果，Rust 逐项核对窗口大小、额度、增长周期与更新结果。
2. 载体回归证明额度实际发到对端，流关闭释放状态；专项验证慢读、上限和多流协调。
3. 在每方向 100 ms 延迟、上行 262,144 字节/秒的受控链路上传 2 MiB。
   固定窗口为流 16 KiB / 连接 32 KiB；自动上限为流 128 KiB / 连接 256 KiB。
   两种接收器的自动窗口吞吐都应超过固定窗口的 1.5 倍，两者自动窗口吞吐比在 0.7–1.3 内。
4. 工作区全量、严格 Clippy、validation-only、原有 BBR/Brutal 与官方/sing-box 互通通过。

状态输入、Go 生成器和参考输出位于
`crates/transport/src/quic/tests/receive_window_reference/`，CI 固定模块版本后重新生成并比较结果。
本批不包含 TCP/UDP 共用认证连接、任意网络性能等价或长稳验收。

## 本地对照结果

2026-09-09，macOS 使用上述固定标签源码构建的官方程序，运行同一高时延测试：

| 接收端 | 固定窗口耗时 | 自动窗口耗时 | 全程吞吐提升 |
| --- | --- | --- | --- |
| Zero | 29.536 s | 9.010 s | 3.28 倍 |
| 官方 | 32.280 s | 9.045 s | 3.57 倍 |

两个自动窗口场景的全程吞吐比为 1.004。测试未设置周期性丢包，但有界链路队列会丢弃溢出包，
因此这些结果不是无损网络的理论上限。147 个官方状态检查点与 268 项 Quinn 回归均通过，
包含 RESET_STREAM 归还未读额度一次、空重置不启动采样以及关闭后清理流状态。
本地结果与 CI 的发布资产验证分别记录，不能用本地源码构建替代发布资产互通验收。
