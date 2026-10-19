# Browser Dialer 与 XHTTP 的 CI 根因及修复

本记录不以重跑覆盖历史失败。修复提交为 `a5d24eb7`，VLESS 测试同步提交为 `503af4c0`；集成源码提交为 `6f1ae3b09c30a253de31d2edfb4cf80cb57eb850`，tree `4c1005f50fdcb7c6e1ce1cae4d2a32722b36b989`。后续冷路径修正后的冻结源码为 `8785216503a7744c7b4340e9f4c3b3c05b02193f`，tree `e282d3172361fe4c66163b29aec09511597e03d3`；[精确验证](pr04-validation.json)和[原始证据摘要](pr04-ci-root-causes.json)分别记录资格及来源。

## Browser Dialer 永久退出与 Interrupted

历史 `6cd36858` 的 Linux CI 挂在 Browser Dialer 生命周期测试，45 分钟 job 上限处取消。`c9c06b2a` 增加直接 stderr 阶段记录后，同一问题停在 `read retired stream`；job `118232208932` 最终取消，汇总 CI 失败，后续最小 feature 检查及 optional surface 未完整执行。

本地旧锁使用 Tokio 1.53.3，而该 CI 解析到 2026-10-19 发布的 Tokio 1.54.0。隔离复现从准确的 c9c 源码开始，锁文件仅将 Tokio 1.53.3 换为 1.54.0；未改产品 manifest、协议 reference 或生产代码。Tokio 1.54 的整数读取辅助在同一次 poll 中重试 `ErrorKind::Interrupted`。BrowserStream 在永久退休后每次 poll 都返回 Interrupted，导致 `read_u8` 忙循环，同线程的原五秒超时也无法推进。

准确的原生命周期测试在未修复二进制中持续占用 CPU：11 秒时约 1099 个用户态 CPU tick；外部父进程在 12 秒终止它，原五秒计时器没有生效。原始 JSON 记录二进制 SHA、锁摘要、CPU 采样和强制终止结果。

生产修复将永久退休后的错误改为 `ConnectionAborted`，保留 `Browser Dialer retired` 消息。原测试仍检查 listener 复用、不触发 native dial、原五秒截止时间、旧流退出、Drop 与 peer join；仅将特定的预期错误种类同步为永久关闭。没有删除断言或提高超时。

新增 transport 回归先直接 poll 两次，再检查 read_u8、write_all、flush、shutdown，以及对端关闭和服务器 join。旧实现会在原始 poll 错误断言处明确失败，避免让回归测试本身陷入辅助函数的无限重试。

同一隔离 Tokio 1.54 环境中，修复后的原生命周期测试约 0.032 秒通过。随后整套 VLESS 库的前 14 轮中，Browser 测试全部通过，但第 14 轮另一个 CCS 测试失败；该失败及输出完整保留，不能称为 20 轮全通过。

### CCS 测试同步问题

CCS 原测试忽略三秒 timeout 的返回值；生产 probe 最多有三次一秒响应窗口，另加 TLS 握手，测试可能在尚未写出原始 Finished flight 时就检查部分捕获。测试对端还可能提前关闭。修复仅修改测试：实际 Rustls TLS 1.3 对端在收到两条 CCS 后明确发送拒绝 alert，并保留真实 Duplex 直到读到原始 Finished flight。原三秒截止时间、两条 CCS 和后续记录类型断言保留，新增 probe 完成、limit=1 及对端 join 断言。没有修改生产 REALITY/CCS 探测或加密行为。

同步后，隔离的 c9c + Browser 修复 + CCS fixture 在实际 1 CPU / 1 GiB / 无 swap / 16 测试线程环境，整套 50 个 VLESS 库测试连续 20 轮全部通过。该环境用独立网络命名空间、普通用户和默认非特权 Docker，不改变主机安全配置。这一结果是专门复现资格，不能代替完整新 head 的 workspace 与远端 CI。

## XHTTP 已接收上传被许可饥饿拒绝

`c9c06b2a` 的 VLESS reference job `118230429004` 在 `xhttp_extended_metadata_payload_padding_interoperate_both_directions` 失败：official Xray 向 Zero 发送 packet-up header 数据后，出现 30 秒 admission timeout / early EOF；后续 reference 矩阵跳过。

使用既有准备脚本中的固定 Xray 26.3.27 / d2758a0 及校验过的归档。抓取真实 HTTP/1 字节显示，每个逻辑会话的序号 0..65 已全部到达。在失败 case 30039 中，session `93c2e163...` 的 42/59/65 与 `0913fb07...` 的 58/59/60/65 未 dispatch；对应响应全部是 HTTP 503。其他上传响应为 200。原始上/下行、分析脚本及序号清单保留。

根因为 Received 回执已允许同连接继续流水线，但尚待 admission 的任务仍持有全局 64 个请求许可。Hyper 在这些任务获得执行机会前继续解析流水线，`try_acquire` 因许可耗尽而拒绝完整上传。reference 不重传该 503 上传，因而产生无法填补的序号缺口和后续 30 秒超时。

修复在收到并完成有界任务所有权转移后让出调度，允许 admission/writer 前进，再解析下个流水线请求。64 请求许可、payload 字节预算、队列、重复拒绝、取消和原截止时间保持。回执仍只表示已接收和保留所有权，不代表业务投递。

### 对照证据

- 新回归使用真实 Hyper HTTP/1 parser 和 in-memory duplex，复刻四会话、每会话两条 HTTP 连接、66 个 header 上传的连接分配，执行八轮。没有依赖外部 reference 进程。
- 对同一源码仅去掉 yield，回归约 0.12 秒立即以 HTTP 503 失败；恢复完全相同 packet.rs 后约 0.73 秒通过。`pr04-xhttp-ablation-2.json` 记录源码 SHA、负/正对照及精确恢复。最初 split_http-only 命令在既有 QUIC 测试 cfg 处编译失败，不能算作负对照；有效对照显式启用 split_http,quic。
- 旧二进制在 20 个独立命名空间、1 CPU / 1 GiB 的整套六项 options 试验中，13 次 admission/gap 失败、1 次 echo 端口 fixture 失败、6 次通过。
- yield 二进制同条件 20 次中，19 次通过、1 次 echo 端口 fixture 失败，零 admission/gap 失败。没有把端口 fixture 与生产根因混记。
- echo fixture 的 free-port 再异步 bind 改为转移已经 bind 的 listener，保留原 payload/echo 断言；修复后整套六项 options 主机运行通过。新 head 的完整 reference 矩阵须独立记录。

最初两个进程共享同一网络命名空间的重复 harness 会碰撞进程内固定端口范围，因此被排除；有效试验每次具有独立网络命名空间。

## 资格边界

原始日志和 SHA 清单保留在相邻 evidence 及根因 manifest 中。旧 CI/TUN 的结论只适用于 c9c。TUN Windows job 成功，但出口切换报告为 `executed=false`，缺少合适的第二个已连接接口；三平台独立 smoke 及 macOS offline 场景还有跳过。Hosted VM 成功不等于物理设备或真实出口切换验收。

f90 的 Hysteria 原十秒认证失败根因仍未确认。该历史问题未被本次 Browser/XHTTP 定位替代；后续完整 head 的结果按实际记录。

## 后续重复中新发现的参考客户端计数缺陷

6f 冻结源码使用 Tokio 1.54 和已绑定 echo listener，20 轮受限 options 中仍有 1 次超时；另做 40 轮带 trace/字节抓取的诊断，39 次通过、1 次超时。这两次失败完整保留，不能将前述 503 修复写成整个 reference 矩阵已全部通过。

第二次失败的抓取 case 为 `pr04-head6f1-trace-tap-19/30072`。四条 TCP 上传均为完整 0..65 序号，全部 dispatch，响应全部 HTTP 200，四条 TCP 的 131073 字节 echo 都完成。UDP 的两个独立上传序号 0 和 1 各携带完全相同的 1643 字节 VLESS MUX 请求及 1600 字节载荷；两份 SHA-256 都是 `a6c8b96d239be8695fc2a53c636b2407c235a511b85be54b8d162e8ca7944f00`。这是 HTTP 入站之前已经产生的重复 VLESS 请求头，不是 Zero 丢弃同一个上传或重复 dispatch。

解块后的 UDP 下载共有 1636 字节：VLESS 响应后先出现 MUX END sid=0x1111、END sid=0，随后才是带 1600 字节回包的 KEEP sid=0。重复的 VLESS 请求头进入已建好的 MUX 字节流，被当作 MUX 帧处理；客户端先收到逻辑关闭，后面的正常回包不再交付。服务端实际 UDP 计数为收发各 1600 字节，最终在原 30 秒 idle 截止时间结束。这与前述 HTTP 503 / 序号缺口是不同故障。

固定参考 `26.3.27` 对应不可变提交 `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`，Go module 同一提交为 `github.com/xtls/xray-core@v1.260327.0`。其 [`uploadWriter.Write`](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/transport/internet/splithttp/dialer.go#L597) 将 buffer 转移到 pipe 后才读取长度；而 header/cookie 上传的 [`FillPacketRequest`](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/transport/internet/splithttp/config.go#L325) 在另一个 goroutine 中复制并释放该 buffer。释放会将长度归零。如果消费方先释放，实际提交 1643 字节的 Write 就可能返回 `(0, nil)`；真实 [`WriteAllBytes`](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/common/buf/io.go#L87) 因没有推进计数而重新提交同一请求。

单独的 Go 取证测试直接使用同一提交、校验过的真实 `common/buf`、`transport/pipe` 和 `WriteAllBytes`，通过 channel 固定合法的转移/释放顺序，没有靠竞速重复直到失败。原计数顺序稳定产生“已交付 1643 字节但报告 0”，实际 WriteAllBytes 产生两次相同提交；仅在所有权转移前保存长度后，两项检查通过且只提交一次。原/正对照日志、测试源码、go.mod/go.sum、模块来源和源文件 blob 摘要单独保留。

这证明参考版本存在与抓取吻合的计数/重放路径；它是确定性的所有权反例和实际 wire 证据，尚未用修改后的完整参考二进制做端到端消融。因此不得将模型测试当成官方二进制修复或新的完整 head 资格。固定官方参考的两组失败仍是未通过的资格记录。内核不应任意去重重复业务字节，也不因此放松认证、MUX 帧检查、载荷断言或截止时间。任何补丁参考或额外版本只能作为清楚标记的补充证据，不能默默替换规定基线。

## 实施期间捕获的架构和初始化问题

6f 的首次完整 workspace 为 2696 通过、3 失败、159 忽略。失败都在架构守卫：ClaimedRelayChain 拆到所属子模块后的读文件路径、通用测试根中的具体协议 feature gate、旧 capability 与新 factory 的重复 feature 清单。已保留全部失败输出；末尾 CI annotation 会重复失败摘要，统计时排除 TEST_END 后的 annotation，不能重复累加为 2882/6。

守卫仍检查实际模块委托、具体协议 feature 只在 owning adapter/registration、统一 feature 清单；没有用白名单或跳过取消这些约束。收敛两种登记时曾引入 ProtocolRegistry/ComponentCatalog 的 Default 递归，六项真实业务测试在推送前捕获 stack overflow。`87852165` 将冷路径工厂表保留为状态化 OnceLock，实际 prepare 时初始化，六项业务测试随后通过。当前完整冻结验证单独记录，不将这次修正之前的 Clippy 或测试结果归到后来的 head。
