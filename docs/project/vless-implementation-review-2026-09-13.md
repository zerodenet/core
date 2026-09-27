# VLESS 实际实现核查（2026-09-13）

核查对象是本机 `develop` 工作区，HEAD 为 `f49adb210da0084e31bd8a413374af0ee328060c`。本轮 VLESS、共享传输和 TLS 改动尚未提交，HEAD 不能代表这些改动。此次只分析并生成报告，没有修复实现、提交、推送或部署。

范围覆盖 VLESS 协议、共享载体、配置验证、代理接入、生命周期和现存测试证据。核查采用入口到执行路径的静态检查及重点失败复查，不等于逐行密码学审计，也不构成所有组合正确性的证明。

固定参考为 Xray-core v26.3.27 / `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`。协议包版本为 26.3.27；本轮在线核对了该不可变提交的 [VLESS 配置源码](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/infra/conf/vless.go)及 [TLS 指纹目录](https://github.com/XTLS/Xray-core/blob/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0/transport/internet/tls/tls.go)。其他官方互通结论只适用于对应历史测试日志，不把本机未带 Git 元数据的参考源码目录作为新的完整版本认证。

## 判断

主要能力已经有实际实现，并进入配置、协议准备、代理执行链路；现在不是大量功能只有接口或文档的阶段。但仍有真实兼容范围差异、未同步的能力信息、可复现的共享运行时回归，以及未完成的组合验证。不能宣称完整官方对齐或实现工作全部结束。

生产验收是最后的独立步骤，不计入下述实现缺口。也不以测试程序通过比例换算功能完成百分比。

## 能力盘点

下表“已接入”表示查到配置/准备/执行实现，不表示该行所有参数组合都验证完毕。路径相对于仓库根目录。

| 能力 | 实际落点与当前判断 | 验证范围和未闭合事项 |
| --- | --- | --- |
| TCP 入站/出站、标准 UDP | `protocols/vless/src/{inbound,outbound,udp}.rs`，代理注册真实 TCP/UDP 操作；已接入 | 本批协议及基本代理用例通过；不等于所有载体组合通过 |
| 身份、短 ID、配置验证 | `uuid.rs`、`validation.rs`、`reality_policy.rs`；字符串身份转换、协议私有校验及配置委托已有实现 | 协议身份测试 3 通过；本批尚未执行 5 个 VLESS 专属配置测试程序 |
| MUX / XUDP | `mux.rs`、`mux_pool.rs`、`udp/`；多路流和 UDP 会话进入中立运行时 | 协议 MUX、MUX crypto 与相关 UDP 测试通过；保留组合验证边界 |
| Vision / udp443 / testseed | `vision.rs`、`vision/tls.rs`、入站与 UDP 握手；已接入 | Vision 12 通过，代理 Vision UDP 1 通过；TLS Vision 专项仍有 ignored 用例 |
| testpre 预连接 | `transport/runtime/preconnect.rs` 与 `transport/leaf.rs`；两分钟有效期、消费后 200ms 补充、retire 已实现 | 不是配置空字段；其效果限于适用的直接载体，不能外推为所有 relay 路径 |
| VLESS Encryption | `encryption/{config,client,server}.rs`，入站 decrypt 与出站 leaf 均接入 | 有 1-RTT、带票据恢复、三种 mask、混合密钥和 padding；协议 3 通过，代理本批 1 通过/2 ignored。全组合硬化不能从这几个测试推导 |
| Reverse / Rvs | `reverse/` 与 `adapters/vless/reverse.rs`，虚拟出站提供 TCP 和 UDP portal | 心跳、DRAIN、worker、路由与重连已有实现；本批原生测试通过，官方路径存在环境跳过 |
| Reverse sniffing | 配置与中立路由/sniff 服务，协议 portal 只保留协议责任 | 路由、metadata/route-only 相关实现已接入；历史专项结果不能替代当前全部 lifecycle 验证 |
| Fallback | `fallback.rs`、`inbound/fallback.rs`、传输 replay；规则与 TCP/Unix/PROXY 交接已接入 | 协议 fallback/replay 通过；代理规则本批 1 通过/1 ignored |
| REALITY 鉴权与握手 | `reality/`：版本/SNI/时间策略、ML-DSA、混合 KEX、握手片段、Finished 校验与转发均有实现 | 本批 `vless_reality_extensions` 3 通过，日志包含实际固定官方服务端及 22 种指纹路径 |
| REALITY 目标探测与外观 | `reality/target.rs`、`target/{probe,shape}.rs`，接入 target accept 和 post-handshake 加密 | 旧文档称“未实现”已不准确。探测有 256 key、8 并发、8 秒超时等显式边界，不应说与官方所有生命周期行为相同 |
| REALITY Spider / KeyUpdate / 压缩证书 | `reality_spider.rs`、REALITY stream、`ztls/post_handshake.rs` 等 | 普通 PKI 分支不会授予代理接入；有有界导航和 KeyUpdate；票据校验后丢弃与完整 PSK 恢复不是一回事 |
| ClientHello 指纹 | `ztls/src/fingerprint.rs`、`fingerprint/`；22 个版本模板及随机模式已有实际生成 | 没有覆盖官方全部旧版、Android/360 7.5、PSK、旧 Kyber 目录；普通 TLS/QUIC 的 provider 调整也不等于使用该模板生成完整浏览器报文 |
| 普通 TLS / 旧 TLS | `transport/src/tls.rs`、`tls/{config,server,openssl}/`；Rustls/OpenSSL 选择、证书、会话、版本和套件已有实现 | TLS 1.0/1.1 与修正后的固定官方专项日志通过；自定义 ClientHello 路径仍是 TLS 1.3 范围 |
| CA 动态签发 / 更新 / OCSP | `tls/{authority,certificates,refresh,ocsp}.rs` 与 OpenSSL context | 实际签发、缓存、last-good、刷新和 staple 路径已实现；旧失败快照不能覆盖为成功，修正 OCSP 仅证明对应问题 |
| ECH / DNS / H3 | `tls/ech.rs`、配置委托、DNS 准备与 QUIC TLS；不是只生成 GREASE | 本批存在环境跳过；单独固定官方日志 2 通过，覆盖指定 ECH/legacy 矩阵。显式 OpenSSL 客户端与 ECH/指纹的限制仍需保留 |
| WS / HTTPUpgrade / Early Data | 共享 WS、HTTPUpgrade 与 VLESS carrier；前导 PROXY、心跳、header/payload 保存已实现 | 共享专项通过；代理 Early Data 本批 3 ignored，不能报成本批官方通过 |
| gRPC / H2 | gRPC 入站复用、Tun/TunMulti、metadata、流控、pool 与 relay connector 已接入 | 本批 gRPC options 3 通过，但 2 处环境跳过；H2/XHTTP relay 原生通过不等于全部官方组合通过 |
| XHTTP 模式 / H3 / download / XMUX | `transport/src/split_http/`、VLESS options/plan/leaf；均有真实执行路径 | 本批 modes/options/H3 的一部分代理程序全部 ignored；原生及历史专项应分开记录。乱序队列已实现容量等待和 30 秒缺包期限，属于明确策略差异 |
| Browser Dialer | 共享页面、控制/数据通道与 VLESS WS/XHTTP carrier 已接入 | 本批共享控制通道与 XHTTP 用例通过；真实浏览器测试 ignored。配置明确限制 packet-up、独立下载和自定义 TLS/Header 组合 |
| QUIC / mKCP / Hysteria 载体 | 协议载体选择、共享 packet I/O 和 TLS profiles 已接入 | 原生相关矩阵通过，部分官方路径跳过；Hysteria HTTP 伪装的所有平台行为不能由 macOS 专项推导 |
| FinalMask | 共享 TCP/UDP stage，Noise/Sudoku/XDNS/XICMP 源码与 VLESS options 已接入 | 本批原生有通过与官方跳过；XICMP 原始套接字实际执行尚无本轮证据，这属于验证缺口，不是“没代码” |
| 多跳 UDP / packet-path | `adapters/vless/packet_path.rs` → `vless/udp/packet_path.rs` → 中立 tuple-flow carrier | 支持直接及已准备的严格更短 relay prefix；associated/datagram 原生用例通过。旧 capability 测试未同步，详见下文 |
| 生命周期 / reload / 配额取消 | `VlessTransportRuntime::on_config_reloaded` 清理池；XHTTP 弱引用+generation；入站仍由 runtime 执行 | 不是所有池都只建不收；基本代理用户更新、取消等测试在本批通过。完整源码级无泄漏证明不在此次检查范围 |

## 已确认的收尾问题

### 1. VLESS packet-path 测试预期已过时

`crates/proxy/src/protocol_registry/registry/tests/outbound.rs:117` 只允许 socks5/hysteria2/shadowsocks 返回 packet-path capability。当前 `adapters/vless.rs:418` 明确注册 VLESS packet-path；其 owning plan 和运行时构建也存在，且 associated carrier 测试已通过。

本轮用之前编译产物单独复查，仍得到 `Some(true)` 对 `Some(false)`。这里首先要更新并扩展能力契约测试，不应为让旧断言通过而删除已实现的 VLESS packet-path。对 plan 构建失败、relay 前缀和不支持组合仍应保留负向验证。

### 2. SOCKS5 → Mieru UDP pooling 超时可再次复现

`crates/proxy/tests/socks5_udp/relays_udp_through_socks5_to_mieru_relay_chain.rs:385` 的第一个 UDP 响应超过 3 秒。本轮独立使用现有产物、有效端口 slot=3 运行，同样失败，测试耗时 3.17 秒。

这是共享工作区的实际回归阻塞，尚未证明根因为 VLESS，也不能说只是并行测试偶发干扰。需继续沿双 association、Mieru TCP underlay pooling、发送/响应唤醒定位。本轮首次复查误设 slot=7 被测试工具拒绝，随后纠正；slot 参数错误不计入项目缺陷。

### 3. 完整指纹对齐尚未实现

当前 REALITY/ztls 目录与固定官方的全部指纹目录有明确差集。TLS 1.3 自定义握手未形成 HelloRetryRequest 二次 ClientHello 状态链，NewSessionTicket 校验并丢弃，没有完整 PSK 恢复链。不能用普通 OpenSSL TLS 1.0/1.1 能连通来说明旧浏览器指纹已经补齐。

普通 `connect_tls_upstream_with_profile` 进入 OpenSSL 或 `tokio_rustls::TlsConnector`，fingerprint 参数在 provider 构建处调整套件/组；不能把 REALITY 的 22 模板结果推广成普通 TLS、WS/TLS、QUIC 的完整浏览器报文模拟。

### 4. 能力元数据与进展文档未跟上实现

`protocols/vless/src/metadata.rs` 的 `transports` 仍只列 tcp/tls/reality/ws/grpc/h2/http_upgrade/xhttp，未描述新接入的 mKCP/Hysteria 等载体；它不足以作为完整能力盘点依据。

`parity.md` 仍说测试在运行且仅完成前 41 个程序；`reality.md` 仍将已接入的 target post-handshake probe/shape 列为未实现；`fingerprints.md` 的最后官方互通未执行说明，已落后于这批 REALITY extensions 原始日志。应按具体测试快照统一更新，不能直接整体改成“全部通过”。

## 测试统计与证据边界

停止后的 `results.json` 有 137 条记录：131 个退出码 0、2 个退出码 101、4 个退出码 -15。即正常结束 133 个测试程序，其中 2 个失败；4 个被停止。不能写成“137 个全部完成”。共 258 个测试程序的整批执行没有最终汇总，doctest 阶段没有完成证据。

已经产出结果的测试合计：1249 passed、2 failed、111 ignored。这是部分批次累计，不是全工作区通过结果。

VLESS 协议自身的 23 个测试程序累计 210 passed、0 failed；再加名字以 `zero-proxy::vless` 开头的 23 个代理程序，共 261 passed、0 failed、43 ignored，另有 13 处测试内部环境跳过。`zero_proxy` 中的 VLESS 注册断言不在这个按名称汇总的 46 个程序内，因此“VLESS 分类 0 失败”不能掩盖该失败。

原始结果：`/Volumes/tool/zero-task-tmp/vless-final-gate-20260913/results.json`。该目录还有构建日志、每个程序的日志。构建产物来自之前批次；本次失败复查未重新编译，不能将其称为本轮重新构建验证。工作区没有覆盖全部源码的不可变快照哈希，不能保证每一份历史专项日志对应现在每一字节的代码。

本轮核对的其他原始证据：

- `vless-final-workspace-architecture.log`：26 passed。
- `vless-final-runtime-boundary.log`：188 passed。
- `vless-final-official-ech-legacy-run.log`：2 passed，日志含实际 official 条件。
- 本批 `vless_reality_extensions`：3 passed，含 22 个指纹对固定官方服务端的实际执行记录。
- 五个 `zero-config::vless_*` 程序未出现在本批完成结果中，不可视为已经在此批通过。

以上日志位于 `/Volumes/tool/zero-task-tmp/` 或其 `vless-final-gate-20260913/` 子目录。历史静态门禁和专项通过有价值，但不能替代当前完整回归。

## 下一步顺序

1. 更新 packet-path 契约测试及能力描述，定位并修复 Mieru 双 association pooling 超时。
2. 将明确未覆盖的指纹/自定义 TLS 能力和刻意采用的缓存、超时、刷新策略分别列入实现差异；不能全归为“只差测试”。
3. 先补本批未执行的配置、ztls 及必要官方组合专项，再在稳定工作区执行一次完整门禁；使用单个受控 runner，保留退出状态与快照信息。
4. 统一文档、选择性提交，然后才进入推送、发布及生产验收。此次未启动新的全量后台任务。
