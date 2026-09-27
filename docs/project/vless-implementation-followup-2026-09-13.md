# VLESS 功能推进记录（2026-09-13）

用户要求先推进功能实现、暂不测试。本轮没有执行单元测试、集成测试、互通测试或全量测试，没有启动代理验收服务；只生成固定版本指纹源数据、进行格式处理和编译检查。新增/更新的回归用例留待之后执行。

## 已写入的实现

| 项目 | 本轮改动 | 边界 |
| --- | --- | --- |
| TLS 1.3 HelloRetryRequest | 解析与校验 retry、cookie、session ID、suite、group；重建 ClientHello 并形成 message_hash transcript；拒绝重复 retry、未提供的组和更换 suite | 在通用 ztls TLS 1.3 客户端中实现，未宣称 REALITY 专属握手也支持 HRR |
| 握手与 ALPN | 检查完整 CCS、避免片段未齐时消费；验证认证后的 ALPN、握手消息顺序和 Finished 边界 | 没有新增 TLS 1.2 握手引擎或客户端证书功能 |
| 普通 TLS 指纹接入 | 直接连接与 relay TLS 都可使用完整 ClientHello，复用已有 CA/证书 pin/名称验证策略；保留可分别切换读写的 Vision control | 当前要求显式 TLS 1.3 最小版本；ECH、会话恢复、旧 TLS 组合继续走已有后端；自定义曲线与禁用 SNI 已在第二轮接入，不能宣称这些组合也完成浏览器指纹模拟 |
| 异步握手生命周期 | 新建 TCP 和 relay 共用带超时的异步握手，写出后 flush；移除不可随 future 取消的阻塞轮询线程 | 未运行取消场景测试 |
| TLS record 交接 | 异步读取限定当前 record，交付明文后再读下一 record；切换 raw 前拒绝未完成的加密 record，先清空已接受的加密输出 | 未运行 Vision 双向互通 |
| 指纹目录 | 加入 Chrome 70/72、Firefox 63/65，目录从 22 扩至 26；从固定 uTLS 生成数据并保留既有 22 份 capture | 新增 4 个模板尚未握手验证 |
| random | 使用固定 Xray 的 19 个 ModernFingerprints 候选，进程内选择保持稳定 | 不再随显式模板目录扩充而扩大随机集合 |
| VLESS 能力信息 | 更新 packet-path 旧断言、公布 quic/mkcp/hysteria carrier 元数据 | 用例未执行；FinalMask 是修饰层，没有混入 carrier 列表 |
| Mieru UDP 退出传播 | 会话仅弱持有响应 sender；worker 退出能关闭既有和后续响应等待者；通过 is_closed 通知运行时清理失效连接 | 修正静态可确认的生命周期缺口；此前首包超时的根因仍未闭合，不能声称已修复该超时 |
| 文档 | 修正仍在测试、REALITY probe 未实现、早期 XHTTP overflow 和指纹互通未执行等过时表述 | 历史失败/中止快照保留，新增代码不继承历史通过结论 |

## 仍未完成的实现范围

- TLS-1.2-only 指纹（Android、360 7.5 等）、PSK 指纹/完整自定义 TLS 恢复和旧 Kyber 模板。
- 普通 TLS 其余策略组合及 QUIC 的完整浏览器指纹模拟；当前 provider 配置仍具有原有意义。
- Mieru 双 UDP association pooling 的首次响应超时根因。响应 channel 生命周期修正没有测试结果证明能够消除它。

缓存/资源上限、超时和证书刷新等刻意策略差异继续按项目规则保留；未将它们擅自改为无界官方行为。

## 验证与交付状态

仅编译检查，不能代替功能测试。测试按用户要求延后，包括新写入的 HRR 和 UDP worker 退出用例。改动仍在原有 dirty `develop` 工作区，本轮未提交、推送、部署或生产验证。

最终 `cargo check --workspace --all-features` 退出码为 0；仍有 config 的两项未使用警告和 proxy 的一项 dead-code 警告。`git diff --check` 通过。未编译或执行新增测试目标。

本轮修改前的文件副本保存在 `/Volumes/tool/zero-task-tmp/vless-implementation-20260913-before/`，用于区分此前工作区修改和本轮增量。编译日志位于同级 `vless-implementation-20260913-check*.log`。


## 第二轮：常用 TLS 策略与握手兼容能力

本轮继续遵守暂不运行测试的要求，优先处理配置使完整指纹路径失效，以及合法握手分片被拒绝的问题。

- 普通 TLS 完整 ClientHello 路径现在支持 `disable_sni` 和显式 `curve_preferences`。SNI 关闭只移除报文扩展，证书名称验证仍使用原配置。曲线顺序去重后进入 supported_groups，首选曲线生成真实 key share，其余曲线可由 HRR 选择。复用 transport 已有组实现，覆盖 X25519、P256/P384/P521 和已有三个 ML-KEM 混合组；HRR 重置密钥时保留该 provider。
- 通用 TLS 1.3 客户端默认只公布能够协商的 TLS 1.3 版本和套件。浏览器源模板仍保留，显式策略覆盖后的报文不能当作未经修改的浏览器 capture。
- ServerHello/HRR 现在支持跨 record 分片，包含握手头本身被切分的情况；单个明文 record 上限 16 KiB、整个 ServerHello 消息上限 65535 字节。兼容 CCS 可在 ClientHello 之后、ServerHello 之前出现；拒绝打断分片的其他记录和 ServerHello 尾随消息。HRR cookie 使第二个 ClientHello 变大时，输出按 16 KiB 分片。
- ALPN 接受列表来自实际生成的 ClientHello，不能接受仅出现在配置中、被 `randomizednoalpn` 模板省略的协议。
- 已补充策略、真实组密钥生成、ALPN 和分片的回归用例源码，暂不执行。最初执行 `cargo check --workspace --all-features --all-targets`，随后主动中止额外测试目标编译（退出码 130）；一次缩小包范围的编译因特性集合变化引起依赖重编译，也主动中止（退出码 130）。最终保留同一工作区特性集合，执行 `cargo check --workspace --all-features`。以上均未运行测试，不能把未完成的测试目标编译记为通过。

该路径仍要求显式 `min_version: "1.3"`；默认允许 TLS 1.2、ECH 和恢复配置继续使用既有后端。TLS 1.2 指纹引擎、PSK 恢复、旧 Kyber、QUIC 完整指纹仍未完成。此次未改动 REALITY 专属握手，也未推进 Mieru 超时问题。

分片规则依据 [RFC 8446 §5–5.1](https://www.rfc-editor.org/rfc/rfc8446.html#section-5.1)。本轮修改前副本位于 `/Volumes/tool/zero-task-tmp/vless-capability-closure-20260913-before/`；编译日志为同级 `vless-capability-closure-20260913-check*.log`。

第二轮最终 `cargo check --workspace --all-features` 退出码 0，用时 2m13s；保留 config 两项、proxy 一项既有未使用警告。`git diff --check` 通过。未运行任何测试；未提交、推送或部署。


## 第三轮：默认 TLS、PSK 与 ECH 统一指纹路径

普通 TCP TLS 直连/中继改用 rustls 0.23.40 的连接内模板钩子，取消默认 TLS 1.2 范围、恢复和 ECH 对指纹路径的绕过。钩子在 binder/ECH 计算之前执行；缓存隔离、真实 key share 和证书验证仍由原状态机负责。vendor 保留上游许可证与精确 SHA，workspace 固定依赖版本。

这里闭合的是这三种组合的 ClientHello 模板接入，不是全部浏览器行为：不宣告 rustls 不支持的 CBC/RSA 密钥交换套件；未补 HTTP/2 ALPS settings 消费、TLS-1.2-only 历史模板、旧 Kyber、OpenSSL/QUIC 完整指纹。新增回归源码未运行。此前已通过的互通不适用于新补丁。

本轮提交将包含此前相互依赖的内核实现、协议文档和版本固定的 vendor 源码；本地审计记录与编译产物保留在工作区。远端目标为当前上游 origin/develop。
