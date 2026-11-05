# C：非入站 Packet、设备/TUN 与适用进程

分支 `codex/v003-cloud-r2-device`，PR base 为 `codex/v003-cloud-r2-integration`。你是本轮第三个实现 agent，不是上一轮主协调 C。对应 Z003-04/05，只写[入口](README.md#文件所有权)中 C 的设备域。

## 已有成果与严格例外

WireGuard 标准入站 Packet source、原始 UDP listener/accepted Scope、neutral raw-IP source 已迁；旧 listener 与 ClaimedPacketLeaf/设备生命周期 SPI 已删除。不要重做这些接口。

本地基线已修复 policy-only outbound 方向撤除及 listener/持久化回滚后的旧 TCP 连续性：只保留同一真实非代理 linked client stacks，disabled 状态不得分配替代 stacks；不可逆业务取消在成功发布后执行。这些精确修复必须保留，不能再次将控制撤权和物理原件退休混为一体。

**TEMP-WG-OUTER-UDP-01** 限于既有 WireGuard `outer_udp_proxy` 及重连历史的原始关闭债务：保留原兼容行为，单独未关闭。本卡不纠正、不扩展，也不把它包装成符合最终严格标准；其他设备/进程/Packet 路径不获得同样豁免。

## 完整交付

1. 盘点非入站 PacketSink/Packet 转换、Direct/WireGuard 装配、TUN 拥有/借入模式、适用进程与转交 I/O 的实际生产调用者和原 owner。把 temporary 例外分支与普通路径分开列出。
2. 沿现有 ComponentFactory/Packet、设备装配和 native provider 接入同一宿主，协议仍拥有握手、peer/AllowedIPs/crypto；通用 Packet/stack 继续拥有报文转换和事实，不创建 ICMP Flow 或设备专用新 SPI。
3. owner 在原件取得前安装，真实 driver/worker 进入原 Scope；设备借入/拥有、原始 handle、返回通道与 join 责任明确。直接 Child reaped 不证明后代或全部 I/O/用户关闭，不建设通用进程沙箱。
4. 非例外路径严格确认逻辑用户、实际任务与原始 provider 关闭后再构造/启动替代；网络换代、丢弃候选和恢复保持同一原件责任。不能依靠 grace、Drop、端口可重绑或历史对象失引用完成退休。
5. 保留当前 TCP/UDP 连续性、Packet/Flow 选择、原始源地址、peer 授权、片段、MTU/MSS、回程与 Inner/Outer 计量，迁完实际调用者才删除剩余兼容执行链。

## 交叉文件和验收

B 负责其他协议出站的原始 carrier/control，若设备借用这些能力，向 M 记录具体依赖 SHA。共享 platform/native grant、runtime/transport、`register.rs`、handle/reconcile 由 M 串行整合；不要在自己的分支改整套事务。Windows/native 平台实现必须保持条件编译，实际原件资格不能用跨编译代替。

必要定向覆盖：真实停止前 pending I/O、非例外停旧启新、discarded candidate 原件保留、准备/发布/持久化失败恢复、方向撤除/恢复的旧 TCP 连续性，以及现有 Packet/Flow 和计量行为。使用同一原始 controller matching close 和实际 Scope join，不以 EOF 证明 cleanup。

特权 TUN、系统 route/DNS 恢复、跨平台或 ICMP 权限不可得时，明确**环境待验项**；可实现的所有权和执行路径不能因此全部写为待验。没有实际 driver/provider 的路径仍记实现缺口，不伪造通过。不更改用户现有网络/内核，不修改消费者。

交付“旧调用者 → 标准执行 → 原件/Scope → 删除边界”的表及本域真实回归。TEMP-WG-OUTER-UDP-01 以外仍有实现缺口时，不能将 Z003-04/05 宣告完成。
