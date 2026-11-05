# A：Z003-01 DNS 完整收尾

分支 `codex/v003-cloud-r2-dns`，PR base 为 `codex/v003-cloud-r2-integration`。在 M 指定的独立 worktree 实现，只写[入口](README.md#文件所有权)中 A 的文件。

## 继承现状

基线已有真实 System blocking 原件/Scope、name/wire/Node 查询监督、网络代次屏障、普通 native DNS provider、直连持久 DoH，以及无独立任务/共享依赖、默认 dial 的标准 Stream detour。DoH 复用原 clients/connect_lock，真实 driver 独立于首个查询；取消的 Stream 原件在原任务退出前清理，嵌套 Node/System blocking 子任务必须真实 join。不要重写这些成果。

具体当前边界见[本地记录](../../implementation/native-dns.md#标准-detour-与同一持久池)。仍存在 `runtime/dns_outbound.rs` 的 `ProxyDnsOutboundConnector`、`Session::new(0, ...)`/裸 TCP 后备、`prepare_datagram_exchange` 回包链，以及 DoH 等 backend 对旧 `DnsOutboundCarrier` 的消费。它们的旧调用者没迁完，不能直接删除。

## 完整交付

1. 盘点 System、直接/绕行 DNS、各既有 backend、持久池和失败重试的实际生产及库内调用者，列本域旧执行链与保留的有职责公开库 API。区分外部 convenience API 与普通 Proxy 后备，不能为维持 API 留第二生产执行者。
2. 将剩余 resourceful/shared-pool/device detour 所需能力交给 B/C：具体调用者、目的事实、标准角色、实际 original/control/任务归属和等待条件。B 负责协议出站原件，你负责 DNS 的持久消费与退出；不修改协议私有池或另建 DNS 出站实例。
3. 在现有宿主、现有缓存及现有 query Scope 中接通真实普通 Proxy 入口。健康池保持复用；selector、profile、DNS/network 改变、失败 retry 和 idle reopen 在旧 logical users、driver、provider 原件确认后启新。
4. 取消立即停止业务，但必须保留真实控制器、blocking 子任务和关闭错误直到清理完成。独立持久 driver 不挂在首个查询的 ancestry 上，不等下一次 dial 才回收已取消查询的原件，不新增维护 task/registry。
5. 迁完全部本域旧调用者后删除相应连接/回包执行链和多余包装，普通路径不再制造业务 SessionId。确需保留公开 standalone API 时说明其真实职责和调用者，不把它用作生产后备或原件缺失的豁免；公开 Rust 变化交 M 记账。

## 不做

不扩协议组合、不升级 DNS 传输或协议上游、不改 route/Node 策略、不绕过 detour 直接拨号、不增加替代协议 SPI、不使用错误字符串或 EOF 授予重试。不要把 B 未完成的必要所有权写成“只待验收”，也不要用长期关闭普通登记或拒绝原本合法配置关闭本卡。TEMP-WG-OUTER-UDP-01 不修改；若唯一剩余调用者触及该例外，明确交 M 对齐例外边界，不能另开普遍豁免。

## 必要验收

- 真实普通 Proxy：System、已有直接/绕行 backend、域名 Node、持久 DoH，多请求复用与不推进多余调度。
- 失败候选、TLS/认证失败和后备：先消费同一真实 original/control，清理未完成不得申请下一资源；原查询与清理双错误保留。
- 实际 I/O Pending 时取消、嵌套 blocking lookup、网络/配置换代、无变化 reload、selector ABA、退出和恢复。
- Scope joins、logical custody 和 matching native/provider receipts 分别断言；库 API 不混入生产路径。
- 生产调用归零检索及受影响最小 feature，相关 no_std/依赖边界保持。命令按[标准](acceptance.md)由 M 排队，避免重编未变的大目标。

完成必须交付“全部实际调用者 → 标准执行 → 旧链删除”的闭环。局部 detour 成功、声明 provider 或单个准备接口不算 Z003-01 完成。
