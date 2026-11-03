# 单环境实现与候选交接账本

本轮接续 `c9b28431bea6e5124fa48ee810aa9d2272e7b4e7`，没有从远端 develop 重建、重置或丢弃后续工作。初始 clean `work` 分支 `5338c727acee537762cfdd95531f6e5130274200` 保留。唯一 remote 为 `origin`，URL 为 `https://github.com/zerodenet/core.git`。

精确冻结候选、完整命令/结果/失败/忽略与未执行项以 [PR #127 的 C 交付记录](https://github.com/zerodenet/core/pull/127)为准。本文件记录实现和兼容边界；实现提交、普通集成、云端资格、本地资格分别判断，不把中间 head 结果转移给最终候选，也不声明 ALL17 / G0–G10 已完成。

## 工作区与提交

| 所有者 | 同一环境中的目录 | 分支 / PR |
| --- | --- | --- |
| C | `/workspace/core` | `codex/v003-cloud-integration` |
| A | `/workspace/core-agent-a` | `codex/v003-cloud-config-transaction` / [#126](https://github.com/zerodenet/core/pull/126) |
| B | `/workspace/core-agent-b` | `codex/v003-cloud-hy2-outbound` / [#127](https://github.com/zerodenet/core/pull/127) |

`/workspace/core-validation` 固定在每组命令的精确提交，整组构建和测试结束并收集结果后才切换。Rust 1.99.1、依赖缓存和 `/workspace/core/target` 共享，由 C 独占排队；源码修改在 A/B 独立 worktree 并行。分支普通追加提交及 merge，未强推。A 初轮普通集成点为 `0cd6ec473d8b2e4190a54888545145737b9c0097`，后续通知/测试清理追加合入；B 普通合并实际 integration head 后再验证。

提交的逻辑分组如下；完整逐条 SHA 清单见本页后续的分支提交索引和 PR。中间修复提交保留，不改写历史。

- A 资源准备：`a5df1b58`；事务与领域发布：`23b1caac`、`ae2326b2`、`32997fc6`、`e50af731`；真实源原件：`bbdba27a`、`93611a7a`、`19d4aaf3`、`5339891f`。
- A 应用任务/文件/IPC：`4fe2a478`、`51a6687d`、`b2a98e20`、`c792e67e`、`876246d5`、`0f26fc92`；通知桥删除与 Ready 门：`4f10d3cf`、`fce58ce0`、`537abb33`；无调用者及测试作用域清理：`13c712ac`、`43c5107c`、`9cdfdb2f`、`65542d04`。
- B 同池原件与标准角色：`88d336a5`、`7d85f1bb`、`1815b9bb`、`e0d6b050`、`5a6b9b9c`、`9bdc72f5`；真实代次/关闭/有限失败预算：`f13e0ce6`、`8efbbb28`、`116233fd`、`79c596af`；实际 UDP 元数据：`9e12f45f`、`e259b41a`；公开 reload、真实材料与健康幂等：`87e71afe`、`e6e86dd1`；最终孤儿 stream-UDP 桥删除：`54a5e1e8`。
- C 共享契约/真实平台原件和应用网络：`be70a805`、`a5c4e64b`、`c10c86a4`、`1e2eab42`、`7799ed8c`、`1afd9164`、`83955347`、`327b324d`、`cb3448f7`、`42323dce`、`6a151424`；有界材料与未知清理门：`7ffc4f1d`、`9ccb8feb`；DNS 实际无变化复用：`4face7cf`；已发布 recipe 实际恢复：`2f1ae0f6`。其余诊断/守卫/兼容文档修复保留原提交。

## 已实现的职责与删除边界

Config 的规则/GeoIP/ZRS 资源 I/O 与运行编译迁到 `zero-runtime::configuration`。一次 `PreparedRouting` 为 route、bypass、DNS dispatch 复用实际资源；初始 run、Engine/Proxy from_path 和日志文档读取在原文件真实 close/matches 后才交付数据。Config 保留 ADT、结构/引用和协议委托校验，公开便利加载函数的独立范围另列。Engine 保留领域计划、快照、策略、selection、revision 及既有公开入口，Engine-only/FFI 不自动启动 Proxy。

唯一 RuntimeHost 的 ApplyOwner 持有受理操作、串行锁和最终 ACK。Ready 前不覆盖 last-good，候选业务发布门保持关闭；最终发布前控制面仍读已提交快照。最小恢复记录、原字节/权限、原子替换与目录同步、持久/临时区别和错误来源保留。失败候选应用/源原件清理不确认则不恢复；取消等待者不取消宿主收尾。原 blocking 配置通知线程桥删除，同一领域通知直接供宿主消费，迟到通知不能撤走未提交的 Ready 候选。

应用 dispatcher 的 source/sink/blocking publish 原线程、监控、JSONL/dead-letter/outbox/SQLite 原件、真实 webhook HTTP 任务和 IPC hook 退休已接入这条事务边界。原 writer/Connection/stream 控制器、完整回调返回、TaskScope/实际 JoinHandle 和 native close 分别核对，Unknown 保留。IPC hook 无效 cached clone/is_alive 链删除，已收到的 allow/deny 与清理诊断分开；退休代际停止新准入，普通不可达保持原 fail-open。

Hysteria2 根出站及既有 first-Datagram packet-path 使用协议自有同一个实际认证池、QUIC endpoint、H3/UDP 驱动和原始 Metadata UDP 所有者，交付现有标准角色与生产派发/计量。DNS/network/full profile/真实材料隔离，保留多目标/空 UDP、单接收者、背压和既有线格式。真实 ECN、源/目的地址、GSO/GRO 与 PMTU 能力穿过原有有界 QUIC wire 队列。没有重迁入站或扩大协议组合。

Root CA 文件由既有有界配置原件槽产生真实关闭后的惰性字节快照，crypto 在实际 lifecycle start 解析；身份比较真实材料而非 pass nonce。实际配置、compiled DNS dispatch 和 backend 未变时，原池、认证、endpoint、DNS 代次/cache/lease 与活跃 TCP/UDP 保留。恢复精确已发布配置时，使用其实际保留 recipe 和旧 crypto，按真实依赖/issued floor 构造新代次与取消域；原 Replacement 的清理和失败发布门继续生效，不复活取消的旧 owner。

删除 root HY2 TCP/UDP 私有桥、最后无调用者的 SessionTcpHandshake/SessionTcpConnectOperation 与 stream-UDP 桥。旧 listener、claim、handler/Any/resume 执行链未恢复。此前已迁入站留下的零调用 generic fallback/client-response/no-client-MUX/logged 包装清理，真实测试辅助限定 `cfg(test)`；源守卫改为实际标准 source、同 Scope、派发/计量与旧链缺席，不删除原行为测试或放宽原件/超时断言。

## 兼容与准确剩余范围

V1 配置/API/events、CLI、FFI ABI 和协议上游基线保持；只修改内核，没有修改 GUI/面板。Rust 内部 API 迁移见[兼容说明](../compatibility.md)与[兼容账本](../../../release/breaking-changes.md)。协议库既有公开 standalone HY2 convenience/pool API 保留原范围，不是 root 回退、第二生产池或完整宿主资格。

下列仍是实现缺口，不能全写成“只待本地验收”：

- Z003-01：System/detour/其余池化 DNS、打开失败重试及其原始资源所有权。
- Z003-02/03/06：其余 Mieru/MUX/gRPC/H2/QUIC/载体的完整生产角色、复杂 prefix、双原始 provider、剩余 UDP/packet-path 与 Registry 总 leaf 投影。已完成 listener/claim/handler 族不重新列待迁。
- Z003-04/05：非入站 Packet、设备/TUN、适用进程及转交 I/O 的完整宿主归属和替代门；直接 Child reaped 不代表后代或全部用户/native 关闭。
- Z003-07：适用资源预算生产配置；内部有限额度不是所有资源的生产可配置化，也不是低资源压力资格。
- G8 整体投影/控制观测对账；控制 IPC/HTTP/gRPC 服务器与订阅本体的剩余生命周期不由 config.apply 最终 ACK 或 webhook 原件结果覆盖，既有订阅 abort 链仍未迁。
- 独立配置/诊断/存储边界：公开 Config::load_from_path 便利 I/O、CLI reload 在提交 IPC 前的文件读取、初启控制面 TLS cert/key/CA、独立 validate/connector/quota inspect、principal quota lease/后台持久化，以及初启 URL 规则预取/缓存原件。这些不是新增 TEMP 豁免，不由已迁热 source 事务的 close 收据覆盖。

`TEMP-WG-OUTER-UDP-01` 单独保留且未关闭、未扩大、未纠正。WireGuard 的本轮字段 shorthand 属纯 lint，无债务行为或资格改变。

G9/G10 与 Z003-08 的完整资格仍另列：实际 GUI/面板同产物组合、特权 TUN/Packet/Flow/源地址/MTU/回程及系统路由/DNS 恢复、Windows/macOS 原件与平台矩阵、完整固定上游互通矩阵、1 CPU/1 GiB 同负载的 RSS/FD/任务/吞吐/延迟和恢复退出。云端为 Linux x86_64、4 CPU quota/16 GiB、无有效 capability、无 `/dev/net/tun`；不能冒充上述资格。

## 验证记录规则

最终冻结后依 [acceptance.md](acceptance.md) 执行格式、测试布局、真实依赖边界、25 分节/17 卡来源与链接、diff、workspace check、严格 all-target/all-feature Clippy、完整 workspace/all-feature 测试、代表性最小/no_std 和适用固定基线互通。未出现的 workflow check 不填通过；专门 workflow 的 dispatch 能力/网络限制与本地等价命令分别记录。

每组外部 JSON/原始日志在 `/workspace/v003-evidence`，记录精确源 SHA、tracked Rust/Cargo 输入摘要、lock 摘要、命令、UTC、结果与源码漂移。Rust 构建成功不是行为/native close 证据；空测试选择、失败、人工终止、忽略及未执行均单列。最早一次尚未等 compiler 真正退出就切换 worktree 的检查已明确无效，不纳入资格；挂起测试的诊断终止也不是 native 关闭证明。历史失败和修复过程保留，最终结果由最终精确输入重新判断。

最终候选只停在 integration 交接。本轮不合入 develop/main，不启用 auto-merge，不发版、打 tag 或部署。

## A/B 分支提交索引

此索引为各任务分支首父链的非 merge 提交；普通依赖 merge 保留在 Git 历史，A 的 C 测试修复依赖原 SHA 也可达。索引中的 head 是记录时输入，不替代 PR #127 的最终冻结候选。

### A：`65542d045540aa09ad9d9c130e9e5fad067e9bb5`

- `a5df1b586a8882708ec8b7acc03bbafd8855f5d1` — refactor(config): prepare shared routing resources in runtime
- `23b1caacd40f3c679fdaea98112089f7ff014154` — refactor(runtime): persist ready configuration under the accepted host transaction
- `ae2326b22a37c96269992f1be6c8f1e87e9d0e45` — fix(runtime): publish configuration snapshots only at the host commit gate
- `32997fc6be80208d6379b5c5a2aac61c85121a77` — fix(runtime): retain source outcomes and fence uncommitted candidate business
- `e50af7311dbb3270610c4e1ab410c4392e5eb599` — fix(engine): preserve acknowledged policy changes across pending configuration
- `91b29c7e57e9ae668ba42246ac2d432688c04a54` — fix(runtime): retain original error sources through failed source recovery
- `f7eb148b07b839efbb026cb28fdce6a50314433c` — fix(config): retain resource validation at the public engine command boundary
- `e6098842d3c469a2f0bbc1144e7a2cb626c86659` — fix(runtime): fence candidate UDP dispatch before constructing native I/O
- `ef93695ad14cb99f6b550866bafe67c435c19273` — test(config): use the existing required route shape in source fixtures
- `23537cd7a9aa1ed386996af7832353f24e7f5760` — fix(runtime): treat unreachable optional recovery sidecars as absent
- `eb732e3fc0d1dc0e0ea9375c18bb7ca21a48ced1` — test(runtime): observe the actual joined UDP peer after local admission rejection
- `8c52a5b3b29864b2e67dac70bbfb853b04de9e59` — fix(runtime): retain and retire actual application preparations before recovery
- `a52e536e1ea373929e4657783d82e416b0371ca8` — fix(application): track the original network epoch before initial preparation
- `f45323d5547cd5cbcfd7db133d61b83c88c334f3` — fix(application): fence dependent preparations while original cleanup is unknown
- `5b74cbdaa0627daee689801caf264532d2a07088` — fix(application): finish independent native retirement after dispatcher failure
- `f9f7057c3ec5fbd0ee58a26a617eeecc852dd784` — fix(application): retain cancelled startup originals in the single concrete slot
- `db96994a1f90b6f4ca7bf6fb1da8b32c9dfc6df8` — test(application): observe cancellation without requiring owner debug formatting
- `82ecb62929497cd2eff2d2b6581333d5a0abf569` — test(application): assert unknown cleanup fences before dependent preparation
- `a1a764f2d470c606b69b22f3ab1fb8021d4f952b` — fix(application): keep service shutdown visible to the application executor
- `4fe2a478bba23da4aaf9b3c5f7671b0a84878b79` — feat(api): recover the original JSONL writer alongside poison failure
- `51a6687d9b1002db0d1f6bc4e0520392a9724597` — fix(connector): retain original files and SQLite connections through dispatcher retirement
- `b1faadd0b24b4d707faedc45f2cb25bb538713bd` — chore(connector): limit standalone index test custody to test builds
- `b2a98e208a1e6972ccb7d99ef8650131f7ea1f7e` — fix(connector): retain original child joins across dispatcher panic and caller cancellation
- `bbfd862c646a7c0a7a593b01504fecba9b2434b6` — test(connector): retain and join the real worker owner in memory fixtures
- `bbdba27a4325e825f68217578ca69d99923be556` — fix(runtime): confirm original files throughout configuration preparation and source persistence
- `93611a7ac55b817316ae9dd4b85b8f77dcdf59bf` — fix(runtime): report source original uncertainty after the accepted shutdown joins
- `f7cf163c259abf6c1ceb8522a62b87b3e8828668` — test(runtime): keep concrete startup ownership cases in the sibling test tree
- `1106e1f89b5a70117c5673ed221dd9cf149c915a` — fix(engine): preserve exact embedded snapshot choices during explicit restoration
- `3e85d9450406485923ef20a2c27c805c77adc397` — fix(engine): retain the actual resource path in V1 validation failures
- `4416d5fd9695376deeb3fb35ff13ce1b7f2a2aea` — fix(engine): preserve the V1 validation summary and assert original error fields
- `461989108160024fbfd3084f08b35fca12fd5089` — test(application): join the original HTTP peer without starving its runtime
- `c792e67eb504046e24291bf6dd3b3d759ae91dd7` — fix(application): retain and retire the original IPC hook callbacks and native streams
- `876246d564aa3d89ed3386b8981df8c31a7b790c` — fix(application): retain delivery status when original cleanup is unconfirmed
- `0f26fc928fc88f178f89e3e863d18f9fe9b28f2a` — fix(application): stop retired hook admission without changing unreachable fail-open
- `eda57bf7258586fe29ea364fc2e9db0497fe92be` — test(runtime): observe the actual simple UDP peer primary join receipt
- `bc3b98be79c98d7c421410894adea4c788f359b8` — test(proxy): verify the real TCP rejection and its separate original task join
- `19d4aaf310215f9faf4558fddb135ddf6ee85d75` — refactor(runtime): retain the actual initial configuration document owner through preparation
- `5339891f88f6e87ec8b43b69c75e8d6577d108a7` — fix(runtime): preserve the original recovery error boundary during initial loading
- `4f10d3cf57ffa9ad3eca84fc0e00c021f4b5f712` — refactor(runtime): consume configuration notifications directly without a blocking relay
- `fce58ce0558134971f5f83b1fe2489642dee70c6` — fix(runtime): retain the same ready candidate across delayed domain notifications
- `537abb3355a9212ea02d844fddbe43f76a4b2490` — fix(runtime): keep concrete configuration watch storage outside the engine dependency boundary
- `13c712ac12b4900e277c5d1d3e0935fcd3014cb5` — refactor(proxy): remove unused compatibility closures and limit real fixtures to tests
- `43c5107c8aab1815e8235831c08ec792ef5a4dc9` — fix(proxy): retain diagnostic test dialing and share the original pooled fixture module
- `9cdfdb2fdfb15d4a768cfbaf2df8759a53946328` — fix(proxy): retain exact acknowledgement types and remove irrefutable fixture branches
- `65542d045540aa09ad9d9c130e9e5fad067e9bb5` — refactor(proxy): keep explicit original fixture declarations while closing unit lint findings

### B：`9f34d7381241e2140b2deba0b9c9ced29366313f`

- `88d336a56bc60e20455a87d19efb0f995d52d4ff` — refactor(hysteria2): prepare sealed outbound authentication and packet loans
- `7d85f1bb4b84ac6be81a04c07a9a327d8b28567a` — refactor(transport): retain each Quinn wire original through scoped retirement
- `0f60f36c7104d57d03e97cfb94e0df9239ed6648` — test(transport): separate packet view retirement from original native close
- `120fc6dc24bcc2085bf712ff3c8a7503b9247280` — fix(hysteria2): preserve workspace formatting and original close identity
- `342de7a6f4e1f81167a212ee7c3188157bd9c475` — fix(transport): retain borrowed controls behind synchronized ownership
- `1815b9bb3a99592e7d7f2d801ca31722ce82bf2d` — refactor(hysteria2): retain authenticated pool entries and standard outbound roles
- `e0d6b0502909fb92ea6f3d55e032c061cb397412` — refactor(hysteria2): retire actual hop originals before replacement and reclaim idle entries
- `7a0474350a986328a697306cb93be03619f449d7` — fix(hysteria2): share original reassembly with owned UDP users
- `5a6b9b9c90e45deef47347804b5b39c1be6c0353` — refactor(hysteria2): project complete profiles and preserve lazy UDP and request accounting
- `9bdc72f559e82e60e66e9fed637b2f884023f8f6` — refactor(proxy): dispatch Hysteria2 through shared standard roles and remove its old claims
- `adb8f8bc4ae4efd41ed07b854c2b466cfec8024e` — fix(hysteria2): preserve ordered dial fallback and original cleanup under pool races
- `76d849d4a9e0b317aee816f618697f268329e9fd` — refactor(hysteria2): preserve actual task failures separately from native close
- `c9b5c329c622d66854f2778fcb3de5ad7525f5af` — fix(hysteria2): retire the retained entry state during candidate cleanup
- `af40155b72c4619a153723470a869af129662942` — test(proxy): exercise standard Hysteria2 shared TCP UDP and packet dispatch
- `c92080d680700dbd617639888a703b56ba686203` — fix(test): use the actual dispatch socket close report
- `19556c5984ce080c4f65d956388d9ebe4b586384` — refactor(hysteria2): prepare CA material through the actual host file grant
- `07b06e3b4ac7edafe2ef77b4eef13bdd9b136806` — fix(hysteria2): retain logical controllers and preserve payload accounting across reconnects
- `e4d8104780304ca8a0061b70ffb0480e37356ddc` — fix(hysteria2): retain one bounded packet allocation while reopening a channel
- `389995e979d9a8df4d3a7ec1533c2fc29ee18865` — test(hysteria2): verify actual identity and network generation retirement
- `80a87d9f785fa6cdb09c95e53851af1c70c2ee3d` — fix(hysteria2): retain selected candidate across native port hops
- `3cbc37387c0481814a72302a8c446e6e73cc9c8c` — fix(transport): receive native observer failures at the packet worker boundary
- `f04174af40378c020205aeaf6f4a8dbb0aca79db` — fix(transport): move original packet views into the bounded worker future
- `88262ed553cf4f4495991ac4bb87e8feca8638c8` — fix(hysteria2): bound each owned candidate dial by the original deadline
- `2d825e2e25bb7f37fe6b638adf1fb0d054140eaa` — test(hysteria2): drive actual Node candidate fallback with concurrent pool users
- `d18eaeb65635475bdd775181d2144b9785a43446` — fix(hysteria2): return the actual logical retirement report to the host
- `0989cfeeeac4cee27d0b774e91291b20022730c7` — fix(hysteria2): hand off the unique logical close report exactly once
- `8f60c160a528663dffc36b269155fe91d7896c14` — test(hysteria2): keep original logical close blocked by pending transferred views
- `aa138f67cc59f2d49c1b4eecd43dc494eabd6cfa` — test(hysteria2): reject full real UDP session capacity before explicit reclamation
- `8111b43e20718a9b6ed1e5cd17f20915894b7bb6` — test(transport): preserve bounded packet wire queue backpressure
- `f3d2d9e06560787a678d77d6b7f84822ccb6b714` — fix(hysteria2): retain interrupted authentication under the original deadline
- `097aa7f7f838d04934ad39e383251808de7ea5de` — fix(hysteria2): issue new donor loans from the actual current native hop
- `8e9849d541cb1840b7586176636645a4c139c134` — test(hysteria2): verify real native hop receipts and donor continuity
- `e05ad517553a8d88880c59ea4faefa5c97b5bab4` — fix(test): use public HY2 wire codec and hashable target identities
- `1fc3a4fa77f3bba32f5ecd5db8dcddf5e054135c` — fix(proxy): rearm only successfully continued standard Datagram associations
- `59cc20442c717cda17e078da235c1baa57162938` — refactor(hysteria2): project cold pool profiles without initializing legacy caches
- `a5bd66828319de1a1f709390f5f45c3b034e3ade` — test(hysteria2): cancel a real pending QUIC dial before same-role reconstruction
- `cd7a45f1efa9fcbfb1dbc12bbbdef9864603b46b` — fix(hysteria2): retain actual preparation pass identity and request-bound TCP callers
- `327e4baeb6149a9599c987ddce17ab62ff6db6e5` — fix(hysteria2): retain original UDP installation outside synchronous callback locks
- `f13e0ce6e2b3bfce817e38bc67c60e126bff597d` — fix(hysteria2): bound unpublished real task failures in the original generation budget
- `4a075f3fb142c1319688c834229acd5e119addc2` — fix(test): retire the real HY2 peer using its existing protocol close code
- `8efbbb28698d82cce6282fb4cce6526296f1f5ea` — fix(hysteria2): preserve real carrier facts DNS epochs and last-good crypto across original cleanup
- `116233fdb3458b22746d03e776f9115b03ba10ea` — fix(hysteria2): retire truly idle application owners before ordered protocol stop
- `9e12f45f862a7be6c7495721eec9c93f628ec96a` — fix(transport): preserve actual native UDP metadata through the bounded Quinn wire queue
- `79c596afdf4b43a51afa737321096671ea5016ce` — fix(hysteria2): pin actual DNS authority for every transferred UDP association
- `e259b41a11de8c4c1e6cad8802afd734c68ea633` — test(transport): distinguish native packet source and destination metadata
- `87e71afedeb453b5513e259bf212c7a6f08b16b8` — test(hysteria2): verify public reload custody and same-path CA rollback
- `17e2639291a7c6d3d741b989bcaf60164b6174ff` — refactor(hysteria2): name retained receive and match the original close permit
- `e6e86dd121b5feefa99ddac312544cfd42445692` — fix(hysteria2): retain actual owners for unchanged prepared CA material
- `a2dcde86774ad8ba3bb962d50b0b1cd39ec29ad0` — refactor(hysteria2): deduplicate prepared CA paths through one map entry
- `11dc62cfe19a9a707cc78e2c84924d9304048da7` — refactor(hysteria2): remove the retired preparation pass identity gate
- `54a5e1e8171cc61e78b3d658b375adbf5cda9563` — refactor(proxy): remove the retired stream UDP relay implementation
