最新接续见[2026-11-09 17:08 检查点](checkpoints/20261109-1708.md)。五域仍继续，未交付最终资格；以下旧日期段落均是历史，旧环境遗失的原始证据没有恢复，也不追认新候选资格。

当前完整实际调用者集合按职责域冻结：[A DNS Query/pool/provider](evidence/a-dns-callers-20261109.md)、[B 出站13个执行族](evidence/b-callers-20261109.md)、[C Packet/设备10个入口](evidence/c-callers-20261109.md)、[控制/G8](evidence/a-control-caller-20261109.md)。B→A 真实原件消费继续配对；不能据一个协议分支完成或保留 gate 关闭该域。

每个调用者必须依次满足普通生产装配、实际原件/任务完整、停旧屏障、旧链删除和必要新输入回归。当前新增的 Mac File/helper 关闭、普通 QUIC Factory、控制故障收件不使整域自动完成。实际生产 scratch、工具输出、DNS 交付前原件和其余合法出站/共享池的实现仍继续。


## 2026-11-07 13:27 UTC：同原件实测结果、最新配套与新锁

当前实现d5f77fdbd3cc65d5d66712378322f4408865b784；最终候选未冻结。
实际offline metadata仅增加zero-connector→zero-engine本地边，无第三方包/版本/source改变；
Cargo.lock与fixture同0d118db581a040ffeca07fe3d16173d25d18639e2d45c5269ce32c22326c5dd3。
解析命令原记录m-46a597-actual-kernel-local-lock-resolution（exit0，锁改变故不当资格），
逐包diff/proof与已用旧548锁全留仓库外。

81b69566bc3ff7a73d7ba3b6c219b11b76479b1d / 548锁新产物实际1576.37s no-run成功，
日志SHA8864edebb6513f71fe0dbd0b5e116b397079cc383038dcf02b4a8c2c2995458f；
Proxy ELF SHA51e2332b9dd15eca9371aec4ef3c1f0972b20101482c295dcdcfc53effad46cf。
同输入结果由m-81b695-serial-regressions.json逐组登记，全部检验nonempty及输入完整性：

| 实际回归 | 结果/边界 |
| --- | --- |
| Lookup/Trace真实取消、旧raw query/native/Scope | 2通过，同16MiB原测试环境；原SIGABRT修复实际获证 |
| 原VLESS材料/普通导航及完整tail | 7通过；另同81b三协议client回应各5通过，不能归因为所有VMess路径已修 |
| 实际gRPC / IPC / HTTP-SSE旧wire、首帧、队列与joins | 24 / 94 / 23通过，原始snapshot Engine2、Runtime/Principal7、Traffic3、Packet5通过 |
| DNS既有profile/cancel全组 / 原双family wire | 50 / 4通过；Catalog完整DNS设备/共享原件与旧链删除尚未完成 |
| 实际H3 endpoint最终drain、TUN completion/exit、TunStatus | 1 / 3+2 / 2通过，不代完整B/C域 |
| 实际hosted preparation / ProbeOwner / ApplyOwner / control retirement / initial material | 3 / 2 / 9 / 1 / 5通过，不代新NoCURRENT片 |
| 冷Target恢复 | 失败0.01s，旧Critical caller故障被当sticky NativeFault永久重返。A五file窄修baa8385e8将同TaskId/同Arc交原CloseReport，Native Unknown门仍保；待新ELF原强断言复验 |
| VMess gRPC domain/first/three-hop | 同3个outer deadline仍失败，组2通过3失败，101.99s；未假称MUX回应片已修 |
| WG fullTX原cap4 | 失败5.02s：SYN已真到peer，5次SYNACK时TX4frames316B满额，kernel payload0。C正在完整修认证/Stack准入前的读原件转换自等，不提高cap/期限，不归TEMP |
| Metadata N1新ACK夹具 | 初始化先于ACK阶段因probe attempts1失败，原GSO/GRO各含v6/v4必要探测；d5f77只修新增正向夹具探测声明4、保MainN1/Root2/任务数/3s/ACK，且强化原probe1负向的matching原生关闭。生产容量未变，尚待实际N1验证 |
| VLESS verified decoy原job | 真Err为撤权后TCP ConnectionAborted，B核到job先poll work再canonical stop；准备窄producer+真实pending/真实先到故障反例，不吞ConnectionAborted字符串 |

旧队列漏请求zero-dns library ELF后assert中断，日志保留；未启动组未计通过。
后续使用同81b产物补完上述三控制等组，再实际构建同输入DNS ELF并跑4项，未借0bf旧ELF。

最新实现配套与剩余：

- M5409完整Connector actual Engine生产源+installation已接原event/history/SourceBook/DeliveryBook，
  不构造丢弃snapshot，保原bootstrap/continuation/持久sink顺序。09a补count与bytes独立压力回归。
  1713该片all-features lib/tests类型检查15.75s通过；不是实际5回归资格。
  原Engine subscriber dequeue后count续存、Drop登记/filter清理仍由A继续，不把此缺口隐藏。
- B→A/C/M共享前置46a59707ee0109d3d26778dc315682a042b16535：26file完整genuine setup Aux、
  同unique queue/Endpoint长期driver跨hop责任、final sealed dual-Arc registration ACK、DNS外owner
  锁外被动等实际ACK。primary registrations()保原语义，消费者须同时处理related_registrations。
  不是完整QUIC/余出站迁移；其registration slice新存储/临时峰值预算仍列未完成。
- 1713第二型检失败仅新增transport测试漏Duration两处，原exit101保留；d5f77追加import。
  当前冻结d5f77同新锁串行检查实际producer+consumer与Source5/Metadata2/probe1/Quinn真实driver
  回归及全控制Proxy类型检查，结果以实际新record为准。
- M b574清理Windows已计额原pipe entries时只借一原件，不再申请全ledger快照；原Native/worker/
  Scope/ACK门不变。当前Windows改动仍需对应编译/真实适用环境资格。
- A NoCURRENT同Host唯一book/真实三frontend任务强custody整片尚未冻结；C完整Source/设备/helper/
  独立WG provider、晚continuation及实际Resource故障接收桥仍在outside，不启缺口gate。
  B完整QUIC/native+borrowed、SOCKS双原件/Mieru/进程及所有剩余合法路径继续，不删未迁旧链。

13:04原www.cloudflare.com:443只读DNS复查仍errno-3；不改目标/断言/期限。
同最终head CI、最终可下载archive及SHA尚未取得；开发日志与不同输入通过不冒最终资格。

## 2026-11-07 12:58 UTC：实际cap4、MUX尾部与完整生产事件source

远端integration再次核实仍为59615e69ea5a413d657a873157ea4adf8f204a90；
本地普通合入至81b69566bc3ff7a73d7ba3b6c219b11b76479b1d，无reset/强推/脏覆盖。
五域未闭合，仍非最终候选。完整Source隔离片继续，不启未完成gate。

- WG原0bf cap4首SYN失败由同控制原件1+cache/outgoing/padded/cipher4超过4产生。
  C18eb与M5ced SDK成对改为保留实际不可移明文、直接写预付cipher；同算法/首nonce，
  不提高cap/栈/期限。7c779固定548锁实际stack1与WG真实握手/重试3项通过。
  这不证明后续TCP ACK，原fullTX已排至81b大产物后的实际回归。
- B842+4423真实三协议client FIFO在原bytebook背压，旧SID pending帧不串新receiver。
  首新类型检查发现VLESS native构造者遗漏，原失败完整保留；e0b追加None修复。
  81b同548锁三协议响应真实回归各5项通过。VMess旧3个outer timeout仍需实际归因。
- 09f含8f原始initial snapshot、63e6 Metadata localACK，Engine/Runtime typecheck
  79.53s exit0、Root/Grpc/Platform全控制typecheck389.39s exit0；新实际回归正在排队，
  不把0bf旧ELF或历史通过数追认为最新片资格。
- M本次完整Connector producer/生产installation配套：optional kernel-events实际Engine源，
  live同RetainedEvent在原SourceBook取得count/bytes后转交同Arc，回放在原history guard下
  先预付再保留，不构造Value DTO/选择Vec；同book replay优先避免N1未来帧饿死缺失前缀。
  持久sink用真实登记后cursor代原来被丢弃的flow.snapshot，初始engine.started先行，
  reload仍用原continuation cursor/待投递原件。原generic API仅保独立嵌入真实用途，
  唯一普通application调用者改到具体kernel入口。原始指针/额度拒绝/真实桥线程join/
  N1前缀进展/实际JSONL旧源退休后新订阅handoff回归均已实现，尚未编测；本地依赖边
  将在当前Cargo队列结束后由实际offline metadata解析，同新锁再验证，不冒用548资格。
- B继续完整QUIC与Metadata genuine per-attempt Aux/实际hop unique queue切换责任；
  A继续同Host唯一ControlBudget/真实三frontend任务交接；C继续实际失败close continuation、
  原错误receiver接收与Host ACK后有限恢复epoch、完整Source生产装配及旧链删除。
  部分query已迁、现Windows原始provider存在及小回归通过，均不代整域完成。

原日志/input/result/同锁/ELF摘要保持仓库外。尚无最终同head CI或可下载最终归档。
REALITY原www.cloudflare.com:443保持目标/断言/期限，环境DNS恢复后仍须定向复验。

## 2026-11-07 11:56 UTC：原首帧、局部ACK额度与完整未闭合调用者

当前主线实现 `63e6a17da9c1657702661217a188120d77ed345c`，工作树干净；仍非最终候选。
实际Cargo/候选fixture同 `548ad5680840a159645a20a7412b09aa44a0be2298c7ca52cf39921552a4ed2c`。
相对旧bb锁，两个offline解析仅加zero-grpc→zero-proxy和zero-proxy→serde本地依赖边，
没有第三方版本变化。不同锁/checkout的结果严格分开。

| 实际生产调用者/owner | 当前原件与执行落点 | 剩余实现/删除门与必要回归 |
| --- | --- | --- |
| HTTP/IPC/gRPC Config/Runtime/Traffic/Packet/Tun 查询，M/A | 已接同原表/真实config代次/原TunInfo与Egress metadata guard；Packet route最大登记数接实际同表生产配置，Closing等lease与response双归还 | 最新原件字节/旧形状/定向压力与配置恢复，Endpoints/Health/Capabilities等剩余生产投影继续；不能用这一子集关闭G8/预算 |
| 三控制initial flow snapshot，M，8f156d08a | live先登记，同Session/Principal冻结；预付后直接生成最终首帧；IPC ACK在前，SSE replay→initial→live，同原N1 slot与组合额度；gRPC原队列首项 | 新producer/真实消费者全片已提交但尚未编测，需原wire/Scope/Pending/N1回归；不是空准备接口 |
| Connector dispatcher真实生产source，M：services/installation→prepare_event_dispatcher_paused_with_network→dispatcher.rs source.subscribe(default) | 仍用owned初始快照，已有原delivery budget/Scope/network代次 | 必须接同实际delivery owner与原件预算，保持startup watermark/engine-started/持久投递语义；迁完才删除旧生产链，不标standalone-only |
| NoCURRENT公开blocking调用与真实三前端CURRENT work，A/M | A正在同Host有限原command rows中强持真实raw JoinHandle/outcome/cleanup，移交原PendingCommand permit，不建第二账 | 裸metadata/裸permit不证明Work所有权；真实登记后发放资格、callerDrop原件交接、原Scope joins/HostACK、唯一book shutdown/reopen均需完整pair和回归 |
| Metadata pool既有全部消费者，B/M，63e6a17da | 原native/users独立正面可见；本owner局部N等actual caller/graph joins+HostACK才退；固定caller/receipt Arc借阅与ordinal逐项扫描，无临时Vec | Root2>localN1真实UDP/迟ACK新回归已注册未运行；复验HY2等既有调用者停机不自等，不把此N修复当完整QUIC |
| 完整QUIC与其余合法出站/共享池/process，B | 未激活完整pair持续实现：firstIO前原Metadata/CarrierSlot强custody，实际setup注册窗、phase-controller、原FIFO及Scope/native/ACK、原Root逐frame字节 | 实际生产source/roles、所有既有合法callers、借入与native两原件、旧跨执行器/投影删除后才可关闭B域；无新增例外 |
| 完整Packet/Source/TUN/独立WG provider，C/M | 隔离audit有实际wire装配、同Source origin投影、finite原错/逐原件清理、原LoanOwner晚正面观察与原FlowUsers census | 未合入/启gate；全peer/原cleanup continuation/普通失败恢复/helper与转交/metadata预算及旧caller删除继续。无provider/Scope仍属实现缺口 |

实际检查记录：

- fe96：首locked因verify ignored旧bb锁立即exit101，原97923c18日志保留；同步0c3后
  66.55s exit101，Tun producer缺直接serde（13条连带诊断），原65830b5e日志保留。
  dfacf补真实本地serde边，实际锁548ad；不升级第三方。
- dfacf Engine/Config tests typecheck16.12s exit101：Traffic fixture不存在Success枚举，
  4b848修DirectRelayed；同两包v2 5.19s exit0，日志
  8dac1c7c41564ea2d375f44409297a4b700ce97efc29b29f921590e9f7e74916。
- 4b848全控制 typecheck312.07s exit101，tonic展开具体Proxy AutoTrait溢出，
  f20d7e7e69171c361ba875b72cd747af10933f986a3c702e784b27bf19dae860；
  46e42在私有具体handle存储边界封装，未提高recursion/栈/容量，未加SPI/provider。
- 46e42全控制typecheck10.54s exit101，仅HTTP两个尾临时借用E0597，
  4c00a536be5084241818747e955a00702bf26b593df422879db81a77642fc2ba；
  70dd追加局部encoded结果确保原guard前销毁临时，不改断言/额度/wire。
- **70dd9726b** Root+Engine/Grpc/Platform/Runtime/DNS all-features lib/bin/tests类型检查
  **41.8s exit0**，原日志
  8427f27ee4bec831c83f140689c3e66324ffda306e87d92400a0fda2860bf640。
  同输入必要ELF no-run1137.37s exit101，仅DNS fixture私有host字段访问，原
  c4fd8af5418ac2a8deb044f490893be96662ee530b766e7027e7f4a079a8e00d；
  未运行本组测试。0bf1a72ef改走既有protocols.runtime_host，保真实计额断言。
- **0bf1a72ef** Proxy all-features tests定向typecheck通过（实际12m44s，详原result），
  现同锁同输入ELF构建/随后必要回归在串行队列。**它不覆盖8f初始快照或63e6 ACK修复**。

Source隔离audit普通前向组合至2a276131f93c6f89c2ba3ee9a9e7e39428463b57，未激活、
旧bb锁仅该隔离开发输入；d2d83 finite3361、d971bb70f实际finalLoanOwner观察、
5a2489d7e callee固定数组、2a276131f逐FlowUsers census均待完整Source实际回归。
其纯metadata/原control分支不冒充全Source失败恢复。

清除inactive旧Proxy本地缓存前逐文件记录摘要并排除当前/队列依赖，释放1,261,678,405B；
原始log/input/result/lock/产物摘要全部保留，不提交缓存/日志。REALITY原目标10:55仍
DNS errno-3，无目标/期限/断言/网络变更。所有域继续；同最终head CI/下载归档尚未取得。

## 2026-11-07 10:59 UTC：同 Packet/TUN 查询原件与新本地锁边

五域仍继续，不是最终候选。当前实现冻结 `fe96fee6347c6d49d758140e11e1e8c93f566544`，
Cargo/候选fixture已同为 `0c3a04f5d0a1ebb21f491dfac6b3e1ff993d4b9c3b8024678ec61d1974eefd82`。
实际 cargo metadata --offline 解析仅在 zero-grpc dependencies 增一行 zero-proxy；
没有第三方/协议版本变动，旧 bb 锁输入证据保持原摘要，不追认新锁。

- PacketRoutes/PacketRoute 三控制前端改借同原表；原 Arc 工作区预付后冻结状态和
  数值，原 owned query 留真实库角色。c60f20424 增可选 packet_route_max_routes，
  默认4096/省略不导出，实际 lease+response双归还前 Closing 继续计额；纯准备/
  最后发布同表复查与 rollback 恢复。新的真实原表/输出/拒绝/恢复回归尚待运行。
- A TunStatus 完整 producer/HTTP/IPC 已普通合入；M 配上可选 proxy-control 的实际
  gRPC Proxy源和Root启动。原 TunInfo/lastError/Egress metadata只读guard编码，不
  复制 DTO，不创造 TUN 关闭凭据；None/Some字段、atomic一次冻结、native gRPC
  预算拒绝/同连接恢复测试已注册，尚无运行资格。
- shared IPC query.rs 的3way组合曾由未启set-e的命令过早提交冲突标记，3e29055d2
  立即普通追加清除并保留Packet与Tun两组入口；该临时提交未构建/推送，不回退历史。
- 6085主线 Root check 自然结束 exit101/351.42s，仅 Nav options.rs 两处相对父类型
  位置错误；2bb78已窄修复。原日志SHA a49b6c73dfda64c9f69ef3a8162c46036449bc5733623e505d0217e8c70b2ded。
- Source audit5a947 check 进入 libtests 后6条已知诊断来自4个fixture缺陷。实际窄修已
  冻结，M 对精确rustc300559 SIGINT释放串行队列，1164.86s exit101；显式中断记录
  和原日志SHA e41409681605475f0c9457a609a803733864a6d8523f060bc3c46048ace48e7b 保留，
  不计完整类型检查资格。audit0d632加入实际fixture修复；b28279c93加入同cached原
  driver custody交接，逐项核验不分配临时Vec；仍未合入/启gate，完整prefix/多phase
  故障/Source原件责任继续实现。
- fe96首个 --locked 命令因验证worktree保留ignored旧bb锁0.1s拒绝，未编译。原日志
  SHA97923c18558dfad8ed668ebab2f7c4a5c7fd95a62762de8c115bb06073e69657；M核对旧新
  摘要后同步同0c3候选fixture，第二次串行Root+Engine/Grpc/Platform/Runtime/DNS检查中。

原REALITY目标www.cloudflare.com:443于10:55UTC只读解析仍errno-3，未改目标/期限/
断言或系统网络。其环境待验与本轮仍未完成实现分开；唯一TEMP原范围保持未关闭。
最新全部 ordinary Proxy/Root 回归、同候选CI和最终可下载归档仍待完成，不以局部结果
替代域关闭或本地验收候选。

## 2026-11-07 10:40 UTC：完整调用者续作与同锁开发证据

五域未完成，继续实现；没有最终候选、同 head CI 或可确认下载的最终归档。
远端 integration 仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`；本节实现输入
`dbcce472b8e16a8de8d1b427744bc53be5a285d0`。所有追加均普通提交/合并。
Rust1.99.0、实际 Cargo.lock 与仓库 candidate fixture 同为
`bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`。

- A 原 Target TCP-connect-only、Lookup/Trace 与 DnsResolutionCall 已接同查询/Probe Scope；
  typed cleanup 接收保留实际 Arc/原阻塞任务 ID，不改 ApiError wire 或 Rust 字段。
  HTTP/IPC/gRPC 接同 ApplyObservation 故障；没有 Apply TaskJoined 时不伪造 witness。
  c270 同冻产物 Probe2、Apply9、Engine event10/replay1/runtime5、gRPC22 PASS。
  DNS4 中一例 EDNS question fixture 误读保留失败；149257402 的同实际 DNS parser 修复后4 PASS。
- M SSE replay 原历史 Arc 在冻结/预付后借入同 Root workspace，历史删除也不提前退额度；
  原 standalone owned replay 留真实库职责。初始 flow snapshot/事件采样 DTO 仍待迁移。
- A TrafficStat/TrafficStats 从同管理锁/原 registry/实际配置代次冻结数值和借阅字段，
  HTTP/IPC/gRPC 已实际消费；不存在 generic query fallback。8e8 full Root check 的5个类型
  错误原日志保留，dbcce 已窄修复，等待最新检查，不追认成通过。
- B `8e8bc180e586a70972831a0465cad3f867a240bf` 完成同原 REALITY 非缓存导航 job 的
  actual Aux -> job 交接和所有已接 Nav 调用者；同 Root 原表保持 pending/failed/retiring
  job/request 额度，生产 navigation_max_jobs/requests 可配置、旧省略形状保留。
  这不关闭 QUIC、其余合法 mixed/Mieru/SOCKS/process 与 legacy crossing 的剩余实现。
- B 同 accepted recipe 的 VLESS PacketPathFacts 已作为唯一 cold 私有准备投影，不授予 IO。
  C 全 Source/Packet pair 仍在同环境隔离 worktree 类型检查，未合入/启 gate。
  latest audit `5a9471830fdbcb2ffd16b64df1e427dc6770896c` 已含原 wire装配、同 Source
  logical projection/Scope身份核对；有限多 phase fault、prefix/共享 donor 交接、all-peer
  与独立 WG provider 等仍是实现工作，不列为平台验收替代。

| 实际 checkout | 检查及结束 | 原始日志 SHA-256 |
| --- | --- | --- |
| 96c8044e64798d7a15666c71c094e5dbd90f7e10 | Root all-features bin/tests typecheck 391.62s exit0；未运行 Root tests | 26dd8d6dc5fc5be9cf1a14e72d3ff29fb179ddeb60a440e7ca184cb38caff274 |
| 96c8044e64798d7a15666c71c094e5dbd90f7e10 | 实际借用 QUIC packet driver 6 PASS；先前错误 selector 0选中另存，不计通过 | 0d5f931405751fd5ff6b5936d9c158d997bc43d5e3a59b7f0c3d75a54a319bef |
| c270e6f8c60d64ebedb91295ddf96f34b21816d2 | Runtime/DNS/Engine/gRPC 定向 all-features typecheck exit0 | c4a702dd74bae6eff8f1704d3613430a67ffd5ac910bfcb6eed386bdb0ecdeab |
| c270e6f8c60d64ebedb91295ddf96f34b21816d2 | gRPC22 PASS，含真实 raw blocking panic 原 Arc/实际 request Scope 回归 | ac3d1b872b6ef6284ca3acdc96030acd324177347c508dd3605680984ddd555d |
| 14925740254452e22d344f9a68e0da8ea30c4fd4 | DNS wire_call4 PASS，46.28s exit0 | 9130fa9ff7b9fe0688074057b659ee0231a036b2376930edd650eb625feacf49 |
| 8e8bc180e586a70972831a0465cad3f867a240bf | Root all-features bin/tests typecheck exit101，5个 Traffic 类型错误 | 89fa1cf331ca055d04ae4e8f599c4643dcce55acac957058e0420601d2707c3d |

普通 Proxy VMess/H3/DNS/fullTX、新 Probe/Target/TUN/Source 原件回归、最新 Root consumers
仍需新冻结源码产物。旧输入失败不由以上小包通过替代。全部 input/result/raw log、同锁、
ELF摘要和显式中断/未执行项保留在外部证据目录，最终须一并打包取得下载渠道。
TEMP-WG-OUTER-UDP-01 原范围独立未关闭；没有增加例外或更改系统网络。

## 2026-11-07 09:25 UTC：真实事件、借用原 driver 与隔离类型检查

五域仍未完成，不是候选冻结；源代码工作继续，所有 Cargo 仍由 M 串行。
生产改动已普通追加至 `96c8044e64798d7a15666c71c094e5dbd90f7e10`：

- C `d456a3d2b4187d73191d2316c307b3e0b0998379` 经 M `c1fc33995` 普通合入，
  实际 TUN 生产只剩 managed System TUN Source completion；旧独立 completion 的
  native/acquisition/任务执行字段与无调用者安装接口已删。原真实取消/通知/joins 测试保留
  在 cfg(test) LegacyCompletion。独立 zero_tun 库职责未删；完整 Source/Packet/TUN 仍继续。
- M `38b0bbbf4e545561d4b2e5c9a6ac2103bec63cd6`：实际 Resource 在原主任务故障之外
  保留同 cleanup receipt 的稳定原 Arc，以及 driver/external 两固定观测槽，先持久再 ACK；
  terminal 传递这些实际原错，Native/User/Scope/Host ACK 各自独立。新真实 TCP/文件 OS
  错误/原 Scope 恢复回归已注册，尚未运行；这三个槽不替代 C 未激活组合的各有限 controller 接收。
- M `fa4ffacc65c9f3ce84acc0bc55cc4e506a198085` 和可见性修正 `aef65dc02`：
  HTTP/IPC/gRPC 实时订阅保留 Engine 原队列项及同订阅字节责任直到计量/编码结束，
  不在出队时深拷贝历史仍持有的 payload。gRPC 先测量原字段，预付后才产生 protobuf 副本；
  生产选择同 Engine 订阅，standalone 库 API 原职责保留。重放/初始 snapshot 与其他 DTO
  仍未迁完；新增实际原队列额度/Prost/native 订阅回归尚待运行。
- M `4e7c5115ab4585fc489f17ec2670766c2f438bc1`：同一借用 datagram driver 的有限
  清理 continuation 不再被外层取消先 Drop；普通 spawn_driver/related donor 撤权不变。
  hop 排空已接受前缀，final 封原队列再排空，实际 in-flight send 保留；同 TaskJoined/失败
  接收、真实 Host ACK、adoption 清理分别处理。新真实 UDP/取消/最终两帧/延迟 ACK 已注册，
  尚未编测。B 未激活 QUIC 实际 Entry 同 cleanup fence/退出顺序正消费此提交。

实际失败均保留，Rust1.99.0/同 bb045365 锁：

| 冻结实际输入 | 命令/结束码 | 原始日志 SHA-256 |
| --- | --- | --- |
| `f22b5c728588ac273410615c82010443049bf93e` | Proxy+Root all-features lib/bin no-run，742.37s exit101；Root 两 fixture 接口与 IPC layout130，未产出本组测试资格，队列未执行 | 18891759e71b998a469ea13f6b60d112b6e2f3601999b2a101132f6c1a1cc2d0 |
| `fa4ffacc65c9f3ce84acc0bc55cc4e506a198085` | Root all-features bin/tests check exit101；RetainedEvent 旧 owned 转换可见性，aef 追加修正 | 详见原 input/result |
| `aef65dc023af24bbd741a09cfda8b7d80e36ff97` | 同 Root check，322.55s exit101；IPC inner Work layout130仍在，未提高 recursion limit/栈/容量 | c612f5eadb9a9ced0206e3a82342991d5460ca03b043227457be6de4dba25b30 |

`96c8044e6` 将 type erasure 移到同 admitted execute_ipc_command 边界，删除无效 outer
Work Box，未增任务/执行器；待复验。先前 VMess/H3/DNS/fullTX 和 Root 生产回归仍排队，
没有用不同输入旧产物补资格。

C 147 文件基础全片以精确 base38b/patch SHA
2d42a31faf1caf018fefac4d357c96aff883907927890cd81b9b9874fd256d5a
冻结，M 在同环境隔离 worktree 提交 `c0908efb24ae322290806bb5dafbae6b6117b2ea` 作
check-only，使用 M 唯一 bb045365 锁，不采用 C 旧参考锁。没有合入 integration 或激活。
真实 all-peer Source 装配/停旧、VLESS accepted recipe、各 finite receipt 仍是实现缺口；
该检查仅提供真实类型错误供继续实现，不把 foundation 编译作为完成。

开发 b658 控制证据已打包（298064 bytes，SHA-256
 e07233dd4105559804ac9bf94dd44c0c2f764f941bcb18ddd88827aa60c03a60），
Pages 上传返回 outcome_unknown 且无可确认 Files reference，未盲目重传、未声称可下载。
最终仍须取得真实可下载归档、精确同锁及同 head CI；当前均未获最终资格。

## 2026-11-07 09:00 UTC：接续原控制调用与实际 ACK

远端 integration 再核对仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`；
M 本地实现输入 `f22b5c728588ac273410615c82010443049bf93e`，工作树干净，普通追加，未回退/强推。
本节是开发证据索引，五域未完成，无最终候选、同 head CI 或最终下载归档。
工具链仍 Rust1.99.0，实际锁仍 bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb。

- Engine Config/Policy、Runtime/Flows/Stats、PrincipalFlows、Sinks 真实借阅视图已接
  HTTP/IPC/gRPC。冻结实际原表、标量与修订后先计量、预付 workspace/output，再序列化；
  归还 guard 后才 IO/退款。Traffic、事件重放/队列原件及其余生产者仍继续迁移，不能由此宣布控制预算完成。
- gRPC H2 HPACK 能缓存已结束 RPC 的错误 header。`8b9a9ea90` 将同一预付 response
  字节信用转交实际 HeaderValue/Bytes 原件，保留缓存存储额度，归还已结束 RPC 并发额度。
  不复制第二错误池，不以 Scope join/EOF 归还仍实际持有的 header 字节；原连接低额度拒绝后
  配置增长可恢复。该输入 Engine5/gRPC19 PASS，日志分别
  83fe587c448bc63c8a911f7e749c9d30e141f75f625028fa92a12ea7237c8015 /
  aec386b242d6e9f3350ade4c5fabd2e594f3fcee91e37460cafbf6a735fa7807。
- 原控制调用携其原 stop。Apply 接受前取消不建任务，接受后继续补偿/实际 ACK；
  coalesced Probe 仅最后调用者退出时取消同一真实 ProbeCall，控制调用保留原清理。
  HTTP/IPC/gRPC 使用同一观察元数据，未新增执行器。Target/DNS/Trace 与 typed cleanup
  故障接收仍在实现，不能把裸 blocking 完成或字符串错误当作原件交接。
- `a2df49dd4` 保留原 Probe 额度至同 Task 实际 join 且原 Hosted registration ACK；
  不以业务 body 完成替代 ACK。精确 `b6581ee8e1a3c42aace2b17ae5376cb128cfa616`
  同冻源码产物：Probe 原 ACK1 PASS、Apply9 PASS、Engine runtime_query5 PASS、gRPC19 PASS；
  日志 SHA 分别 afa6153beefc60917f88604ecf7eedc933472321bd28fdecd353bd5f908baa77、
  4910d54ddf53b82c4ecf902b9c27d658f856b4118ca9e6d9a3f9d86b57516264、
  83fe587c448bc63c8a911f7e749c9d30e141f75f625028fa92a12ea7237c8015、
  ab29cfbbfd609c311d1aa13ef6b3f88415a079dcd59347dc28e63280fd7f25c3。

保留失败：`a3abaa0eddcd49309283c396ce41f0cba29904f4` Root IPC 无完整结束摘要，
原队列 fixture 与接受后原调用必须清理的语义冲突导致挂起，由 M 对精确进程 SIGINT；
中断记录及原日志 abbfe171d6713f5d013159b5f2cbeb862c9c5eabace33d32eb1b9d2d83152171 保留。
同输入 HTTP/SSE16 PASS/1 FAIL（原2s），fixture预付未含实际事件身份/修订，日志
5dae2096ab9f41acff44b6c1a2f51355d8ebc88133487695b3d733015f69ba77；
实际 retirement1 PASS、startup/TLS5 PASS。后续实际 stop 接线及 fixture 修复已普通合入，
没有放宽期限/容量/断言，也不把后续修复追认成此输入通过。

现串行编译 f22b 的 Proxy/Root lib+bin 测试产物，随后跑实际 coalesced stop、DNS normal
profile、普通 VMess/H3、fullTX 和 Root 控制。A/B/C 源码并行，不能同时 Cargo。
B 完整 Nav/QUIC、合法混合/Mieru/SOCKS/process/实际字节额度，以及 C 完整 Source/TUN/
独立 WG provider 外部草案均未激活，不以准备接口/小子集宣布完成。TEMP 原范围独立保留。

## 2026-11-07 07:30 UTC：控制实际视图与原 H2 故障

本节是开发输入索引，五域仍未完成；没有最终候选、同 head CI 或最终下载归档。
同锁仍为 bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb。

- `4f8270cf188710685d36678c01833c6e66d3d74b` 根目标 all-features bin/tests check
  exit0/373.85s，日志 e82ad2ddbcf3273dc30096341f3b6ab7a5303145e1cfa60066dd2f77768dea57；
  仅类型检查，尚未覆盖后来的 Engine 借阅生产消费者。
- `81f6d2c874fe4d21c8a9371d2c4651224c5e4270` Engine/Grpc/Transport no-run
  exit0/4.26s，日志 6a1e3617f5cf08d67734953c6973ea0b0872808015e2b0894341389105566a8e。
  同产物 Engine borrowed config/policy/generation 3 PASS（a448cfca747f6f24f01280e94a4ab43e1bb766b683b3e1ef489d64a0bdf3bf4b），
  gRPC19 PASS（0b0dcb8be63ebcc2bf3629e14958de1ac2361f8b5069800f7ebb5457fde91660）；
  Transport gRPC42 PASS/2 FAIL，实际 GOAWAY 错误及 keepalive 原超时类型不符，
  日志5a0dededfa9a3424ac2d0f13a9270af622ee87137e6ad49cea6954c807f8c2b6。
- `0b0b54c3084bdc633fc84e3c46a5bc03c3d3a2ff` gRPC19 PASS，9.89s含build，
  日志a36e1e2b1b8bd355e37531a629097accf3da09d22e0ff319479848ebb17e1941。
  实际错误状态存储在格式化前预付，真实 quota Pending 后返还，header clone 在同 Scope
  join 后仍持额度；请求、原物理 close 和输出存储分别验。
- `70915945f5d0a9851b98291e27e8b125932a2f28` Transport gRPC44 PASS，44.75s含build，
  日志f06b9d11b8d1b32a7afd5cc4c43b47a48199a30879f8c789dc9f2f62b4f7de97。
  同原 Flushed 的第一物理故障传递给原 read/write，连接错误不丢已解码 worker/tail；
  新真实 GOAWAY、旧 keepalive 原类型均通过。普通 VMess 尚待下一 Proxy 输入复验。

前三份编译失败分别保留：0ae692 gRPC fixture 缺 protobuf method；756ed Engine 测试
需借阅传参；3ca3 Engine 测试需先归还原 view。修复均追加提交，没有改目标/断言/期限。
`a3abaa0eddcd49309283c396ce41f0cba29904f4` 已排实际 root bin 测试产物构建，尚无结果。
该输入包含 A exact DNS normal profile cancellation d571e5f 与后续注释修正；预算/CA/material
仍 active Busy。原 DNS 查询、Native/Scope/新 bootstrap 断言未改，尚未运行新 Proxy 资格。

当前余项：A完整DNS detour/删除及 Runtime/Flows 等 DTO；B 全合法出站、QUIC/Nav、
无observer/SOCKS/process、同Host队列字节配置；C完整Packet/Source/retainer/preflight/TUN；
M共享接线/G8和剩余总额度。C外部完整草案未激活，B→C cold formatter v2待完整消费者。
真实 GUI、特权 TUN、跨平台和1c1g另验，TEMP仍仅原outer_udp_proxy/重连历史。

## 2026-11-07 续作：7fff 实际 Proxy、f1e 控制与 4f827 清理

本次再次核对 origin 为 `https://github.com/zerodenet/core.git`；远端 integration
仍是 `59615e69ea5a413d657a873157ea4adf8f204a90`。无 reset、变基或强推。
下面均为开发定向，五个完整职责域仍未完成，无统一最终候选/同 head CI/最终下载归档。

实际验证锁均为 `bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`，
Rust 1.99.0。命令、逐文件输入、工具、环境、原始日志及产物摘要保留在对应 harness input/result。

| 实际 checkout | 命令/结果 | 原始日志 SHA-256 |
| --- | --- | --- |
| `7fff412cabefc7e072b244ddf27e88af3692dc66` | Proxy all-features lib --no-run exit0，746.64s，jobs1；产物 c69edfbc43d71082a8f6f1b4d8258798b87ccf169eeaf30bd8c48afa93ad2426 | a49ef1f1bd2e32f8c30abf1b498f57567330ec74d52d34566aa66faa34fe339b |
| 同7fff，同一产物 | H3 0/1 FAIL，实际 Task19 remote 与 HY2 native 原端点一致，非 web peer | bb071236f3e0185392537e8807348d5c30161e51ca510159e664f1a725fcb7f8 |
| 同7fff，同一产物 | VLESS 6 PASS，含原 owner MUX material 失败恢复 | c203f40eabf70da678836a16efc38376aaaef4dc9ac993e730cb145f214c4cdf |
| 同7fff，同一产物 | VMess 2 PASS/3 FAIL；真实 raw TLS tail 和 MUX 通过；gRPC/relay 仍正确前缀261120/262144后 Receive Io | a98bf855be665316708063dd48a8861664a7fd344eb11e77a3820bae79a6599d |
| 同7fff，同一产物 | DNS49 PASS/1 FAIL，normal profile replacement 已返回实际query/blocking仍live；原query未退出 | b0bed239ffa30d73c86813f9b9887771337a5b2fafc4a9afa4b3118cd0c60052 |
| 同7fff，同一产物 | fullTX 0/1 FAIL，DuplicateInboundListen port0，未触达WG ACK；fixture修复f693已普通合入，不能据此声称ACK已验 | 1cd7d5dce31e4c70cc3a7fca77074699f83d40c09afaeba1764d32b6b6ab5f2f |
| 同7fff，同一产物 | Packet ordinary budget/input-only 1 PASS | aeff5c465cd1938b2fe603e43f209f3c7f6661fbf9a84293434978886314e5e6 |
| `f1e8d72fa06b0258ec37d6cb90c2d051a72b48b3` | gRPC all-features lib 17 PASS，含真实空闲断订阅worker回收、锁外退款、同Apply原注册观察 | 3fc206e5662976e65a1365706c9183ddb2aec6f0320f619e6e8e0b59ac3d49d1 |
| `4f8270cf188710685d36678c01833c6e66d3d74b` | transport all-features lib packet group 4 PASS，真实Scope取消后原FIFO data/close按序送达、actualjoin/issuerreceipt和Nativeclose分别确认 | 0bf940480638d64186ea718a296069a8238b9c03a50922ac9e336019b48f024f |

Proxy前两次构建均中断：实际僵尸wait_status=9，无成功结果/资格；未确认信号来源，
不能归因为OOM。v3 detached原命令成功；没有提高生产容量、栈、测试期限或弱化断言。
`4f8270c` 保留同清理Aux直到最终endpoint实际users/drivers退出后封住原FIFO并排空；
UDP hop用原队列有限前缀，非第二池。仅transport定向已验，普通H3尚未重验。

当前完整调用者仍按下表及 execution.md 冻结：A DNS资源/设备detour与旧链删除；
B全部合法transport/MUX/Mieru、动态BND双原件、无observer custody、process和计量；
C完整Packet Source/预付/设备/TUN/独立WG provider与helper；控制全部DTO及G8恢复；
实际各owner生产总额度。A现复用control角色，B/C各自继续完整域，M唯一共享写入与排队。
TEMP-WG-OUTER-UDP-01单独保留，其余实现缺口均未改列平台待验或新增豁免。

# 第二轮续作：剩余生产调用者冻结

## 接续检查点 2026-11-07 06:49 UTC（五域继续实施）

M普通集成源码为 `2aa63ed41d9705bfc94944ccb9e1cc37e8648124`。同一次实际Apply观察前置
`eaa2742df`与A完整HTTP/IPC消费者 `b35780bcbce7183329f339fe34b9e54601279c2f`已成对接入；
M `7fff412cabefc7e072b244ddf27e88af3692dc66`接通gRPC同原记录，保原15s wire超时，之后
由原任务继续取得该次真实TaskJoined及同原错误，而非wait_idle借另一任务。无实际run
有明确NoAcceptedWork；原blocking closure只携观察元数据，不另造执行任务或宿主。
后续 `2aa63ed41`将gRPC实际连接退款通知移出custody锁并加强原真accept/close回归，未编测。

精确 `7fff412cabefc7e072b244ddf27e88af3692dc66`、Rust1.99.0、同bb045365锁：六包 lib及
vmess_contracts no-run exit0/99.15s，日志SHA `a82516b4f9739c0874ca5d9225e26c30ee516acec907725341c233fad84a3393`。
同产物定向运行见 `m-7fff-serial-regressions.json`：SS固定workspace4项（其中一项实际遍历
全部现有cipher/地址/负载与安全状态）、HY2 1、Mieru 1、VMess TCP原framing/半关闭/真实
Pending取消14、gRPC真实原件/事件/同Apply16、ApplyOwner原4加新4共8，均选中且通过，
源码及实际Cargo.lock不变。这不是全部普通调用者、HTTP/IPC运行或五域完成资格。

此前d995小编译exit101/20.72s为两个新真实IO fixture缺原AsyncSocket的Sync约束，原始
日志SHA `c87f1ac89db237d3ab2c90243cda0529028b6fb6349f004eaccf794002ca3c2d`；仅fixture
补界后在上述7fff取证，生产接口未收窄。SS/Mieru更早失败继续保留。

7fff普通Proxy构建首次会话消失，原wrapper没有result JSON；日志停于编译、没有资格。
关联同启动时间残留Cargo256561/rustc257028的真实kernel wait_status均9（SIGKILL/-9），
M未发送终止信号；cgroup memory峰近16GiB、oom_kill0，不能据此确认OOM原因。原始
`m-proxy-vmess-material-recovery-dns-h3-and-full-transmit-artifacts-interruption.json`保存冻结
输入、日志SHA `d93e7438ee027284a6da3f02f0052c70b52ae70bba54b329645400b234997b74`、真实
wait status及环境快照。确认无live Cargo/rustc后，同源重跑仅将CARGO_BUILD_JOBS降到1，
并每10秒追加原始进程/内存采样；生产配置、测试栈、断言、期限和协议目标均未变，当前
仍在构建。后续不得用首批不完整产物冒充这个输入的普通生产资格。

必要前置已经接入不等于Kernel消费者完成：C完整PacketCaller/cold Source/RX全层预付/
多peer声明/reconnect仍未激活；B完整QUIC/Nav、其余出站、process与transport实际字节额度
仍在实现；A DNS完整donor/旧链删除、控制Engine DTO首次物化前预算及剩余投影继续推进。
当前无统一最终候选、同head最终CI或最终下载归档；TEMP原范围不变。


## 接续检查点 2026-11-07 06:05 UTC（未冻结候选）

实际 remote integration 再核仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`，M 普通接续实现
至 `29468204555ec17457b994cf603f07882bb8ac66`。A/B/C仍在同环境原worktree实施；无reset、
覆盖dirty、强推、develop/main合入或部署。全部下列输入仍用Rust 1.99.0及同一实际锁
`bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`。

| 精确开发输入 | 实际结果与边界 |
| --- | --- |
| `37dc7bb7a41f5ba5c7446a59e5cf4ae4375e28dc` | 普通Proxy all-features lib no-run exit0/759.35s，日志 SHA `d9522a76a529b564954860274f69d7fc28a7ab5638f00138c5101a78fd8a78b5`。同产物6组见 `m-37d-serial-regressions.json`：Direct/VLESS5通过1失败、VMess4失败、H3 1失败；Trojan真实wire拒绝1通过、input-only Packet预算1通过；DNS49通过1失败。Direct五项错误及Packet修复已获该输入普通回归，仍不表示全域完成 |
| `c41ea296f8fd470312ea20b2e8774dbddebbabe2` | grpc/runtime/config lib/config_contracts no-run exit0/15.81s，日志 SHA `bb01f77025d2315ebf5c05f376e33539e2fe88565eb82a6f9f5b746582416362`；同产物gRPC14、Root控制账8、13个可选控制字段边界1均通过，实际选中且源码/锁不变。原生TCP RPC/断开后原命令future/Bytes克隆切片跨实际join仍持额已覆盖；真正Apply 15s后同原operation join及Engine DTO物化仍未接齐 |
| 同上 `c41ea296f…` | `cargo check --locked -p zero --all-features --bin zero --tests` exit0/350.76s，日志 SHA `44c30523e64b89567870ed4fc8d1be6641d86a1c5250ebbda27706928084266b`。仅为实际全部控制消费者的类型检查，不作为运行回归或最终资格 |
| `b21b7902d94d7ad8fb7875f3e95f151b6b1db192` | 四个codec/gRPC包 no-run exit101/23.29s，日志 SHA `c1effa80805d9b55d0bcd46a75a7e8d884519992f3fb1a9dcefb345dce46fd8f`，SS原AES helper可见性5处lib/7处libtest错误；仅扩到crate原helper已由ff9f真实修正，无crypto判断变化 |
| `29468204555ec17457b994cf603f07882bb8ac66` | 同四包 no-run exit101/8.37s，日志 SHA `3cc396a1516ff9d74293134265941bd9408183924f7a38e02658da71df2a1013`。Mieru实际trait impl移入固定workspace子模块后命中单泛型Result别名，7处编译错误；B机械修待同完整半关闭后续合入。失败不改称通过 |

H3当前真实事实是远端Source running、一个未joined实际peer且stop=false；尚未把它与web/
HY2原endpoint对上，新增只读地址关联待同输入复验。Scope取消先撤Packet wire及retire
直接丢已入队close帧是代码风险候选，未凭静态先后宣布根因。MUX恢复失败实际处于cold/
instanceNone，没有新claim；`bb3c727d9`+B `0169bda7…`先安装同原CarrierOwner registration，
再进行可失败材料准备，材料成功后才原start/Ready/claim。保留完整badCA原错误及原wire
断言，尚待普通生产复验。

DNS唯一剩余回归维持原『取消旧Query、H2/node原件join、再新bootstrap』，未改成让旧
Query先成功或放宽期限。普通DNS配置换代与预算缩额不驱逐active业务的语义分别核查；
新增044诊断只读取原replacing future的已ready结果，不加wait或执行新的替换。
C新增真实cap4满TX TCP ACK/SYNACK原wire观测，待必要Proxy产物，不根据既有握手测试
宣称Data ACK满额可运行。完整PacketCaller/cold Source/多peer memory/reconnect仍未激活。

既有DatagramCodec的固定slice前置与SS全cipher、HY2/Mieru真实实现已成对接入，供C现有
flat路径的全部层一次预付原输入/中间/输出及实际Scope转交；当前Kernel旧消费者尚未切换，
不把standalone方法或默认Unreserved当生产新分支。必要B→C前置为M `bb3c727d9`及B
`0169bda75913d410bd71a4112aeac38c0438a854`；完整旧链删除仍待实际调用者迁完。

初轮gRPC no-run还保留9e79的19处拆分/测试接线编译错误exit101/73.66s（日志SHA
`0e59236e592a0196c04fd4cf4e23c5543a6e5c9db65544359877e114e09b28b8`），c543的兄弟模块
路径错误exit101/7.53s（`c1f21dbf601e3626501e999e458010a9b7bf1846fed4f59ee44d8181c9082bc5`）；
c41修正后保全部断言实际通过。未发送中断信号，不能把曾尝试查找已退出Cargo算中断。
更早d0a中断后下一build启动时有短暂旧rustc存活，随即复核退出；其队列操作失误保留，
不冒充完全无重叠。后续各轮均核对实际后代退出再切verify输入；冻结输入完整性独立记录。

05:37原REALITY目标www.cloudflare.com:443仍实际getaddrinfo errno -3，原始
`reality-dns-20261107-053754.json` SHA `caad2c2b3aa8f0667839aed42ced20ba62255de98fbc14fb9fca4a7ba308657e`。
目标、断言、期限、网络策略与用户系统网络均未改。环境DNS待恢复与内核缺口分别保留。
未运行的后续/跨平台/GUI/特权TUN/1c1g以及统一最终候选CI均未取得资格；最终下载归档
仍须包含同一candidate.Cargo.lock、精确源码输入、原始日志、结束码及调用者/删除/依赖表。
五域继续实施，唯一TEMP-WG-OUTER-UDP-01原范围未关闭、不扩大。


## 接续检查点 2026-11-07 05:23 UTC（继续实现，五域未完成）

实际 remote 仍为 `https://github.com/zerodenet/core.git`，远端 integration 完整 SHA 仍为
`59615e69ea5a413d657a873157ea4adf8f204a90`。普通接续源码至
`37dc7bb7a41f5ba5c7446a59e5cf4ae4375e28dc`，未 reset/rebase/强推或覆盖 dirty。
M 的 gRPC 原请求/原编码字节/最终命令 ACK 工作仍在工作树，未进入此提交或测试资格。
A/B/C 同环境 worktree 持续实现，Cargo 与对应测试仍由 M 单队列调度。

本批实际工具链 Rust 1.99.0；实际使用 Cargo.lock 与版控 candidate 锁均为 SHA-256
`bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`，未升级依赖。
下表是不同冻结源码的开发回归，不合并为最终候选资格或职责域完成率。

| 精确输入 | 实际结果、失败与证据索引 |
| --- | --- |
| `10983d96cf9dca76cbcadba42ff4a1ecb1c90ef5` | 四包 all-features lib/web no-run exit0/729.54s，日志 SHA `93bc58075fc5004416cbeee73d28a67763f308b95c1b4548a9ab46fbda23887c`。`m-current5-serial-regressions.json` 保存同产物24组。Service UDP关联3、Target5、借入Packet1、旧Packet3、SS codec1及process5、Metadata1、Target普通2、Source4、Direct budget1、native flush5、gRPC43、QUIC4、DoH1、echo1、TX3通过。H2 7通过1失败（原8秒仅227991字节）；H3在真实业务及client关闭后远端peer idle仍失败；Direct/VLESS4通过2失败；VMess4失败；Trojan1失败；Packet budget1失败；DNS44通过6失败。所有原期限/容量保留，失败继续实施，旧Metadata CloseIo原因仍未证实 |
| `17c8b9e0c2772fd0ae03272cb844ba2350a7f8b5` | API/stack lib/api_contracts no-run exit0/4.93s，日志 SHA `0ce2aa8a1fa9c3bd7641db91c5172421974041805febb4b576588cb7b77c93e1`。借用Query wire契约22及真实pending Packet独立内存2通过。首次错误target exit101保留，未作为通过 |
| `6bf039945e69305b0b1b68c7b2ed3cf2ee4beeb4` | runtime/config/platform/grpc lib/config_contracts no-run exit0/97.17s，日志 SHA `c49c88fac217278c195aa4000cc3aeadf7a78dd0e688dbe443dcf9edcc040fcd`。`m-6bf-serial-regressions.json`：真实Direct Service转交/held ACK1、实际Root byte/local/command账6、12个可选配置边界1、既有预算边界4、gRPC既有生命周期10均通过，全部选中且输入不变。首次两处测试编译错误 exit101保留，修正后未减弱断言 |

新批实际源码修复包括：真实多层 UDP logical/wire association；Direct pending ledger 只在
实际选定调用者图保留，Direct Service 复用原准备任务；input-only Source 独立保留一个实际
crypto carrier、零Stack owner；Query借用Serde避免隐藏DOM副本；SSE同原Connection frame
账及IPC/HTTP真实输入/输出账。真实Scope/Native/ACK仍分别取证。

已接入的 H3/MUX 诊断仅记录实际原件、代次及任务状态，不添加关闭凭据、等待或状态变更。
Trojan旧测试等待未进入记录的inner source文字，现改为同一真实完成流的完整typed Display、
目标、网络、stage及code；保留空包未上wire、后续业务和全部期限。二者待新冻结输入验证。
`37dc7bb7a41f5ba5c7446a59e5cf4ae4375e28dc` 的普通Proxy定向产物正在单队列构建，
不是已通过的结果。

本批编译失败/中断均留原日志：5d的E0034、722的private方法、071的模块路径、05a及6ae
的SS测试接线、d0a已知fatal后显式SIGINT，以及91b的Direct测试接线。d0a exit -2/382.07s
是中断而非通过；下一轮已核对其编译子进程退出，后续独立新产物不继承旧资格。

五域剩余：A完整DNS旧执行链删除依赖B/C真实donor；B全transport/MUX/Mieru/QUIC/Nav/
process及无observer custody；C完整PacketCaller/source/reconnect、codec预付、System TUN/
独立WG provider和helper转交；控制最终ACK、Engine DTO预分配、blocking实际Scope与恢复；
预算的全部实际生产投影。此清单继续按下文冻结调用者推进，不以局部接口或PR完成结题。
只保留原范围 TEMP-WG-OUTER-UDP-01，未将其他缺口列为临时例外或平台待验。

仍无统一最终候选、该候选同head CI或最终可下载原始归档。不同输入通过结果、Windows
GNU平台typecheck、云端路径不能代替最终资格。原REALITY目标DNS失败保持环境待验；
不变更目标、断言、期限或用户系统网络。真实GUI/面板同产物、特权TUN、跨平台及1c1g按
适用环境另验；内核实现缺口继续独立列明。



## 接续检查点 2026-11-07 03:41 UTC（开发中，五域未结题）

remote 再核仍为 `https://github.com/zerodenet/core.git`，远端 integration 完整 SHA
`59615e69ea5a413d657a873157ea4adf8f204a90`。本地普通接续至
`887c4ed4ce511dc41ae1cfd10e08b230c47d2d5a`；未回退、覆盖 dirty 或强推。
Rust 1.99.0，实际 Cargo.lock 与版控 candidate 锁摘要继续为
`bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`。

| 冻结输入 | 实际定向结果与边界 |
| --- | --- |
| `09900efb95289070874969cb3c742b37e202ac23` | 七包 all-features lib/web/config no-run exit0/780.31s；原始日志 SHA-256 `05f874532329d0252c4d5ae3a6bde353f756596f77311f375b9ff88f3197225d`。同产物23组见 `m-099-serial-regressions.json`：真实 accepted local/global held ACK、gRPC43、native flush4、Packet memory12、WG security4、Mieru压力、DoH canonical恢复等通过。H2原8秒、Source预算/cold VMess材料、Metadata/H3换代、Direct预算、部分VLESS/全部VMess/Trojan材料、Packet及WG warm退出仍有失败，原始记录完整保留。后续修复不继承此输入资格 |
| `383d9285ba70923d59ba1357f257c56a6182d469` | runtime/config/gRPC/stack及config_contracts no-run exit0/71.34s；日志 SHA-256 `a16a366953ef3f2eac9ff12fbdb52f26c35f0f6e088095740d5d00620b79d496`。实际 ControlBudget3、可选配置4、真实accept取消/同步重入3、gRPC控制10通过。首次错误runtime filter选中0项的原记录单列，不当通过 |
| `19b1e1fd1d87fada2db3143d87651ea1d6ae2a55` | 三协议/transport no-run exit0/13.88s；日志 SHA-256 `c3f53f91913708e29301e0b34ae8549383ca53657d4873945629f9f33fced17c`。同产物8项真实FIFO满队列、取消后DATA→END、最后strong writer归还后END→EOF通过，均保持原8秒。记录 `m-19b-serial-regressions.json`；尚未替代普通生产材料资格。首轮fixture同名方法编译exit101保留 |
| `d648ce90d752f607cf61b559251044eda3615280` | 平台/transport no-run exit0/30.39s；日志 SHA-256 `02e22ac02193e50f61a7521bc5835a50709585e6545d207abcf7e82e2ea29a65`。既有Packet实际队列/metadata/native-close3通过，新Service UDP2项1通过1失败（真实重转交association未映射），新借入driver1失败（测试未配置source_ip，native本地通配地址与真实wire不等；887c仅补实际source且保精确断言，尚待复验）。失败不改为通过 |
| `97e55fdc406754b0d00a6417a2700a7074af94ab` | Windows GNU平台provider全feature/tests typecheck exit0/10.04s，日志 SHA-256 `75da339dd3f7457e226b472c0fdc45e6ca68a88b3b25a83581ffd46a9e448fa4`。更广Root typecheck因缺交叉C编译器exit101；安装无权限exit100均保留。不构成真实Windows运行资格 |

已接但仍待新输入普通生产回归：真实Source预算缩额不驱逐活跃业务、DNS复用实际Direct
ServiceRoot双入口、Source metadata地址cell及VMess TLS夹具、真实Stack accept不持锁跨等待、
Windows Pipe原工作区及原worker/Host ACK后退款、MUX原队列背压和取消END债务。
`57e099b17` 将现VLESS fallback/HY2 proxy masquerade的同一Target候选图接入现Root，
单独保存真实native/view/原actor join/Host ACK；删对应旧裸prepare/controller列表。
每图同Source socket额度且真实HappyEyeballs并行保留；尚未取得运行资格。

M `d648ce90d` 公共Service Datagram为真实非空prefix借入专用，无root native/cache授权；
借入段使用既有Packet队列、实际同Factory Scope单Aux，持有原LoanOwner，等真实join及ACK
后归还原队列。B正在接实际QUIC producer和同一个Entry额度；不能以公共接口完成结题。
A继续全DNS donor集合/重转交ID修复/旧链删除，B继续全部transport/navigation/池退休，
C继续实际PacketDriver caller、同Root/Scope完整WG/TUN/独立UDP及helper责任。

全部原始输入、日志、命令、结束码、工具版本和产物摘要留在Git外；尚无统一最终候选、
同head CI或确认可下载的最终归档，不提供本地验收资格。REALITY原目标DNS探针
2026-11-07T02:37:26UTC仍为errno -3，原始探针SHA-256
`bd17fa73f77749da1191575cd070d1459dce50bad905c8a3579f298af0f92415`。
原目标/断言/期限不变。Darwin whole-job provider/authority缺口与真实跨平台/TUN/GUI/1c1g
环境待验分别记录；唯一TEMP仅原outer_udp_proxy/reconnect范围未关闭。

## 接续检查点 2026-11-07 02:35 UTC（开发中，五域未结题）

remote 仍为 `https://github.com/zerodenet/core.git`，实际远端 integration
`59615e69ea5a413d657a873157ea4adf8f204a90`。仅普通追加与合入，无回退/强推。
本轮实现已接续至 `e34899b42`（完整提交见 Git），实际验证锁仍为
`bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`。

| 同输入原始记录 | 实际结果与继续项 |
| --- | --- |
| `5c5da4fee0f5d189b13be3a941b58f85eb86bdb4` 七包 all-features lib/web no-run | exit0/755.92s，日志 SHA-256 `de912f796413a8e789584526335b40fd6986bdf1b9ff3ad2037495395483cf63`。同产物15组逐项保存于 `m-5c5-serial-regressions.json`：Packet预算及5项linked WG在初始apply失败，原因为真实Source内存来自第二Root；3项standalone WG通过。DoH canonical故障恢复、完整TX预付3、拥有Echo1、MUX同池cold1、真实前缀原件1、Runtime网络3、WG安全4、gRPC原2MiB drain1通过。VLESS材料3通过/2失败（差1024字节），VMess材料4失败（截尾/reset/恢复Revoked），Trojan1失败（初始invalidpacket事件），Metadata StopOld1失败，入站工作预算2失败，均保留原始错误 |
| `d9bebad3502d246f2f38b095b67046fbfa1f9c3e` | Packet三包实际库：WG10通过/2忽略，stack47通过/1忽略，TUN57通过/1失败/2忽略。TUN失败为辅助FD数字被并行复用；真实receipt/同错误Arc断言保留，18c4修为匹配原件receipt并辅助同真实UnixStream peer，尚待复验。QUIC真实共享用户等待7通过。H2原8项7通过/1失败，原8秒phase1仅230890字节 |
| `12ce6f984e992c0f3032e412c95ffb7f4fb75d0b` | 严格同原件flush epoch no-run exit0/24.92s；真实flush挂起、旧Pending不能满足新DATA、任务body返回不能充当ACK共4通过。gRPC完整43项41通过/2失败（旧fresh-file identity断言、Elapsed包装链断言，继续核验，不删断言）。H2原8项7通过/1失败，原8秒phase1仅236596字节。实际native close/Scope/Host ACK与flush严格分开 |

已追加但未取得普通生产新输入资格的修复：`e3cbca492`/C `b2e39a4b`
修实际注册次序，将唯一Kernel Root注入已安装WG adapter，并删除生产get_or_init第二Root；
`df9263254` 将实际charged Source集合传入原ListenerPlan强制StopOld，禁止复用绕过；
`1570d9638` 保同实际SOCKS/SS listener graph的可配置工作额度，并以真实Mieru payload/
目标尚未accept/旧TCP连续性验证压力。新全部9项Source原件额度schema `63a692a65`
与None默认修复 `e4b5818a6` 已冻结，完整producer/等待/ACK/Target图仍由A续作，
不因schema存在结题。`58a411472` 将Direct原4096 socket/256任务上限接入可选生产配置、
实际cold identity/普通登记与换代回归；尚待新输入编译运行。

B→M `60ec99035` 加私有 `692692890` 已普通接入实际Direct前缀，复用同原件/
原TaskScope Auxiliary，Node DNS与原候选scheduler，失败逐原件关闭和真实join/ACK
后才重试/交付；有限无observer custody仍在同Host保留。literal/Domain已有真实wire
回归源码，未以接口或冷登记代替运行资格。B继续全部剩余transport/REALITY navigation
实际借入责任和原件退休。C继续实际PacketDriver caller/有限pre-delivery ledger、
TEMP原worker任务责任、独立WG UDP、macOS helper/SCM_RIGHTS/实际exit；不扩大唯一TEMP。
DNS完整调用者删除片仍等待B/C真正donor集合，不提前删旧connector造成合法能力丢失。

同输入验证保留实际SHA/逐文件摘要/工具链Rust1.99.0/相同锁/命令/结束码/原始日志/
逐target/产物摘要；旧通过数不是迁移比例。尚无统一最终候选CI或已确认的最终下载归档，
不能作为本地验收候选交付。REALITY原目标DNS仍须恢复后按原目标/期限复验。
Darwin任意SIP whole-job provider/权限实现缺口与真实平台/TUN/GUI/1c1g环境待验分列。
其他可做内核实现继续，不合develop/main、不发布部署。



## 接续检查点 2026-11-07 01:25 UTC（开发中）

远端 integration 完整 SHA 再核仍为
`59615e69ea5a413d657a873157ea4adf8f204a90`，remote/工作树普通接续。
五域仍须逐实际调用者收尾，下面结果不换算迁移完成率。

| 冻结输入与实际锁 | 真实必要定向结果 / 未关闭问题 |
| --- | --- |
| `04f6298980e64f818afd381acfe0f346104e7e34` / `402d67cc…` | 八包 all-features lib no-run exit0 / 773.39s，日志 `m-complete-halfclose-node-relay-dns-custody-and-tun-recovery-build.log` SHA-256 `b0f6c55dc9f969c4abba79ce2a8664689029b7865a1cec8373448a8c8257c7ad`。Runtime完整cold restart 3 PASS；VLESS材料3 PASS/2 FAIL；VMess材料及实际Node/Relay 4 FAIL；Trojan完整MUX 1 FAIL；Packet预算、WG warm预算、DoH恢复各1 FAIL。完整逐项/原始错误保留于 `m-04f-serial-regressions.json` 及各 input/result/log，不借单独协议 halfclose 通过替代生产装配资格 |
| `707f613f`（完整 SHA 在 input） / `402d67cc…` | 同原H2 Connection outbound turn no-run exit0 / 23.09s。web `h2` 筛选实际8项：7 PASS/1 FAIL；原1字节窗口/256KiB/8秒用例 phase1仅233617字节，继续保留实现问题；另 transport lib ALPS 1 PASS。初次 e1c 构建缺Future导入已最小修，不抹去初次 exit101 |
| `1eefea32`（完整 SHA 在 input） / `bb045365…` | Packet中性三包 all-features lib no-run exit0 / 0.62s。原Root/read/TX额度11 PASS，真实TCP锁释放后同步重入1 PASS，预付Echo原件1 PASS；真实WG握手/安全回放2 PASS、多peer回退1 FAIL（原控制buffer被再次封装的Unreserved），尚待修复。首次和第二次编译错误均保留；普通VMess AEAD实际1024缓冲双向256KiB/原8秒回归1 PASS，但生产rawTLS/完整MUX截尾尚未关闭 |

现源码新增的未资格化修复包括：d1b66cef先回收真实Source空闲载体视图、再检查同Catalog实际用户，Packet Main将长生命selection元数据与真实活动分开且保留其他stack/native/queue栅栏；47e14431让待发布Source持有确切目标snapshot；DoH原提交token夹具341/1fc；三协议标准MUX Datagram角色7552；Packet逐包未提交typed拒绝保持同association；VMess三个真实role失效边界诊断9f。尚须用新同输入普通装配回归验证，不把04f结果归到这些新提交。

Metadata/QUIC五个可选生产字段已由实际DoQ、VLESS、HY2 Source/共享池消费（1cfa5709），协议私有配置仅在适配层判定；其余Source任务/资源/请求和H3队列仍继续。Packet中性Root/traits/SDK/stack先整体接续，完整Source/TUN TX生产消费及8项实际Root policy事务仍由C实现，未因中性切片通过而开启缺原件的路径。

新锁文件 SHA-256：
`bb045365baf88f40277e4b87e2941e4990eb630841dd1cd2b2d0de3709d0b0fb`。
它来自已有实际锁的offline metadata最小更新，只增加 gotatun→zero-traits、
wireguard→zero-tun、zero-stack→zero-tun三个本地依赖边；包版本新增/删除均0。
与manifest匹配的实际字节和旧锁均在Git外按摘要保存；版控candidate锁同步。
旧402/9026验证严格按各自锁保留。锁转换操作不是运行资格。

CI结果归档脚本已在本地模拟验证原始字节、实际源码及used-lock不匹配拒绝等边界，
但尚无统一最终同head CI，也未确认云端原始归档可下载；不伪造artifact链接。
REALITY原目标 `www.cloudflare.com:443` 于2026-11-07T00:51:53UTC仍返回
getaddrinfo errno -3，原始探针SHA-256
`1667a36f4a0282944dd7496d0c0bb95c7740edca3dee8bcc5f0bc186096a01b3`。
不换目标/解析器/期限。唯一TEMP仍为原outer_udp_proxy历史范围；Darwin任意SIP
缺whole-job provider/authority与平台运行待验分别记录，不减免其他内核实现。
下方均为历史输入。

## 开发接续检查点 2026-11-07 00:10 UTC

远端 integration 实际完整 SHA 仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`，
remote 为 `https://github.com/zerodenet/core.git`。普通接续，无 reset/强推/覆盖 dirty。
最新冻结构建输入 `f7af2259`（原始 input 保存完整 SHA）仍在必要定向构建；
不是最终候选，五个职责域均未获完整关闭资格。

当前实际 `Cargo.lock` 和受版控 `.github/round2/candidate.Cargo.lock` SHA-256 均为
`402d67cc184df4d4bc306d50e0badb8a57ccf43f2807b490502eca3b480dd698`。
变化来自 inspect 消费既有 ConfigurationSource 的实际依赖；先前 `9026e325…`
测试继续按原锁单列，绝不跨输入借资格。

| 实际被测输入 | 本次真实定向结果与边界 |
| --- | --- |
| `91beda0531c7e03cbc31db2ac19e3771fe8d5ad1` | 九包 all-features lib/bin/web no-run exit0，769.43s，原始日志 SHA-256 `338cc81c8963d0c4e2f2ad83ef0e5f467890f46dbf432af6bf6bca68d4aae847`；同产物 Source 全局真实预算1、普通 H2/Upgrade Business Domain1、SIP1、Runtime cold13/late10/实际 policy phases2、独立 inspect1、quota 真 worker4、控制资源退休29、DoQ6、DoH callback1、staged N1 deadline3、ALPS2、software firstPending/真实 Scope1 PASS。Echo3 PASS/1 ignored（保原忽略原因） |
| 同 `91beda…` | H2 原1字节窗口/256KiB/8秒：5 PASS/1 FAIL，收到224397字节 phase1；linked WG 3 standalone PASS/5 linked FAIL 于初始 apply；Packet budget1 FAIL 于初始 apply；DNS stateless10 PASS/新 cached gRPC→MUX1 FAIL Revoked；ordinary MUX4 PASS/2 FAIL；DoH故障恢复1 FAIL 原 panic 未交清。全部失败原始记录保留，不称环境待验或完成率 |
| `1e96a503`（完整 SHA 在原始 input） | Runtime/Engine no-run exit0/90.51s；完整 network restart 实际旧 Business Busy/原 TCP 连续/停旧后刷新 cold 集合3 PASS；统计 byte 同 owner原子配置/真实held Busy9 PASS；实际carrier/process policy phase2 PASS。尚非 TUN 生产装配资格 |
| `6c7511ac746b62fdff67f312dfba44768ac20573` | 组合 no-run exit101/32.39s，新 Service 回归误用不存在的 adoption getter；`f7af2259` 修为实际 retained LoanObservation。没有生成该输入新测试资格 |

本次实际实现推进（均待各自新输入回归，不当作整域完成）：

- A DNS：保原 Query/持久池；`9b82a195` 将原 DoH native close 与同故障 canonical receiver/真实 Host ACK 分开，保原错误；待复验。完整 donor 集合、设备/TEMP 调用者及旧 DNS bridge/Session/prepare_datagram_exchange 删除仍未完成。
- B/M：普通 VMess/VLESS/Trojan 完整既有 MUX transport 材料进入同池，原 H2最后 DATA/END真实flush修复通过 Business/scoped 原件回归；`1e96a503` 修原 pooled preparation strong owner 提前 Drop，`0623919a` 修原 VMess MUX END→正常 upload EOF 后继续 downlink，待复验。Relay/Service、REALITY navigation、QUIC/XHTTP/mixed及全旧执行器删除继续。
- C/M：删除不可达 TUN legacy executor；`e4808629` 修 Source host拒已校验 PacketDatagramReads 的真实初启缺口，待 linked/Packet 回归。`5852c659` 接实际 System TUN 停旧→同 Root target policy→明确 candidate Arc Source Ready/真实网络代次→同 immutable材料 cold refresh→BuildNew→Source binding；失败恢复全链、完整 TX/SDK/8项实际Root、TEMP软件root/provider、macOS受控helper仍须继续。
- G8：独立 inspect 已消费同 ConfigurationSource/真实FileClose与既有scratch；quota未接最终worker fault保同Source且新原件前交接，4真worker回归通过。Windows NamedPipe/raw handle与订阅、真实跨平台恢复仍要适用环境资格。
- 预算：新增统计历史/每订阅可选生产字段已消费实际同账，缩额不驱逐held事件；9项通过。其余 Metadata/QUIC、Packet/TUN/transport/process及SS replay完整投影继续；固定常量不当作生产配置已完成。

所有定向原始 input/result/命令/工具版本/日志/产物摘要保存在 Git 外。
`m-91beda-engine-final-quota-worker-fault` 误筛选0项不计通过，正确后续
`m-91beda-quota-actual-four-worker-lifecycles` 4 PASS；一次software诊断从dirty主树
发起，保留并排除输入资格，正式 frozen91beda 记录另列。Callgrind诊断保原8秒，
受插桩影响的失败只作归因，不作运行资格。

尚无统一最终同 head CI，也尚无确认可下载的云端原始归档；Pages先前上传
outcome unknown，未伪造 FileID/下载链接。唯一 TEMP 仍严格为
TEMP-WG-OUTER-UDP-01 原 outer_udp_proxy/reconnect 范围。Darwin任意SIP003
缺少可用whole-job原生provider/权限与普通跨平台运行待验分列；其他可做内核实现不停。
不合 develop/main，不发布部署。下方先前内容均为历史输入。


## 当前续作状态 2026 11 06 22:53 UTC

2026-11-06 22:53 UTC 开发检查点。远端 integration 再核仍为
`59615e69ea5a413d657a873157ea4adf8f204a90`；本地继续普通合入，不重写历史。
实现输入已推进至 `3ec3b64879ccfb7078188c5ccb394d3276704575`，正在必要定向
类型检查，尚无统一最终候选或同 head 最终 CI，五域均未完成。

| 实际调用集合 / 唯一写入人 | 当前接通与证据 | 必须继续的实际实现和删除 |
| --- | --- | --- |
| DNS 全 backend、Query/Blocking、持久 DoH / A | `03c6320dde3514931c40968116e868211ab597a1` 同产物：普通 VLESS/VMess/Trojan DNS 10 项、standalone WG 3 项、DoQ 6 项、DoH 原锁外回调 1 项通过；保原 Query/池 | linked WG 初始 apply 超时；SIP 同 owner 夹具与 DoH 独立缓存退休断言修正 `7b6938f9` 待复验；B 完整 MUX/QUIC 等 donor、C TEMP 逻辑原件后完成全消费集合，才应用已准备的旧 DNS 桥/Session/prepare_datagram_exchange 删除片 |
| 协议出站、所有共享载体 / B；共享契约 / M | 同产物普通 Business 拒绝地址已重试到真实目标，但最后 1025 字节缺失；同真实三层 Mieru warm borrower、有限 N1/实际 Auxiliary Host ACK/调用绝对期限通过。`3a63496e` sealed direct dependency StreamLoan、`8e2028db` 同 actual graph 去重容量与退休校验已接，尚无新运行资格 | 完整既有 gRPC→MUX 同池/材料借用接入，修 server H2 最后 DATA/END 的真实 flush；继续全部合法 transport/QUIC/XHTTP/mixed/device、跨执行方式、SS 安全历史额度与旧执行器/投影删除。不得以 gate 关闭或新接口结题 |
| Packet、Direct、设备、TUN、进程 / C；公共 runtime / M | 同产物 Bound late view 实际回收、原 Source 全局预算换代通过；Packet 原路由预算回归在初始 apply 超时。`7ddffe05` 同原 Main/fenced acquisition→精确缓存 receiver StopOld→Wire census，保实际原计数和 Scope；C `72c57256` TUN 原 Control 恢复意图与原件链已合 | 首启真实阶段诊断 `3ec3b648` 待运行；TUN capsule 尚须接真正预算事务，保真实 egress 代次后重新准备实际 binding；完整预付 TX/SDK、8 项 Packet 配置实际 Root 消费、TEMP 实际软件 Root/独立 provider、受控 macOS helper、适用原进程及旧链删除仍继续 |
| 控制 G8 / 复用 A-control；Host/跨域 / M | 同产物 Linux 控制恢复 29 项与原 bootstrap/rule-set 3 项通过，原 prep fault 接收与 Unknown 清理栅栏独立；Windows NamedPipe 原 handle/任务代码保留 | 初启 TLS、quota/inspect/lease/后台持久化全部独立实际入口继续逐 caller 审核，缺实现继续补；真实 Windows/GUI 同产物资格另验。29 项不能替代整域关闭 |
| 预算 Z003-07 / M，协议私有投影 B/C/A | 同产物全局 Source 停旧、活跃业务 Busy 后旧 TCP 连续性、原安全历史和新 Source 真实 wire 通过；四项新增 Packet read/TX 可选 schema None 导出保持 | 每实例/身份/共享/总额的全部实际投影，包括完整 TX、WG/TUN worker/process、SS replay；pending/failed/retiring 继续计额。未消费的配置字段或内部常量不算完成 |

Darwin 任意 SIP003 whole-job 缺口与运行环境待验分开：已保留 Apple 原始源码、
许可证和不可变 blob SHA；公开 kqueue NOTE_TRACK/NOTE_CHILD 为 ENOTSUP，公开 ptrace
无 follow-fork，coalition 所需真实权限不能由 euid0 替代。当前缺可用原生 job provider/
授权，不能用 PID/pgid/EOF 假关闭或长期拒绝原本合法配置结题；其他内核实现继续。
受控 TUN helper 的精确 peer/转交 ACK/exit 职责独立推进，不套用此阻点。

Rust 固定 1.99.0；同一 Cargo.lock SHA-256
`9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`。
原始输入/逐文件 SHA、实际工具、完整命令、结束码、日志及同产物摘要均保留在 Git 外。
原始证据可下载交付仍有外部缺口：Pages 早先上传 outcome unknown，无已确认 FileID；
GitHub REST/GraphQL Forbidden。已请求可写上传位置，未以云端绝对路径冒充下载。
REALITY 原 `www.cloudflare.com` 22:51 UTC 仍 getaddrinfo errno -3；保原目标/断言/期限，
恢复后原项复验。唯一 TEMP-WG-OUTER-UDP-01 仍只含原 outer_udp_proxy/reconnect 历史。
续作 PR #133/#134/#135/#136 均 Draft、base integration，只有 M 串行合入；不入 develop/main、
不发布部署。下方全部先前内容为历史记录，不能冒充最新输入资格。

## 以下为历史调用者盘点与开发检查点


续作起点 `59615e69ea5a413d657a873157ea4adf8f204a90`；remote fetch/push 均为
`https://github.com/zerodenet/core.git`，与核对的远端 integration 相同，工作树干净。
固定包基线仍为 `c940b727193f69752c66a9f49a7d64693ad74c1c`。
历史 #128–#132 及其结果只作为历史输入，不代表各域完成或剩余完成率。
本次按完整职责域持续实施，不以准备接口、局部组合、PR 合入或复杂度作为结题条件。

A/B/C 复用同一环境与独立 worktree，原任务分支普通接入 integration 后追加提交。
C 的未资格 device-stage 原型保持独立，实际续作为 device-cont worktree；不整段盲合原型。
M 唯一写入共享契约/装配，agent 的共享建议只放仓库外，经 M 审阅串行提交。
Rust 1.99.0 / protoc 35.0；同一 Cargo target，构建和对应测试由 M 排队。
起始 Cargo.lock SHA-256 为 `05843fd0d9b831fd66bd3c69e67dd257cbe27429607040d0b1f2f858d2ecf599`。

| 负责人 / 实际调用集合 | 当前 owner 与缺少责任 | 标准执行落点 / 前置 | 旧链删除点与必要回归 |
| --- | --- | --- | --- |
| A：所有 DNS backend、direct/detour、Node bootstrap、失败重试 | 原 Source/Query Scope、System blocking、持久 DoH；剩余 detour 尚消费裸流/旧 exchange | 同 native DNS provider；B 全合法 Stream donor，C 同 WG endpoint Datagram donor；单次 selector/LB resolve | 删除 ProxyDnsOutboundConnector、占位业务 Session、prepare_datagram_exchange；真实 Pending 取消、TC→TCP、失败后备、selector ABA、换代/退出 |
| A：standalone DNS 库入口 | 显式库调用者，无 Proxy 生产后备责任 | 保留有实际职责的原 connector API；host 绑定后全走同 Source | host/standalone 分流回归；禁止缺原件后回退 standalone |
| B：SOCKS UDP ASSOCIATE/root 与既有 prefix | 原 TCP 控制与动态 BND UDP 两件，缺同 owner 双原件 custody | M：ControlledAssociationPool、相关 peer Node 候选与同原件关联；prefix 借真实 DatagramLoan | 删除裸 UDP/session 生产后备；真实 ASSOCIATE/BND/UDP Pending、两 view 归还、matching 两件 close、源/族/dial 约束 |
| B：Mieru 全部既有 TCP/UDP、mixed/device/nondefault 合法图 | 同 adapter pool，whole-legacy 尚未受总额度/完整 Scope 约束 | 同 Managed records/grants/Scope、真实 Stream/Datagram donor；B→A 必要前置 | 删除 whole-legacy 资格分支及旧 dial/relay 投影；配置/网络/跨执行方式实际退休与继续原业务 |
| B：VLESS/VMess/Trojan 全既有 transport/MUX | 私有 TLS/REALITY/Vision/encryption、gRPC/H2/XHTTP/Xmux、QUIC/H3/Hysteria、preconnect/browser/reverse 与同池职责未全接 host | 同实际原件/driver/Scope 和标准角色，M 唯一全局登记/transport 接线 | 全调用者迁后删兼容 executor/投影；认证/profile、安全历史、Pending、半关闭、空包/多目标、复用及计量 |
| B：SS UDP/replay、SIP003 出站 | 实际安全历史与原 Child，缺普通 process/provider/后代及预算全投影 | 原协议 owner 与同 host finite process/resource/task grant；不重做已迁入站 | 删除剩余 outbound process/UDP 兼容执行；真实 readiness Pending、转交 I/O、child/后代适用确认、有效 replay 保留 |
| M：普通 business/Service/association 首次 role await | 已有 accepted graph/RetiredDatagrams Entry，未全接 pre-delivery adoption | 在原有限 caller 账本首 await 前保实际 adoption；association 寿命独立首业务 Session；不另起 reaper | 删除无 observer 即时取消缺口；成功须实际 loan/driver custody；取消首 waiter 不破坏仍活跃共享用户 |
| C：Direct Packet、既有 TUN descriptor/Windows adapter | 原 AdoptedTunOwner/OwnedWintunOwner、私有 host driver/returns | M concrete device provider，C 实际 factory/Packet Tx/Rx/control；routing/MTU/Inner/Outer 接普通 dispatch | 删除私有 open/start/Packet route 路径；原 buffer/ReturnAssociation、Unknown 不重放、held view/Scope/native 各确认 |
| C：plain linked WG、独立 WG UDP、Packet→Flow/DNS | 同 EndpointStackOwner、冷 union；独立 wire 缺 source-preserving BoundDatagram，DNS donor origin 未补 | M 同 host Bound provider/真实原件 lineage；C 同 crypto/stack 标准 Packet/Stream/Datagram | 删除非 TEMP direct raw bind/private workers/old exchange；认证 roaming、Pending、回程/片段/MTU、方向撤除和恢复旧 TCP |
| C/M：配置与动态 System TUN start/stop/recovery | 同 TunCompletion/native channels，缺标准 InboundSource/动态 actual slot/TaskRegistrar | M 同 RuntimeHost/canonical allocator 与真实 Packet-device source identity；C 原设备、channels、main/route workers | 删除私有 create/spawn/completion-only ownership；保 TunInfo/ACK/父退出、系统恢复，实际平台资格另列 |
| C：macOS helper、系统 route/DNS command、Windows reader | 原 fd/Child/SCM_RIGHTS/session，缺完整 transfer ACK、适用 join/后代责任 | 同实际 helper/control/原件，先安装再 ACK，真实 worker 进同 Scope | 删除全局 stream/丢失 launcher 责任；不能 EOF/PID/direct-child reaped 当完整关闭 |
| 控制（首波后复用）：Windows pipe、订阅、fault handoff/recovery | 原 NamedPipeServer 仍依赖隐式 Drop；部分 fault 已物理关闭未交 supervisor | M 原始 handle/provider；同 control server/subscription joins、实际故障交接 | 删除隐式 handle/永久 fail-closed 实现缺口；慢/断订阅、panic、最终 ACK、revision、身份撤权和流量结算 |
| 控制：TLS/quota/inspect/持久化剩余调用者 | 已迁同 Source/lock/worker，不重复实现；核全部独立入口及失败/退出边界 | 同 ConfigurationSource 和原 lease/worker/native；Engine-only/FFI 不启动 Proxy | 实际原件关闭、worker join、持久/临时、quota/计量回归；不动消费者 |
| 预算（首波后复用）：Mieru legacy/新全池、WG/TUN worker、Packet return、transport/process、SS replay | 现额度局部，缓存/常量不是总资源配置 | 实际 owner 稳定后由唯一 config 写入人按每实例/身份/共享/总体范围投影；pending/failed/retiring 同计 | 默认省略兼容、耗尽拒新、确认后回收、不驱逐活业务或有效安全历史；1c1g 独立资格 |

B→A、需要的 B→C 和公共依赖随精确前置提交更新；未给出合格 SHA 时不得将接口存在记为已迁完。
各域完成均须：全部实际调用者迁完 → 普通生产装配 → 原件/任务完整 → 旧链删除 → 必要回归。
最终统一检查只在五域稳定后排队，保留每次实际输入、失败、忽略、中断及未执行。

环境资格单列：同新内核的冻结 GUI/面板、特权 TUN/系统 route/DNS 恢复、真实 Windows/macOS、
全部适用互通与 1 CPU/1 GiB 实测。实现缺 provider/source/Scope 不得归为环境待验。
REALITY 保原目标/断言/期限，DNS 环境恢复后定向复验；不改网络或目标绕过。
TEMP-WG-OUTER-UDP-01 仅原 outer_udp_proxy/reconnect 范围保留未关闭、不纠正不扩大。
最终 artifact 必须可下载并含同一验证锁及原始证据，不只提供云端绝对路径。

## 续作共享前置（不是职责域完成）

- `1f47c32353d539df8d54ae95e558f9225a3dbc1f`：实际 TCP 控制与协商 UDP 原件使用同
  ControlledAssociationPool 总额度；关联责任不改物理 CarrierOrigin；原 Tx/Rx 转交保同 LoanOwner。
  平台 carrier 受影响组 53 PASS，额外关联/保留 view 三项 PASS；首次测试编译缺 Future import
  的失败留在外部证据，不将前置资格算整域完成。
- M 暂时唯一写 `component/stream_bridge/{network.rs,datagram.rs}`，按 B 的具体需求补
  同 actual Node resolver 的相关 peer 候选、真实 UDP 观察与 TCP establishment 分离，之后归还 B。
- DNS 的 Single Packet+Datagram 冷选择/实际 open_carrier 只准备现有 wire 形态，仍需 C 真 donor；
  A 只解析一次 selected target，未选 Datagram 时才使用同一次 resolved 的 Stream 路径。
- concrete Packet provider 扩展 Create/ExistingWindows，先留原件/issued identity，再配置；
  失败保同控制器，唯一 view 返回、同 Scope driver join 与 matching native receipt 仍分别确认。
  不因 provider 存在就开启未完成的普通 Packet/TUN source。

## 当前接续状态（五域均未结题）

再次 fetch 的 `FETCH_HEAD` 仍为完整远端 integration
`59615e69ea5a413d657a873157ea4adf8f204a90`；没有回退或强推。
本地依赖以普通 merge/追加提交累积，尚未作为最终候选发布。
此前组合实际验证锁为 `3e9fea043e217bb125dcfe6040e8e2b60757ab403fa66f1d3719db8e86f33214`，
其中只接入实际 Packet backend 的现有本地依赖；最终归档仍须携带相同锁本体。

| 实际调用者 / owner | 已有具体前置 | 尚须接通、删除与回归 |
| --- | --- | --- |
| DNS→SOCKS→Mieru / 原 Query Scope、同 SOCKS TCP+UDP 与 Mieru 池 | `f53`/`e5d6df9c` 的实际 pending 原件容量与唯一 view 先留责任（完整 SHA 以依赖图为准）；失败原始日志保留 | 原两个真实取消/复用回归复验；其余 resourceful/mixed/device donor 全迁完后才能删旧 DNS connector/exchange |
| 普通 TCP / canonical plain linked WG、原 FlowUsers | `b144b4c4` 实际 source wire origin；`e8be9c88` 普通 factory/endpoint-free dispatch；`9cb64a9c` 原 admitted facts/调用者 witness；`6c6ea7b2` 同用户表、Inner 计量、真实 SYN 与换代检查 | 真正普通生产装配回归、借入与直接用户关闭/Scope/native 分别确认；独立 WG provider、Direct Packet 和旧 executor 删除仍缺 |
| 普通 Datagram / 原 DatagramOpening 与设备用户表 | `ca308dd5` 同实际 Packet opener；`5c203509` 完整 OptionalEndpoint 消费与日志，保持网络 endpoint Some | 普通 WG wire/DNS 借入的真实取消、失败/换代与退出回归；不可将 None 填成假 server/port |
| 动态/配置 TUN / 同 inbound registry、canonical allocator、源原件 | `446d3687` 同唯一 ListenerObservers 消费动态源 Scope；`0834b21d` + `bd4d3dfb` + `b2019c70` 原 source 子任务 Scope 与实际声明 | TUN 全旧能力（DNS hijack/sniff/计数/ACK/readiness/错误码）与普通 start/stop 仍须完整切换；后删私有 executor；特权平台资格另验 |
| 无总期限的 gRPC/Mieru / 原 protocol 账本 | `e19371f1` 原 native 单次关闭；`c0acda54` 实际 view/调用者/逻辑归还快照；`51310bc8` Mieru 真实 ordered barrier；`bcc094e9` gRPC 同原件完整结算 | 定向原 wire 回归；所有兼容/跨执行方式与其余 transport/MUX/process 不能以此局部代替 |
| per-connection Service TCP / 原 caller 与组件 Scope | 已冻结错误移交点：仅登记 driver 不能证明长期共享池接受了责任 | B/M 接具体 noncached Service 声明与原 pending/Accepted finalizer；保同额度和 Scope，不建新清理任务/池 |
| IPC/HTTP/gRPC 故障恢复 / 原服务器、accepted/subscription 与预算 | `30ba9212` 物理确清才释放原 slot，始终交出原 typed fault | 四项实际服务器故障再起测试待 M 排队；初启 TLS、quota/inspect、持久化全入口继续核查 |
| Windows Named Pipe / 原 Win32 handle 与 std worker、同 Scope | `656f0d07` 真实 overlapped drain/std join/Scope join/handle close；`0fac8800` 未知关闭不重试可能复用的数字 handle；`ab7a07a5` 外部 waker 不在原锁内归还 | GNU 交叉编译及真实 Windows 回归待验；Linux 不能报告 Windows 运行资格 |
| SS replay 与剩余生产预算 / 复用 A 的预算 worktree | `af535bbd` recipe 绑定同实际安全历史；既有 51ad/DNS/control 不重复记成果 | policy/capacity/profile 替换与有效历史、总额度、WG/TUN/returns/transport/process 实际 owner 投影；保每实例/身份/共享/总体语义，pending/failed/retiring 不退款 |

A 完成控制增量后在等待 DNS donor 期间复用原 budget worktree；SS replay 文件由 B 明确唯一转交 A。
B 保留其他协议/transport/process，C 保留设备/source，M 仍唯一串行应用公共契约与装配。
所有上述提交只是前置与当前调用者状态，不是最终资格。开发编译失败逐次留档；修复只追加，
没有放宽断言、deadline、生产容量或栈。统一最终检查、续作 Draft PR 与可下载原始证据仍须完成。


## 续作组合记录：原件生产责任继续推进

远端再次核对仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`。
协调 Draft [#133](https://github.com/zerodenet/core/pull/133) 的 base 为 integration，
当前远端 head `ff7a1f7a69743701be63c9772f3f1fe571494205` 仅为阶段输入，
没有最终候选资格。本地普通 merge/追加提交仍在继续；下列事实不表示任一域结题。
该组合验证锁为 `3e9fea043e217bb125dcfe6040e8e2b60757ab403fa66f1d3719db8e86f33214`。

| 原调用者与真实 owner | 已组合的必要前置 | 剩余执行及删除门 |
| --- | --- | --- |
| DNS query → SOCKS 双原件 → Mieru 原池 | B `0ecf82c4662824700f817e1f47cf589a88814484`；A `dbe7a6586394c8f1fae0a5f97b96cf4a87163078` 改用普通 RunningProxy 同配置/同源 | 首次请求/取消 ACK 原失败未获修复资格；其余 resourceful/device/ECH caller 全迁完再删除旧 connector/exchange |
| 诊断、URLTest → 同 RuntimeHost 与 Direct 原 Scope/原 provider | M `0bbd922b` 真实 probe task、唯一全局额度、首 await 前 custody；`995c7ece` 候选额度与退出；配置 Direct 用其原 Scope 实际 initializer | 普通冷/热、coalescing、最后 waiter 取消及既有 V1 错误/计量必要回归；新真实用例 `a438b5bd` 待构建，不能拿票据测试当生产资格 |
| 无 observer 的取消 carrier | M `fc65d433` 同原 Slot 持有关闭 future，同原 host supervisor 轮询；固定首次清理期限，不增加 task/Scope/池 | 平台真实 I/O + actual Scope join 才退唯一额度的回归通过 1 项；Mieru/gRPC 普通 wire 及各合法调用者仍须验证 |
| VLESS gRPC Stream/Datagram 与原 gRPC pool | B `daac42b5`；M `a4f67381` 同 immutable recipe、`3f8e2417` 普通登记、原 root totals 配置 | CA/ECH/all backend、Relay/MUX 与其余合法 transport 仍有实现；不以局部 gate 关闭当全域完成 |
| TLS CA、初启 control TLS、父 stdin、inspect/quota | M `e8e76efe` 同授予 CA 原件的真实 TLS backend；先前 Source bounded read、parent std worker/Scope/native 均已组合 | 新 CA/backend 回归与最终统一输入；Windows GNU 类型检查仅代表相应历史 SHA，不代表 Windows 运行；quota/inspect 原职责继续审计 |
| System TUN ordinary source、route/monitor 与实际 Registrar | C `c141883d`；M `a132e704` 既有 UDP 构建 neutral source 闭包及原 native 计量，`ab50ab81` 实际 shared-Scope 总额 | 真实 privileged backend 资格另验；helper/Windows reader/独立 WG/Direct Packet 仍是实现职责。旧生产 runtime/route 执行链迁完后删除 |
| System TUN main exit、TunInfo/失败恢复投影 | C `7ee6d484`；M `1df7ab88` 同 exact original observer 投影和自动停依赖前真实 stopping 事实 | EOF/主动 stop 对照、错误身份隔离、所有原 Scope/native 清理及恢复回归待验；投影不发关闭凭据 |
| TUN 小总额度满额与恢复 | M `3c60b3c7` 保原每组 ceiling，同 Registrar Capacity 已知未登记拒绝仅归还冷 reservation | 真实 TCP Pending/main+child、actual join 后可再登记的必要回归待验；无 native TUN acquisition，不能作为原设备资格 |
| SS TCP/UDP/legacy replay 与同 adapter 原安全历史 | A `98f240d8` legacy 实际 bitmap bytes、`404dc5d2` UDP 五额度，B `0ecf82c4` SIP 原 Options 投影 | 新模块/可见性编译错误逐项留档并追加修复；SS 两测试三 case 的整体超时保留，Phase 诊断 `a183adab`/`1ee3e16b` 待实际运行 |
| SIP003 原进程/派生 loopback | B `02847eb0` 单一真实 actor 做 prepare→Ready→monitor | Linux/Windows/macOS 适用 process 原件、后代确认、原 actor join 与派生端点授权仍缺实现；不归环境待验或 TEMP |

开发仍只排必要构建/回归，原始失败日志留在仓库外。`4db6e4d5` 正构建所影响的 root/
proxy/runtime lib 产物；新的本地提交不得借用该产物声称被测。最终统一检查、精确候选 CI、
可下载原始归档及各域旧执行链归零检查尚未完成。TEMP-WG-OUTER-UDP-01 保原范围单列未关闭。


## 后续实际调用者与原件检查（实现继续，未冻结最终候选）

远端 integration 最近核对仍为完整 SHA
`59615e69ea5a413d657a873157ea4adf8f204a90`。协调 Draft #133 仍非最终候选。
现有 manifests 增加 zero-dns 对既有 zero-core 的直接依赖以匹配真实 Scope 身份，
实际验证 Cargo.lock SHA-256 已更新为
`9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`；
没有改变任何第三方包版本、来源或校验和。仓外锁差异证明与每次原始输入记录均保留。
最终仍须提供实际同锁本体及下载地址，不以本段替代归档或候选 CI。

| 职责域 / 真实调用者 | 已取得的必要证据与后续前置 | 尚缺实现、删除或复验 |
| --- | --- | --- |
| DNS / 原 Node Query、持久 DoH 和 detour | `4abc0a63d2e66dfb2c58b4ccffe10c7d3a6e74c1` 上 ECH Domain 与跨 Default/Node 同物理 DoH 通过；UDP65 夹具原错把 EDNS 尾部当 question，`3ea3d50f` 修真实 question 解析 | VLESS gRPC 四项首次真实生产调用均在 native 前报缺 controlled grant；B `6073acca`/`59508ba7`、M `f59dd581`/`5fbe371c` 接原 Node opener 与完整候选，待同测试复验。`f47b8e1e` 同代 N1/N2 实际 peer 原池修复也待验证。全部 mixed/device donor 到位再删 ProxyDnsOutboundConnector/占位业务 Session/旧 exchange |
| 出站 / taskless Service、Business、Relay、gRPC/其余 transport | 同 `4abc0a63` 的四协议 None-deadline N1 真 wire/cancel 通过，复用唯一原件与额度；gRPC CA/ECH/Business 六项均暴露同前置 grant 错误，原日志保留 | 所有 taskless Business 仍有 `inner.connect -> bare Node TCP` 旧链，SS 原件断言真实失败，B/M 正补同一个原件 owner 的严格双需求授权；全合法 Relay/MUX/transport 及跨执行方式继续迁移，不能放 gate 或增第二池代替 |
| Packet、设备/TUN / 同 Source Native、原 Direct Packet provider | C `a15a83ee`（M 单取 `f6d3a429`）核同 EndpointIo/Live/listen/active lease，修新虚拟 Link 错停真实 Source；`4abc0a63` 配置/网络替换及普通 literal/domain 三项通过。Direct Packet 两个真实原件负向、TUN 一主任务额度在 native 前拒绝通过 | Packet 第四项停在新增夹具错误 tag，C `5c51a9e9`/M `193ca853` 修后待原强断言复验。独立 WG provider、原生读取/解密/重组及分片真实计额、Windows reader/helper、适用进程 lookup 和旧 TUN/Packet 执行链删除仍有实现；特权 TUN 原生资格继续明确 ignored |
| 控制 / TLS、父输入、Named Pipe 与服务器 | 先前 Source TLS/父输入及真实 Linux 控制恢复证据保留；A `c704b97d` 增真实 Windows ping/订阅/失败再起原件测试，未运行 | 同原 ControlTask 登记窗口仍可能丢失已 spawn JoinHandle，A 正修唯一槽；Windows GNU 检查和真实 Windows 运行分别记录，不能互换资格 |
| 预算 / SS Source、UDP 安全历史、进程和 Packet | `4abc0a63` 的 SS pure Source 与双 UDP Source 两项实际预算换代通过；Source acquisition idle 由真实 peer cleanup 唤醒。SS Business 第三项失败保留。Linux 原进程树 `6ab90ae1a9071dfdcabb36b742db3d9d690092b1` 三项真实回归通过；Host 原件/实际 Scope/原 max_starts 两项在 `2e3f9565cb61653fe5ccd5331605c489161f4233` 通过 | Linux Process provider 已接 Host/BoundResources 冷 grant，B 实际 SIP 调用者仍须接入；全局生产 process/transport、原 Packet return/native-read、WG/TUN worker 等额度投影仍缺。进程树、实际 actor join、原 fd closes 各自核对，不把 body/EOF/端口重绑当关闭证明；Windows/macOS process 仍是实现缺口 |

以上为必要开发回归，不是统一候选资格；不同 SHA/feature 产物严格分开，失败/忽略均保留。
最终五域调用者归零、普通装配、真实原件任务责任、旧链删除和必要回归完成后，
才冻结统一输入做完整验收与精确 CI。TEMP-WG-OUTER-UDP-01 仍仅原 outer_udp_proxy 范围未关闭。

## 续作核对更新（先前表中待复验项以本节的新输入为准）

同一输入 `88c6bcea246f1a29c780d7be30ef7b4fc68cbdcf`、Rust 1.99.0、
Cargo.lock `9026e3253d6a42c91cd557ee3ff95c2b1e93e46ba7749c707dec592e055ca6a1`：
SS 三项真实业务/Source/安全历史预算、gRPC 六项实际 CA/ECH/网络/计量、VLESS gRPC
DNS 四项候选重试/原查询取消/换代、Mieru 两项原 Node 代次/嵌套真实原件定向回归通过。
这不包含剩余合法 transport、MUX、mixed/device 或完整 DNS detour 集合的资格。

同输入的实际 Relay 回归在最终读取出现 `unexpected end of file`；真实 wire 和计量
断言没有删除。B `b988036f` 改为实际认证流 half-close 后读取 reverse EOF，尚待复验。
同输入的 taskless ACK 回归在实际 unused retry 后仍占额度而失败；后续 `02d6116d`、
`4db80898` 修原件/view 在 IO 前持有与正面无原件退款，`21158565`、`a41e257d`
另外保留已获得原件直到真实 Host ACK，`fd82139a` 区分可见的 Native/User receipt
和总额度释放，均需在含这些提交的冻结输入验证。旧失败日志保留。

root 的 `--lib` 不编译 `zero` 二进制。首次补 `--bins` 在上述输入发现三处控制类型
错误；`38870732` 修正。仅将该修正加入原冻结源码的独立验证输入
`3b609770`：控制原始任务登记、并发命令故障、Unix 原件/无效 UTF-8、HTTP 与 gRPC
服务器退休、订阅任务及控制故障恢复的必要回归通过。第一次 HTTP filter 选错而
运行零项，单独保留记录；正确 `http_adapter::retirement_tests` 已执行通过。
随后 A `6190bb9e` 保留 status monitor 的原始 JoinError，待下一 root 输入验证。

`7e9c324d` 接同一个 Process job 的实际端点预留、匹配原件关闭、inline readiness
probe、真实 actor 及跨 Scope 直接依赖授权；`407bc7f8` 接 Windows Job 原件同一
预算/端点职责。Windows Job 先 suspended 创建并强存原始 handles，分配 Job 后才
resume；清理要求实际 root wait、Job 内零进程、原 actor join 和逐 handle 关闭。
`8e490d33` 的 Windows GNU platform all-features/tests 源码检查通过；真实 Windows
运行尚未执行。Linux 端点与 Packet 原件测试的第一次编译发现 fixture 私有字段访问，
`ecb001a8` 修实际 accessor，继续排队验证。macOS 任意后代进程责任仍有实现缺口，
不能用进程组、单 PID/EOF 或源码检查称完成；已在调查 XNU 实际可用机制。

C `0c3ebe93` 私有整合及 M `7ba21b4a`、`bd52371f` 接真实 Packet return MTU、
同原存储分片/重组、Direct/TUN 读取预算和 Windows 原 SDK reader 的实际 Scope。
WG 首 bind 前的成对 workspace、独立 provider、完整 Source 暂停/预算 preflight
与旧链删除仍在实现，未把新字段或 foundation 回归计为生产调用者已迁完。
M `22dbd013` 将全部现有 H2 client 入口的 connection/read/write 执行合并到同一
实际调用者，删除该入口旧三个 spawn 和无界写队列；真实 wire/backpressure/half-close/
原 loan 取消测试已接实际 `web_carriers` suite，尚待执行。这不关闭包裹它的旧 native
socket、其余 transport 或共享池迁移责任。

上述均为开发输入；所有五域仍按首节完整调用者清单持续收尾。当前 Draft PR #133
仍是早期进度记录，尚无统一最终候选或该候选 CI 资格。最终锁文件、源码清单和
原始日志必须以可下载归档交付；云端路径及分散输入结果均不替代该交付。


## 当前接续检查点（继续实施，不是最终候选）

源码输入 `ea953646d257ada18c0658e62ee5f69567b9cfcf`、Rust 1.99.0、上述同一
Cargo.lock：原 Host initializer 首借用取消后继续推进与清理两项、真实 ProcessEndpoint
预留/探测/原件/actor 四项，以及 Metadata 实际可写后同总体额度与 caller ACK 一项通过。
先前 `5905c97b` 的原件总账五项中四项、ProcessBudget 真 RawWaker panic/延后 ACK 一项、
Metadata driver/user graph 三项、双 reader SourcePause 一项通过；Metadata 非阻塞发送
夹具首次 WouldBlock/超时保留，由 `d62e58c4` 等待同原 socket 可写后在上述输入复验。
两次匹配零项的错误 filter 另记未执行，不能算通过。

H2 原期限/一字节窗口/256KiB 断言未改。`5905c97b`、`55de5a53` 均仍三通过一超时，
实际接收分别为 178701、183332 字节；继续定位，不提高期限、窗口或生产容量。
进程监控两项失败分别归因为 canonical 模式冲突的错误分类及主动撤权不同于进程退出；
B `f8c5ceec` 与 M `007a26dc` 已保留实际类型/真实 exit 23，尚待同输入复验。

Carrier/Metadata/Bound 同 Host 原件总额、实际 Source/Direct 暂停、异步 SDK 获取及
按需同进程启动已组合，仍须补完整普通生产投影、剩余 provider 与旧链删除。
A 的总体额度 StopOld/DNS 委托，B 的完整 SS UDP/全部剩余 transport 与共享池，C 的
独立 WG provider/前置 process lookup，以及控制末项与全域预算都继续实施。
五个完整职责域尚未关闭；远端 integration 最近核对仍为 `59615e69ea5a413d657a873157ea4adf8f204a90`。
Draft #133 仅保存可审阅的接续源码，无最终候选 CI 资格。TEMP-WG-OUTER-UDP-01 原范围不变。
