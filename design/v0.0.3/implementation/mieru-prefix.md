# Mieru prefix 原始所有权链与实际 Proxy 装配

本地 `develop` / v0.0.3，接续 [节点 DNS 与 UDP underlay](mieru-carriers.md)。Mieru / mieru-config 继续固定 3.33.0。本批未提交、未推送；**普通生产登记保持关闭**。下面的原生装配入口只供内部资格回归，不能把同池场景通过解释为全部协议迁移完成。

后续跨 owner 与无独立 driver 的实现见 [跨 owner 借用与传递替换](cross-owner.md)；以下保留本批同池资格范围与原始验证记录。

## 标准借用契约

`StreamOutbound::connect_carrier` 与 `DatagramOutbound::open_carrier` 是显式可选角色，默认在 I/O 前拒绝。普通 `BoxStream` / Datagram 会话不会在打开以后被升级为原生所有权。实际 donor 返回唯一借用视图、真实逻辑控制器和不可凭数字构造的 `CarrierOrigin`。

`CarrierOrigin` 捕获同一个原始 slot 中已经实际注册的 TaskJoinWitness；外来 scope、未使用的 grant 和其他 driver 的 witness 不能替代它。原始根与每层借用使用独立 origin，父链用共享元数据保留，不保留 Proxy、配置、注册表或原生 I/O 所有权。观察者可以检查真正的 driver join 和同原始 slot 的关闭处置，但没有取消 donor 的权限。

`LoanClosed` 仅证明这个逻辑借用的视图已经归还、协议控制已经完成。它不证明 donor socket 关闭，也不能签发 donor 的任务退出。借用方先把真实 LoanOwner 安装到持久 carrier slot，再进行协议准备或等待；失败、取消和期限届满保留原控制器及预算。只有真实关闭流程收齐当前 slot 的原始凭据、driver join、逻辑借用控制和转交视图，才发布该 slot 的完成元数据。原生根的处置另行标明实际 native acquisition；借用 slot 不冒充它。

宿主给借用 I/O 加当前角色与共享依赖的实际用户租约。TCP 租约跟随唯一流视图；UDP 租约跟随两侧包视图。协议的持久逻辑控制另保留实际共享池用户债务，避免同池内部借用形成“先等角色控制归还、但归还只能由池清理执行”的清理环。

## Mieru 自有复用链

Mieru 的逻辑 TCP 控制拥有原 session ID、真实 writer barrier、reader entry 移除信号和原 driver witness。归还借用时先等视图返回，再提交有序 CLOSE barrier 并等待实际 session entry 移除；已经退出的真实 driver 可以解释死会话，不能伪造发送成功或吞掉原任务失败。中断等待保留同一个控制 future。它不关闭仍供其他逻辑用户使用的共享 driver。

标准 UDP 角色同样持有真实逻辑控制，而不是把 `poll_shutdown`、Drop 或 EOF 当作原生关闭。UDP prefix 直接转交有控制的包会话；协议继续使用既有 packet reliability / ACK / framing driver，只在原 TaskScope 注册这一个真实 worker。没有辅助读写 worker、管道、第二池或第二内核实例。

`ControlledCarrierPool` 描述固定 profile endpoint / dial / underlay 和有限上限；动态选择的 prefix 与网络代际来自实际宿主。一个适配器仍只有一个实际 ClientPool，全部 endpoint、TCP/UDP underlay、pending/live/retiring/Unknown 记录共同使用原有保守 policy 的一个总预算。完整 profile 集合包含未使用凭据；凭据变化替换整个共享 owner。

网络或 resolver 代际改变时，仅更换缓存键不够。实际池先取消旧记录的真实 worker，保留并收集原始关闭凭据；旧逻辑视图和控制还持有用户债务时拒绝新分配。只有原记录和旧用户都结清，才在同一个实际池 owner 中接纳下一代网络。正常同代复用与有界扩展仍使用原协议策略。

## 实际 Proxy 连接

`Proxy::from_engine` 的普通入口明确关闭原生背景装配。内部资格入口复用同一个构造函数、实际 egress、DNS 对象、ProtocolInventory 和 RuntimeHost；注册 decoder、共享池 factory 和兼容适配器引用同一个 adapter pool。

持久 provider 只保留实际 egress 与 Weak DNS / Weak CurrentServiceSource。每次操作临时借用当前服务，按真实 RoleInstanceId 重建严格更短的 prefix；不能保留业务 Session、调用方 opener、整个 Engine、注册表或旧配置。当前根和每一层 Outer 计量使用原有观察边界；连接端点事实从实际原始 controller 反向报告，不根据配置虚构 socket 地址。兼容 reload hook 只清 legacy 池，不能在宿主事务之外清掉刚确认的新 native generation。

当前资格范围为同一个真实共享 owner 内的 Mieru 自有链。跨 owner 的 prefix 在 I/O 前拒绝，不能绕过缺失的宿主传递依赖替换关系。实际 Proxy 根域名也明确拒绝尚未接入原始 DNS 查询 provider 的路径；库中的 owned UDP DNS 支持与该实际装配资格分开记录。远端 prefix 的命名目标仍使用原协议 wire 形式。

## 生产开关与兼容边界

尚未具备的生产条件包括：无独立 driver 的协议 donor 的原始所有权转交、跨 owner 的真实依赖与替换事务、实际 Proxy 全部 DNS 查询 carrier 的原始 provider，以及对应故障、取消、计量归因与外部互通矩阵。不能为满足开关而构造 driver、借用别人的 join、默认直连、补发关闭凭据或只登记一个子集。

公开配置、Zero API / IPC / HTTP / gRPC / FFI、客户端与面板发布协议不变；未增加配置选项或改变协议基线。本批需要以冻结源代码完成工作区全量、严格 Clippy、架构边界及同一新二进制的 GUI IPC / 面板消费者回归。旧批次的通过记录不能替代本批验证，也不代表真实 GUI/TUN、线上发布或 1c1g 实机压测。

## 本批验证

证据目录：[validation.json](local-mieru-prefix-20261025/validation.json)，源码冻结为 471 项记录。本批同池 Mieru 与实际 Proxy 内部资格装配已完成以下本地回归；普通生产登记仍关闭，前述剩余条件没有因此豁免。

- 工作区全部特性、正常测试并发、16 MiB 测试线程栈：155 个目标，3,794 通过、0 失败、175 项既有忽略；`TEST_END exit=0`。10 项新增 Mieru 宿主／实际 Proxy 回归均包含在完整结果中。
- 稳定版 Rust 1.98 的全部目标／全部特性严格 Clippy（`-D warnings`）通过；运行时边界 190 项、工作区边界 27 项、CI 范围 21 项，以及格式、测试布局、方案链接和 diff 检查通过。
- 同一二进制 SHA-256 `188161cbb10fe8d0f8894904cb3b848376617daad273b13908e07c7763fbddb7`：GUI IPC 检查 24.96 秒通过，真实子进程正常退出 0，无 terminate/kill；面板检查 99.75 秒通过，保留原 120 秒期限，现有 TCP/UDP 流量跨三个发布进程持续可用。GUI 与面板源码仓库保持原提交且无改动。

首轮完整回归有一个既有 QUIC 回环夹具失败：握手被拒绝后没有 RTT 样本，默认初始 RTT 对应的三次 PTO drain 接近夹具原有的 3 秒 join 期限。只为该夹具显式配置真实 QUIC transport 的 20 ms 初始 RTT，保留原期限、两个实际 driver join、原始 panic 和原生关闭断言；定向连续三次及第二轮完整回归均通过。运行时源代码未为此改变，首轮失败记录和两个原始夹具文件均保留，见证据目录的 `workspace-before-quic-fixture-fix-summary.json`、`quic-refusal-fixed-three-runs.txt` 和 `runtime-source-equivalence.json`。

这些消费者结果覆盖普通兼容入口；原生业务入口的完整 TCP/UDP、fallback/MUX、故障与计量互通矩阵仍是生产登记条件。本批未进行线上发布、真实 GUI/TUN 验证、1c1g 压测或额外官方外部工具互通。全部修改仍在本地 `develop`，未提交、未推送。
