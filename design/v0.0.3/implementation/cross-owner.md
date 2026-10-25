# 跨 owner 借用、无 driver 原始所有权与传递替换

接续 [Mieru prefix](mieru-prefix.md)。本地 `develop` / v0.0.3；Mieru 和 mieru-config 仍固定 3.33.0。未提交、未推送。**普通生产登记保持关闭**，内部资格入口用于真实宿主回归。

## 原始所有权

有 driver 的 `CarrierOrigin` 仍只能引用同原始 slot 实际注册的 TaskJoinWitness。无独立 driver 的原生 TCP/UDP 现在可以转交唯一 custody view；同 slot 必须已经有真实原始 TCP controller 或 UDP controller、宿主登记和唯一 transferred lease。元数据明确返回 `driver = None`，不借用其他任务的 join、不创建占位 worker。转交后不能再注册一个 driver 改写原始谱系。

原始 controller 一直保存在实际 CarrierOwner 的持久 slot。流或两个包视图归还只证明 custody 返回；LoanOwner 的真实关闭随后驱动该 slot 的原始 controller 并消费其关闭结果。deadline、取消、Drop、EOF 和 view return 都不能生成成功的 native-close。整个 owner 的完成凭据仍只由其显式生命周期 shutdown 颁发。原始来源、slot 和逻辑借用凭据分别具有各自身份。

SOCKS5 的内部资格装配显式声明有限 ControlledCarrierPool（4 个原始 slot）和 `task_limit = None`，使用其真实 SessionOwner、CarrierOwner、HostResources 和空 TaskScope。根 TCP 经实际 host provider 安装 controller 后再 await。位于已有 prefix 上的 SOCKS5 直接在唯一 LoanIo 上执行原有协议握手，保留同一个 LoanOwner 和原始谱系；没有第二条流、FD 或读写任务。普通 SOCKS5 装配保持原来的资源声明和配置。

通用原生 UDP 转交保留同一个原始 UDP socket 和唯一 custody lease，发送和接收仅共享这个视图，不增加 worker；目标、association、完整包发送与原始 peer 过滤仍受约束。这不意味着已经支持 SOCKS5 UDP ASSOCIATE 的双原始所有权：它的控制 TCP 与协商 UDP 必须同时完成接入才能提供相应 carrier 角色，当前仍拒绝。

## 宿主依赖

选择背景 carrier 时，实际 ComponentPlan 在 I/O 前登记 consumer generation 到所有实际 donor role generation 的边。宿主验证真实已安装 owner token；只有当前或准备中的实际代次可以登记。同共享 owner 的直接角色依赖已有真实共享池及用户租约，不添加反向伪边。其余边与声明依赖联合检查，实际循环原子拒绝。

图只保留 ComponentInstanceId 对和修订号，按实际 owner 对去重；不保存 Session、配置、Proxy、协议池或每业务流注册项。失败或取消的 acquisition 不会通过 Drop 删除依赖，因为原始 owner 可能仍持有待清理资源。实际代次清理并发布后回收其边；完整宿主 shutdown 确认后清空剩余元数据。

冷规划先验证候选声明图，再用旧代次的实际借用边传递失效。供体变化或被移除都会使直接、传递消费者各推进一次；无关 owner 和未改变的实际角色继续复用。新启动图只保留未改变实际代次之间的借用顺序，不能把旧代次的边按相同 ComponentId 投影到替代代次，从而误判合法新依赖为循环。旧图仍按原 ComponentInstanceId 控制清理；借用关系不能伪造成工厂 BuildContext 的额外声明依赖。

替换事务先冻结新借用登记，再选择候选，避免冷复用快照与 StopOld 之间新出现依赖。取消的等待保留冻结与真实事务。所有受影响旧 owner 先同步 revoke；联合图按消费者到供体收集真实 protocol、TaskScope、provider 和 transferred-user/control 凭据。任一消费者未完成也阻挡供体完成。全部旧资源确认后才 stage/start 候选。stage 后的新背景借用只接受确切 preparing 代次，避免通过旧角色重新借入已退休供体；普通业务 admission 仍按原有发布边界切换。

## 兼容与剩余生产门槛

不改变公开配置、API、IPC、HTTP、gRPC、FFI、客户端或面板的消息形状，未改变产品版本、锁文件或协议基线。内部资格入口和普通生产登记继续分开。

仍需实际 Proxy 完整原生 DNS 查询 provider、其他协议 carrier 角色（含相应 UDP 双原始）、完整原生业务 TCP/UDP、fallback/MUX、故障取消、计量归因与外部互通资格；不把本批通用契约或真实回环用例通过作为这些门槛的替代。未进行线上发布、真实 GUI/TUN、1c1g 压测或外部工具互通。

## 验证记录

本批证据为 [validation.json](local-cross-owner-20261025/validation.json)，[冻结源码](local-cross-owner-20261025/source-final.json)共 484 项，最终无漂移。普通生产登记仍关闭；以下是本批记录范围的本地资格，不能替代前述剩余生产门槛。

- Rust 1.99、全部特性、正常测试并发和 16 MiB 测试线程栈：完整工作区 155 个目标，3,802 通过、0 失败、175 项既有忽略，`TEST_END exit=0`。本批 8 项新增原始所有权与宿主图回归均在完整结果中，更新后的未迁移角色拒绝用例也通过。
- 稳定版 Rust 1.98、全部目标／全部特性严格 Clippy（`-D warnings`）通过。运行时分层边界 190 项、工作区边界 27 项、CI 范围 21 项，以及格式、测试布局、方案链接和 diff 检查通过。
- 同一新二进制 SHA-256 `fac9a9d250852e4ec43797103d68bc9922032b70cbdabac37f5015f0485658c5`：客户端 IPC 20.47 秒通过，实际子进程正常退出 0，无 terminate/kill；面板 83.40 秒通过，保持原 120 秒期限，既有 TCP/UDP 流量跨三个发布进程持续可用。GUI 和面板源码仓库保持原提交且无改动。

早期定向回归保留了两个夹具失败：原生 UDP 夹具默认通配绑定，不能把 `0.0.0.0` 当作实际回环源 IP；改为显式回环 source bind，保留实际源地址相等与原始关闭断言。另一个负向夹具误用 VLESS 的 `uuid` 字段，改为既有 `id` 字段，未改公开配置解析。两个旧 ComponentPlan 测试初始化补上宿主图元数据，私有辅助函数重命名的遗漏调用一并修正。完整回归使用最终冻结源码；失败日志、源码差异与同运行时代码证明均保留，没有跳过失败用例或放宽并发、超时及关闭证据。

现有 469 个未跟踪文件均保留，锁文件与开工前一致。API/config/engine/FFI/gRPC 与根应用源码没有相对 HEAD 的变更；进程内 Rust `CarrierOrigin::driver()` 的 Option 返回类型已在 [兼容账本](../../../release/breaking-changes.md)登记。未提交、未推送，未进行线上发布、真实 GUI/TUN、1c1g 实机压测或新增外部官方工具互通。
