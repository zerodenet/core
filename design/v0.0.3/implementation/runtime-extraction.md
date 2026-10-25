# RuntimeHost 独立 crate：批准的依赖方向修订

分支 `develop`，产品阶段 v0.0.3。本地实现，未经提交或推送。

## 决策来源

冻结规格中的方向是 `zero-runtime → zero-proxy`。现有公开 Proxy 已经拥有宿主；直接照搬会形成循环。用户在本地实施中明确批准改为 **`zero-proxy → zero-runtime`**，保留现有 Proxy 入口。这是已批准的规格补充，原始冻结正文与来源哈希继续保留。不能以冻结图中的旧箭头重新引入循环依赖。

## 实际边界

`zero-runtime` 拥有 ComponentCatalog、ComponentSlot、代际分配、依赖排序、规范共享资源准备、准入栅栏、真实使用者计数、TaskScope 监督、原生资源 provider 和共享资源替换事务，以及配置操作 ApplyOwner。迁移原有状态与执行器，不创建另一个 Tokio runtime、后台管理框架或内核实例。

`RuntimeHost<A>` 使用泛型保存 proxy 的发布回执；宿主无需认识 EngineRuntimeSnapshot、API 请求或协议变体。Proxy/Inventory 持有同一个 Arc；弱转发引用不能延长宿主及协议池寿命。宿主内部字段不对外开放可替换访问，Debug 不输出待发布配置。

`zero-proxy` 保留协议装配、配置投影和兼容能力登记、Direct 实际套接字执行、现有数据平面桥与客户端/面板控制入口。装配向宿主注入同一次登记生成的真实工厂及协议状态。协议实现只依赖其标准契约、协议算法和载体，不依赖宿主。新增协议不得要求 runtime 识别具体协议名、密码、缓存键或 engine leaf。

普通依赖图为 `application → proxy → runtime → config/core/platform/traits`；协议库及载体仍通过既有契约接入。runtime 的单元测试可通过 dev dependency 使用 proxy 的真实 DirectFactory；它不是生产依赖或运行中的反向调用。跨 crate 测试显式开启 test-support，复用同一宿主监督器与真实资源 provider，不伪造 join、关闭凭据或成功状态。

## 不变的事务条件

先撤销受影响旧资源及其传递消费者的准入；确认真实转交使用者结束、协议清理、TaskScope join 与原 provider 的关闭凭据，才允许构造/启动替换资源。一个消费者未结束，不继续清理其依赖。取消或超时保留原 owner 和已收集证据；候选失败必须先确认候选清理，再按最后成功配置重建。计数归零、Drop、abort 和端口空闲均不能代替原生关闭凭据。

## 兼容性

现有 Proxy/ProxyHandle/RunningProxy、V1 配置/命令/事件、CLI 和 FFI 入口不因 crate 提取而改 schema。内部目录变化不要求 GUI 或面板同步改版。共享资源变更时依照既有约定取消受影响业务；无变化资源继续复用。客户端/面板验证使用同一个固定内核二进制，不能把编译通过、服务替身或旧批次验收当成本批的实际兼容结果。

## 后续资格

提取与协议迁移分开验收。Mieru TCP 池、Hysteria2 QUIC、WireGuard 设备、Shadowsocks 插件等只有接入实际 provider 与原始关闭证据后才能开启生产工厂；不通过移除 Unsupported 栅栏来宣布完成。剩余协议/传输路径的覆盖、真实设备/TUN、1 CPU/1 GiB 压力资格仍按冻结任务卡逐项验证。具体本批结果以新的源码摘要和验证记录为准，历史批次结果不覆盖新源码。

## 剩余协议迁移顺序与阻断条件

| 范围 | 已有实际入口 | 下一项必须具备的条件 |
| --- | --- | --- |
| Direct / SOCKS5 普通流，VLESS / VMess / Trojan 已支持的非池化流，SS 无插件 TCP | 现有生产 ComponentFactory 登记，随同一个宿主迁入 runtime | 保留各自 native_input 的真实覆盖及旧路径，不能将部分 native 支持写成全协议完成 |
| HTTP / Mixed | HTTP CONNECT 入站回复及入站复用，现有协议握手和统一入站执行 | 它们没有已支持的 HTTP 出站配置，不为“完成迁移”新增出站协议或改变客户端 schema |
| Mieru 池 / TCP / UDP role | 已有分批真实资格：多 endpoint/profile 共池、UDP underlay、同/跨 owner prefix、无 driver 原始转交和传递替换；实际 Proxy DNS 查询 owner 见 [DNS 记录](native-dns.md)，DoQ 原始资源与驱动链见 [DoQ 验收](native-doq.md)；尚未生产登记 | 继续闭合 system/detour、打开失败类型化重试及池化 DNS，完成业务/故障/计量/互通和可配置资源预算验收后再统一登记；不得并行建立 legacy/native 池 |
| VLESS / VMess gRPC / H2 / MUX | 协议拥有冷计划、驱动和部分受控载体库 | 实际池所有者、原生载体 provider 与 host 转交使用者闭合后登记；不能用协议 CloseReport 替代原始 provider 凭据 |
| Hysteria2 / 其他 QUIC | 现有真实认证会话与 QUIC endpoint 池 | 同一 endpoint / packet I/O / 驱动 / task / 转交用户的完整关闭链，然后使用标准角色；不能另开一个 endpoint 演示 native |
| WireGuard | 真实协议设备与 raw-IP 所有者、冷依赖图 | deferred device 阶段与实际设备替换事务接通，peer 身份与密钥仍归协议；没有确认关闭前不启动新设备 |
| SS SIP003 插件 | 真实 Child owner、readiness 取消保留、同 adapter 插件计划 | 除直接子进程 reaped 外，还需后代与转交 I/O 的真实关闭范围；保持未资格路径拒绝，不能用新 SessionOwner 包装完成 |
| 其余 UDP / packet-path | 既有中立执行桥与协议自有 framing/resume | 按同一标准登记真实角色及资源 owner，保留 datagram 原始关闭凭据、实际使用者和限额；逐路径移除 Any/旧兼容接口 |

顺序优先实际所有权闭合，再启用登记，再用真实业务与消费者验收移除旧接口。不通过新增协议分支、重写内核控制逻辑或全量删除兼容路径来压缩迁移步骤。

本轮提取的[实际验证、源码快照与剩余范围](local-runtime-20261024/validation.json)单独保存；其中全量执行产物与随后测试辅助代码清理后的严格检查源码分别标识，不能将其误读为剩余协议已完成迁移。
