# v0.0.3 第二轮云端任务包

2026-11-05 用户授权：将当前本地验收增量分组提交并推送新云端基线，在一个云端环境内继续多 agent 架构迁移，最后交回本地验收。只改内核，不合入 develop/main，不发布或部署。此授权覆盖前一阶段“不提交、不推送”，不覆盖消费者项目修改。

本包是第二轮执行入口。上轮 [任务包](../README.md)、[执行账本](../execution.md)及原 17 张[任务卡](../../task-cards.md)保留历史和技术标准；它们的旧分支、主 agent C 名称和“未开始”状态不作为本轮调度指令。本轮主 agent 叫 **M**，A/B/C 均为实现子 agent。

## 起点和交付

- 仓库仅限 `zerodenet/core`。实际 remote 的 URL 必须指向该 GitHub 仓库；不能凭 remote 名叫 origin 就向其他服务器推送。
- 产品阶段 v0.0.3。新基线 `codex/v003-cloud-r2-base`，新集成分支 `codex/v003-cloud-r2-integration`；各角色分支见下表。
- [baseline.json](baseline.json) 固定上一轮候选、四组代码提交、当前源码摘要、已有验证和未验证范围。基线分支包含其后的本任务包提交；M 开工时从远端解析该分支完整 SHA 并冻结，所有 agent 记录同一个包 head，不能误从 code checkpoint 或旧云端 head 开工。
- 本地 develop 保持 `bab00b6922600ecdc9461c28ddcf0630b8c730c8`。远端 develop 在准备时为 `5338c727acee537762cfdd95531f6e5130274200`，只作观察，不合并、变基、重置或强推任一 develop。
- 旧云端候选 `66902032e0e4878a10974559fc59b2b4e49d6c98` 已包含上轮 A 的 Config/Engine 职责迁出和 B 的指定 HY2 出站迁移。本轮不重做这些职责，不重建已删除 listener/claim/handler/Any/resume 链。
- 当前新基线已保存本地 HY2 fixture 修复、设备撤除/回滚 TCP 连续性修复、平台原始所有权基础和 DNS 宿主增量；Z003-01 尚未关闭。

## 单环境调度

```text
一个云端环境（一次工具链、凭据和依赖初始化）
  M：主 checkout / cloud-r2-integration，契约唯一写入、构建排队、审查和集成
    A：独立 worktree / cloud-r2-dns
    B：独立 worktree / cloud-r2-outbound
    C：独立 worktree / cloud-r2-device
  首波交付后复用空闲 agent，执行 control 和 budgets 后续任务
```

不是四个云端环境。不得在同一个可写工作区让多个 agent 切分支；下载缓存及默认 Cargo target 可共享，但同一时刻只运行一组构建及对应测试。轻量源码检查可以并行。工具缺少子 agent 能力时如实报告，不能假称已派发；不要要求用户再开三套环境。

## 分工、分支与依赖

所有实现 PR 的 base 都是 `codex/v003-cloud-r2-integration`。初始分支在交接时与基线同 head，不代表实现已启动或已有可创建的非空 PR。

| 角色 | 分支 | 完整交付域 | 依赖及集成条件 |
| --- | --- | --- | --- |
| [A](agent-a.md) | `codex/v003-cloud-r2-dns` | Z003-01：剩余 DNS 所有权和旧执行链删除 | DNS 自身可先做；资源型 detour 消费 B 的实际标准角色/关闭链。相关部分 B → A，不能让 A 另做一套出站池。 |
| [B](agent-b.md) | `codex/v003-cloud-r2-outbound` | Z003-02/03/06：其余既有出站、共享池/载体及旧投影收敛 | 先交 A/C 所需的最小真实原件转交，再迁完本域全部调用者。公共契约/登记前置由 M 唯一提交。 |
| [C](agent-c.md) | `codex/v003-cloud-r2-device` | Z003-04/05：非入站 Packet、设备/TUN、适用进程 | 独立部分可先集成；依赖 B 的 provider 时记录具体前置提交。TEMP-WG-OUTER-UDP-01 不在实现范围。 |
| [控制后续](followup.md) | `codex/v003-cloud-r2-control` | G8、控制/订阅生命周期及独立 TLS/quota/存储边界 | M 在首波对应 agent 释放后，转交一个空闲 agent，按实际资源前置串行集成。 |
| [预算后续](followup.md) | `codex/v003-cloud-r2-budgets` | Z003-07 适用资源上限生产配置 | 各实际 owner 先列预算，等对应所有权实现稳定后整合。配置/公共投影只由 M 指定的一名负责人写入。 |

```text
冻结 r2-base → 公共契约/登记前置（M）
                    ├── B：实际出站原件 ──→ A：DNS 全部消费端及旧链删除
                    └── C：设备域（若借用 B，则增加明确 B → C 边）
                  资源域稳定 → control / budgets → 冻结统一候选 → 本地验收
```

推荐先合入 B 的必要前置，再合 A；C 的独立部分无需等完整 B。后续 PR 更新时普通合入当前 integration，保留依赖历史，不强推或改写其他 agent 已依赖的提交。仅 M 可以写入集成分支，A/B/C 不自行 merge PR。

## 文件所有权

下表是写入权限，不是允许重做整个目录的功能。入站已完成的行为和其他协议职责保持不动；新增文件归所在父域。共享文件确需修改，由提出者给 M 提交具体签名/补丁及调用者影响，M 在一个唯一前置提交实现后，各 agent 合入使用。不得靠重复实现绕开边界。

| 唯一负责人 | 默认文件范围 |
| --- | --- |
| A | `crates/dns/`；`crates/proxy/src/component/native_dns.rs`、`component/native_dns/`；`runtime/dns_outbound.rs`、`runtime/dns_outbound/`；`component/tests/mieru/dns.rs`、`component/tests/mieru/dns/` 和对应 DNS 测试 |
| B | `protocols/{socks5,vless,vmess,trojan,shadowsocks,mieru,hysteria2}` 中仍未迁完的**出站**实现；对应 proxy adapter 的出站投影；`component/background.rs`、`component/background/`、`component/stream_bridge/`；`inventory/tcp/`、`inventory/udp/` 及 TCP/UDP 执行子模块和对应出站测试 |
| C | `crates/tun/`、`crates/stack/`；`protocols/wireguard` 与 proxy WireGuard/Direct adapter 的**设备/Packet**职责；`adapters/devices.rs`、`adapters/devices/`；`runtime/raw_ip/`、`runtime/packet_route.rs`、`runtime/packet_route/`、`runtime/orchestration/devices.rs`；对应设备/TUN/endpoint-contract 测试 |
| M | 所有 crates 根 `Cargo.toml`/Cargo.lock、feature、`AGENTS.md`、全局守卫、CI 和设计总表；`crates/{traits,core,error,platform/tokio,transport,runtime,config,engine,api,ffi,grpc,connector}`；`src/`；`proxy/src/register.rs`、`lib.rs`、`inventory/service.rs`、`inventory/service/`、`inventory/runtime.rs`、`protocol_registry/`、`runtime/tcp_dispatch/service.rs`、`runtime/tcp_dispatch/service/`、公共 catalog/slot/生命周期与 handle/reconcile 装配入口 |

M 保留共享范围是为了唯一写入，不是独自实现所有工作。第二波必须用明确文件清单将控制/预算子域临时转交一个空闲 agent，在其交付前 M 不并行修改同文件；例外分配写入集成进度记录。A/B/C 提出的共享改动不能静默扩大成第二 SPI、协议专用 runtime 分支或第二池。

## 完成口径

- 本轮覆盖现有剩余职责域；不要以一个协议/载体/前缀组合的成功替代整域交付。先冻结既有生产调用者清单，逐项填迁移、删除及必要回归，不新增协议组合或升级上游。
- 实际标准角色必须进入普通 Proxy 装配、派发和计量。仅受控 assembly、准备接口、声明或测试入口不算生产迁移。
- 原 owner、任务监督、logical custody、原始 provider 凭据和依赖替换分别确认。先确认停旧，再构造/启动替代；取消、超时、abort、Drop、EOF 或端口可复用均不能证明关闭。
- 只有 TEMP-WG-OUTER-UDP-01 是用户明确保留的临时债务；其他遗漏只能记为未完成/阻断，不能发明新豁免或靠拒绝原来可用的配置结题。
- 生产预算按实际 owner 补齐，保守默认、按需分配，容量耗尽拒绝新增；不淘汰活跃业务或有效安全历史，不新增通用调度器。
- 交付按[验收标准](acceptance.md)及[协调规则](coordinator.md)执行。实现完成、云端检查、本地资格、最终合入分别报告；不以测试数量计算完成率。

## 兼容与本地边界

只改内核。保留 V1 配置省略字段导出形状、API/events、CLI、FFI、发送状态、计量、控制 ACK/revision 及客户端/面板既有语义。Rust 内部迁移按既有[兼容说明](../../compatibility.md)记账；必要可选资源上限沿既定配置机制增加，不要求消费者改造。不得修改 GUI、面板、其他仓库或第三方业务模型。

云端负责可运行的架构实现和验证；真实 GUI/面板同产物、特权 TUN/系统路由 DNS 恢复、跨平台及 1 CPU/1 GiB 资格按实际环境完成，缺条件逐项保留。不得修改用户正在运行的内核或系统网络。TEMP-WG-OUTER-UDP-01 单独未关闭时不能宣称 G0–G10 全部满足。

启动时直接使用 [启动提示词](launch-prompt.txt)。本包推送只准备工作入口，没有启动云端执行器。
