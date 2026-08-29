# TUN 模式与端到端验证

Zero 的 TUN 模式面向 Linux、macOS 和 Windows。`tun start` 会创建并配置设备、安装事务化系统路由，并将 TUN TCP/UDP 送入与普通入站相同的路由、出站、会话、统计和 block 管线；不需要提前手工创建 TUN 设备。

## 前置条件

| 平台 | 要求 |
|------|------|
| Linux | root，或具备创建 TUN、配置接口、路由和 nftables 所需的 capabilities；系统需提供 `ip` 与 `nft` |
| macOS | root；系统需提供 `/sbin/ifconfig`、`/sbin/route`、`pfctl`，主规则集需执行 `com.apple/*` anchor |
| Windows | Administrator；需提供 Windows Firewall PowerShell cmdlet；官方 Windows x86_64 发布包已内置匹配架构的 `wintun.dll`，源码构建时需自行将 DLL 放在 `zero.exe` 同目录或 `PATH` |

默认行为：

- `auto_route=true`：通过两条 `/1` 路由接管默认流量，不覆盖原默认路由；
- `include_cidrs=[]` 保持上述全隧道起点；非空时只从列出的目的 CIDR 开始。`exclude_cidrs` 从这个集合中排除通用目的网络，两者最多各 128 项、不得重复，并要求 `auto_route=true`；编译结果最多 512 条路由且至少保留一个已配置地址族；
- `auto_route=true` 会继续监听系统默认路由和接口变化；Wi-Fi/有线切换、VPN 上下线或 metric 变化后，内核在不重启 TUN 的情况下更新新建 socket 使用的物理出口和仍需保留的 DNS bootstrap 排除路由。事件会合并处理；协调失败时非严格模式保留上一份可用状态，严格模式进入 fail-closed，两者都会在 `tun status` 中报告错误；
- `dual_stack=true`：在同一设备上配置 IPv4/IPv6 两个地址，并同时安装两组拆分默认路由；`secondary_addr` 可显式指定另一族 CIDR，省略时按主地址族使用 `10.66.0.1/24` 或 `fd66::1/64`；只有明确的单栈主机才应关闭，否则另一地址族仍可能绕过 TUN；
- `strict_route=true`：underlay 出口、仍需保留的 DNS bootstrap 排除、任一半默认路由或平台 kill switch 失败时，启动整体失败并回滚已安装项；运行期 route monitor、bootstrap 重算或路由/防火墙协调失败时撤销受影响地址族的出口发布，使新建 TCP、UDP 与 DNS socket fail closed，协调恢复后再发布新出口；未选中的坏代理节点不影响 TUN 启动；
- `dns_hijack=true`：TUN 内的 UDP/TCP 53 由 Zero DNS 回答，并使用已有缓存、DNS 路由和 Fake-IP；
- 代理节点不再获得自动 host route；Zero 创建的 TCP/UDP/QUIC socket 按 IPv4/IPv6 分别绑定各自 underlay 接口，避免代理自环。DNS UDP 与 DoT socket 使用同一出口权威；DoH/系统 bootstrap 仍由显式 DNS 排除或操作系统解析路径保护。
- 物理出口或受管 TUN 地址集合每次实际变化都会发布单调递增的出口 generation。direct UDP 在下一次发送前发现旧 generation 时重建双栈 socket；重建期间若拓扑持续抖动则 fail closed，不使用旧出口继续发送。新建域名 UDP flow 在解析器首选的可用地址族内采用稳定候选，DNS 答案重排不会把所有流量重新固定到第一条记录。
- macOS 会为每个受管地址族维护物理出口的 interface-scoped 默认路由，使绑定接口的 direct、代理节点和 DNS socket 在全局 `/1` 路由生效后仍可达；该路由参与出口切换、回滚和崩溃恢复。
- 路由恢复日志按稳定的 TUN 入站 `tag` 与地址族寻址，并记录当次真实设备名；因此 macOS 在崩溃重启后即使 `utunN` 编号变化，也能清理旧设备留下的路由。系统路由 lease 则按地址族全局持有，第二个进程即使使用不同 tag，也必须明确失败且报告当前 owner，不能同时改写同一组捕获路由。

调试时可分别使用 `--no-auto-route`、`--single-stack`、`--no-strict-route` 或 `--no-dns-hijack`。`--include-cidr CIDR` 与 `--exclude-cidr CIDR` 均可重复传入以验证选择性接管。生产防泄露验证不应关闭 strict route。Linux smoke case 还会用与 Zero 相同的有效 UID 创建一个未打 `SO_MARK`、强制绑定物理网卡的 TCP socket；该 socket 必须被 nftables kill switch 拒绝，而紧邻的受管 TUN TCP 请求必须成功，从而同时验证“同 UID 不继承例外”和 Zero 自身 underlay 身份链。

## DNS 前置约束

严格 DNS 劫持要求至少配置一个非 system DNS 后端。域名形式的上游必须提供 `bootstrap` IP；Zero 会为实际端点建立明确的物理出口排除，避免解析 DNS 上游本身时递归进入 TUN。

示例：

```json
{
  "runtime": {
    "tun": {
      "name": "ZeroTun",
      "addr": "10.66.0.1/24",
      "secondary_addr": "fd66::1/64",
      "tag": "tun-in",
      "auto_route": true,
      "include_cidrs": [],
      "exclude_cidrs": [],
      "dual_stack": true,
      "strict_route": true,
      "dns_hijack": true
    },
    "dns": {
      "servers": {
        "global": { "type": "udp", "host": "1.1.1.1", "port": 53 },
        "fallback": { "type": "udp", "host": "8.8.8.8", "port": 53 }
      },
      "default_server": "global",
      "policy": {
        "timeout_ms": 3000,
        "fallback_servers": ["fallback"],
        "node_server": "global",
        "node_fallback_servers": ["fallback"],
        "direct_server": "global",
        "direct_fallback_servers": ["fallback"],
        "address_family": "prefer_ipv4"
      },
      "cache": { "max_entries": 1024 },
      "answer": {
        "type": "fake_ip",
        "cidr": "198.18.0.0/15",
        "ipv6_cidr": "fd00::/96",
        "ttl_seconds": 86400,
        "exclude_domains": []
      }
    }
  },
  "outbounds": [
    { "tag": "proxy", "protocol": { "type": "socks5", "server": "192.0.2.10", "port": 1080 } }
  ],
  "route": {
    "rules": [],
    "final": { "type": "route", "outbound": "proxy" }
  }
}
```

`dns.servers` 使用稳定名称，`default_server` 处理未命中查询。需要 split DNS 时使用有序的 `dns.dispatch`；每条规则复用流量路由的 `condition` 结构并首先查询选中的后端。只有 `dns.policy.fallback_servers` 声明的后端才会在超时、传输错误、畸形响应、命中 `reject_address_cidrs` 或可重试 RCODE 后按顺序使用；NXDOMAIN 不回退。`server_timeout_ms` 可按服务器覆盖全局超时。`node_server`/`node_fallback_servers` 隔离代理节点与 QUIC carrier 解析，`direct_server`/`direct_fallback_servers` 隔离 direct 目标解析，三类查询使用独立 cache key；省略角色字段时保持历史 dispatch/default 行为。网络 DNS server 可用 `detour` 指向现有 outbound/outbound group；UDP 在 detour 下使用 DNS-over-TCP，DoH/DoT 通过同一 TCP 出站桥，detour 端点不再加入物理 TUN host-route 排除。只要存在 detour 就必须显式配置无 detour 的 `node_server`，其 fallback 也不得使用 detour，以阻断代理节点解析递归；DoQ detour 当前会在配置校验时拒绝，不会静默直连泄露。`address_family` 控制只查询单一地址族或双栈结果优先级：`prefer_ipv4`/`prefer_ipv6` 保留双栈候选，`ipv4_only` 可作为禁用 IPv6 的客户端兼容模式，`ipv6_only` 则明确禁止 IPv4 回退。当透明 TUN 会话保留了可信域名、原始 direct 目标为 IPv6、且当前物理 IPv6 出口明确不可用时，双栈或 IPv4 模式会通过 direct DNS 角色重新取得 A 记录；路由动作仍为 `direct`，不会改走 proxy。没有域名来源的 IPv6 字面地址不会被合成转换。可用于 DNS 的条件包括 `domain`、`domain_keyword`、`domain_regex`、域名规则集以及 `and`/`or`。共享规则集继续声明在历史位置 `route.rule_sets`，可同时由 `route.rules` 和 `dns.dispatch` 引用。

当前不会合成 NAT64 地址；`ipv6_only` 表示纯 IPv6，而不是隐式 DNS64/NAT64。可信域名的原始 IPv6 目标在物理 IPv6 明确不可用时只做一次 A 查询；物理 IPv6 路由存在但候选实际不可达时，原始 IPv6 与这次 A 查询得到的 IPv4 候选采用有界 Happy Eyeballs 竞速，只有 IPv4 真正胜出才记录回退。

Fake-IP 使用 `answer.type = "fake_ip"` 开启，默认 IPv4 地址池为 `198.18.0.0/15`，也可通过 `answer.cidr` 覆盖；`answer.ipv6_cidr` 启用 AAAA 合成。双池共享域名规范化、TTL、容量和 LRU 生命周期。任一地址池与 TUN 主地址、双栈辅助地址或平台使用的相邻 TUN gateway 冲突时，配置应用或 `tun.start` 会明确失败，不会把冲突地址投入运行。

DNS 后端和所选代理协议还必须由当前构建启用。TUN 启动需要 `zero-proxy` 的 `udp-runtime`；默认 `full` 构建已满足。

## 启动与状态

推荐在 `runtime.tun` 中声明 TUN。此时 TUN 与代理监听器一起启动，`config.apply` 会事务化地新增、替换或移除设备；新设备或路由安装失败时恢复上一份配置和 TUN。配置管理的 TUN 不能通过 `tun stop` 单独停止，应删除 `runtime.tun` 并应用配置。`mtu` 可在 `runtime.tun.mtu` 单独覆盖，否则继承 `runtime.network.mtu`。双栈部署建议显式填写另一族的 `secondary_addr`（必须为 CIDR 且与 `addr` 地址族不同）；省略时使用上述默认地址。

配置方式只需要启动主进程：

```bash
cargo run -- run config.json
cargo run -- tun status
```

也可省略 `runtime.tun`，再通过控制命令临时启停 TUN：

先启动 Zero 主进程，再通过同一控制 socket 启动 TUN：

```bash
cargo run -- run config.json
```

在另一个终端执行：

```bash
cargo run -- tun start --addr 10.66.0.1/24 --secondary-addr fd66::1/64 --tag tun-in
cargo run -- tun status
```

状态应包含：

```text
tun: running, healthy=true, managed_by_config=true, name=..., addr=10.66.0.1/24, addresses=10.66.0.1/24,fd66::1/64, mtu=1500, tag=tun-in, auto_route=true, include_cidrs=full-tunnel, exclude_cidrs=-, dual_stack=true, strict_route=true, dns_hijack=true, egress=..., egress_v4=..., egress_v6=...
```

停止并确认清理：

```bash
cargo run -- tun stop
cargo run -- tun status
```

如使用非默认控制 socket，三个命令都传入 `--socket PATH`。

## TCP、UDP 与 DNS 验证

### Linux

```bash
ip address show
ip -4 route show 0.0.0.0/1
ip -4 route show 128.0.0.0/1
curl https://example.com/
dig @8.8.8.8 example.com A
```

`dig` 的目标地址可以是任意 DNS 地址；启用 DNS 劫持时 UDP/TCP 53 查询不会发往该目标，而是由 Zero DNS 返回。启用 Fake-IP 后，A 响应应位于配置的 Fake-IP 网段，后续到该地址的 TCP/UDP 会在路由决策前恢复为原域名。

持续向同一 UDP 目标发送请求时，会话 API 应只保留同一源 tuple 对应的一条活动 flow，并显示应用的真实 TUN 源 IP/端口，而不是 `127.0.0.1:0`。同一 TUN 源端口访问多个 direct 目标时，目标观测到的 outbound 源端口应保持一致，并在端口可用时等于客户端源端口；预先占用该端口后应回退到稳定的临时端口而不是使 association 失败。向该映射端口注入来自未建立远端 IP/端口的 datagram 必须被丢弃，不能写回 TUN 客户端。制造大量不同源端口或让目标持续发送失败时，活动 association 数必须保持有界；容量、速率、重建退避或关闭队列导致的新 datagram 被明确拒绝时，IPv4 客户端应收到 type 3/code 13，IPv6 客户端应收到 type 1/code 1，且 quoted packet 对应原 UDP tuple。ICMP 反馈通道饱和时允许丢弃反馈，不能阻塞 TUN 收包。日志可以出现限速丢包或退避，但不能出现单个目标每包创建一个新 session 的单调增长，也不能演化为 `WSAENOBUFS` 紧密循环。

### macOS

```bash
ifconfig | grep -A4 '^utun'
route -n get -inet 0.0.0.0
curl https://example.com/
dig @8.8.8.8 example.com A
```

### Windows（管理员 PowerShell）

```powershell
Get-NetAdapter -Name ZeroTun
Get-NetRoute -PolicyStore ActiveStore | Where-Object DestinationPrefix -in '0.0.0.0/1','128.0.0.0/1'
curl.exe https://example.com/
Resolve-DnsName example.com
```

strict route 启动、运行、强杀恢复和正常停止前后，Domain、Private、Public 三个 Windows Firewall profile 的 `DefaultOutboundAction` 必须保持不变。Windows 防泄露策略位于稳定的持久 WFP sublayer：高权重规则只放行 Zero AppID、Wintun LUID、loopback 和显式排除地址，低权重规则阻断受管前缀；删除任一受管 filter 后，30 秒 watchdog 必须原子重建完整策略。若检测到旧版 `zero.tun.leak-guard.v1` 恢复日志，启动时只执行一次旧 profile/rule group 恢复和迁移，此后不再修改全局 profile。

## 网络生命周期验证

在保持 TUN 运行的情况下依次执行休眠/唤醒、DHCP 续租、IPv6 前缀变化，以及连接和断开企业 VPN。每次变化后立即检查 `tun status` 和默认路由；平台事件正常时应在防抖窗口后收敛，即使平台未投递通知，也必须在 30 秒周期审计窗口内更新物理出口、DNS bootstrap 排除路由和严格模式防泄露规则。旧出口与新出口相同时不得递增出口 generation 或重建 UDP socket。

临时移除所有可用物理默认路由时，`strict_route` 必须撤回对应受管出口并报告 unhealthy；恢复网络后应按有界退避自动回到 healthy，不需要重启 TUN。模拟协调失败时必须保留上一份可用路由/防泄露事务，不能留下部分更新。

## WebRTC/STUN 防泄露验证

TUN 能接管浏览器发出的 STUN UDP，但最终是否暴露真实公网出口仍由 Zero 路由策略决定：

- STUN 命中支持 UDP 的代理出站：server-reflexive candidate 应是代理出口；
- STUN 命中 `block`：不应产生对应 candidate；
- STUN 命中 `direct`：会使用真实物理出口，这是显式策略，不属于 TUN 绕过。

验证时打开 WebRTC candidate 测试页或建立实际 PeerConnection，同时在物理接口抓包：

```bash
# Linux/macOS；把 physical0 换成 tun status 显示的 egress
sudo tcpdump -i physical0 -nn 'udp port 3478 or udp port 5349'
```

物理接口上不应出现浏览器到公共 STUN 服务器的直连包；允许出现配置的代理节点或 DNS 上游流量。再检查 `zero status`/会话 API，确认 TUN UDP 会话带有 `tun-in` 入站标签并记录路由、流量和结果。

## 回滚与故障验证

1. 启动 TUN 后记录两条半默认路由，并确认只存在明确需要的 DNS bootstrap 主机排除路由，不存在仅用于代理反环路的节点 host route。
2. 执行 `tun stop`，确认半默认路由消失、原默认路由仍在。
3. 使用无效接口权限或预置冲突 `/1` 路由再次启动；严格模式应返回错误，且第一条已安装路由必须回滚。
4. 强制中止 TUN 数据通道；`tun status` 应变为 `running=false` 并显示 `last_error`。
5. 保持 TUN 运行并切换系统默认出口；`tun status` 的 `egress_v4`/`egress_v6` 应更新，既有连接不中断，新连接使用新出口，旧出口上的 Zero DNS/bootstrap 排除路由被清理。
6. 制造短暂的无默认路由窗口或排除路由安装失败；TUN 应保持运行、`healthy=false` 且显示 `last_error`，恢复后自动协调并回到 `healthy=true`，期间不得出现忙重试。
7. Windows 连续执行两轮 start/stop，确认阻塞 reader 被唤醒且同名 Wintun adapter 可复用。
8. 严格模式下删除一条受管 `/1` 路由或制造协调失败，并确认平台保护仍阻止非 TUN 物理出站：Linux 检查 `zero_killswitch_*` nftables 表，macOS 检查 `com.apple/zero_*` pf anchor，Windows 枚举稳定 WFP sublayer 的 ALE connect filters，并再次确认各 Firewall profile 的默认出站策略未变化。
9. 保持一个实例持有 IPv4/IPv6 自动路由，再以不同 TUN tag 启动第二个实例；第二个实例必须在安装任何路由或 kill switch 前以 `AlreadyExists` 失败。停止第一个实例后，第二个实例应能取得 lease 并正常启动。

路由事务会写入恢复日志。默认位置为 Windows 的 `%LOCALAPPDATA%\\Zero\\run`、Unix 的 `$XDG_RUNTIME_DIR/zero` 或 `/run/zero`，不可用时回退到系统临时目录的 `zero` 子目录；可用 `ZERO_TUN_STATE_DIR` 指定隔离目录。进程被强杀后，kill switch 保持 fail-closed；下一次以相同 TUN 入站 `tag` 启动会接管原保护并消费路由日志。Windows WFP sublayer/filter 使用稳定 key 和持久对象原位接管，Linux nftables table 与 macOS pf anchor 使用稳定资源名原位替换；热更新失败均保留旧保护。

## 自动化覆盖

- `zero-tun`：并发读写、掩码、分流默认路由计划；
- `zero-stack`：UDP 队列等待与原始 IPv4/IPv6 数据包；
- `zero-dns`：DNS wire query、Fake-IP 和 EDNS 计数处理；
- `zero-proxy`：TUN UDP direct、block、防网络泄露以及 DNS/Fake-IP 响应；
- 平台 CI：Linux、macOS、Windows 原生检查并编译特权 E2E harness。

透明 TCP 嗅探覆盖 TLS SNI 与 HTTP/1.x Host。所有已读取前缀必须原样回放；
ECH 连接不得把 outer public name 猜作真实目标，先尝试无歧义 DNS 反向映射，
否则继续使用原始 IP。QUIC Initial/SNI 使用 `zero-transport` 中的 RFC 9001/9369
Initial 解密与 CRYPTO 帧有界重组，TUN UDP 仅负责 200ms/容量受限的暂存和
Session 元数据桥接；不能用
TCP ClientHello 解析器或单包字符串扫描替代。

真实路由和设备操作需要管理员权限，不能由普通单元测试模拟。仓库中的 `tun_privileged_e2e` 会直接读取系统网络状态，校验接口 MTU、地址、拆分默认路由、TCP、DNS 劫持、STUN 基线与 block、强杀后的恢复日志，以及配置移除后的设备和日志清理。IPv4/IPv6 STUN 用例分别以单栈配置运行，需要对应地址族的原生网络和可达 STUN 服务。独立的离线双栈用例通过本地模拟 DNS 和 SOCKS5 出站，验证同一设备的 IPv4/IPv6 地址、四条 `/1` 路由、双族 TCP/DNS 流量和两份恢复日志；它也覆盖物理出口仅有 IPv4 时 IPv6 经代理载荷出站的情形。为公网 STUN 用例提供可达服务并运行：

```bash
ZERO_TUN_E2E_STUN_ADDR=203.0.113.10:3478 \
  cargo test --test tun_privileged_e2e privileged_tun_ipv4_config_reload_stun_block_and_crash_recovery -- --ignored --exact --nocapture
ZERO_TUN_E2E_STUN_ADDR_V6='[2001:db8::10]:3478' \
  cargo test --test tun_privileged_e2e privileged_tun_ipv6_config_reload_stun_block_and_crash_recovery -- --ignored --exact --nocapture
cargo test --test tun_privileged_e2e privileged_tun_dual_stack_configuration_traffic_and_crash_recovery -- --ignored --exact --nocapture
```

运行中的默认出口协调另有独立特权用例。Windows 用例创建临时默认路由并验证状态与 DNS 排除路由迁移；Linux 用例在隔离 network namespace 中切换两个虚拟物理出口：

```powershell
cargo test --test tun_route_reconcile_e2e windows_reconciles_runtime_egress_and_dns_exclusion_without_restarting_tun -- --ignored --exact --nocapture
```

```bash
sudo cargo test --test tun_route_reconcile_linux_e2e linux_reconciles_runtime_egress_and_dns_exclusion_inside_network_namespace -- --ignored --exact --nocapture
```

Linux namespace 用例在完成受管出口切换后，还会以 `auto_route=false` 重启 TUN，重复触发默认路由变化，并确认没有 `/1` 或 DNS host route 被 Zero 安装、状态也不发布内核受管出口。

macOS 的 PF_ROUTE 生命周期用例只增删一条 `lo0` TEST-NET 主机路由，不变更默认出口；仍应只在隔离 runner 上以 root 执行：

```bash
sudo cargo test -p zero-tun --test route_monitor_macos_e2e macos_route_monitor_observes_repeated_route_changes_and_releases_cleanly -- --ignored --exact --nocapture
```

macOS 动态出口用例会暂时修改全局 IPv4 默认路由，并由 RAII 恢复，因此只能在隔离 runner 上运行。备用网关必须已经通过备用接口直连可达：

```bash
sudo env \
  ZERO_TUN_E2E_MACOS_SECONDARY_GATEWAY=192.0.2.1 \
  ZERO_TUN_E2E_MACOS_SECONDARY_INTERFACE=en1 \
  cargo test --test tun_route_reconcile_macos_e2e macos_reconciles_runtime_egress_and_dns_exclusion_without_restarting_tun -- --ignored --exact --nocapture
```

`.github/workflows/tun-e2e.yml` 将冒烟、两个单栈 STUN 和离线双栈用例分发到带 `tun` 标签的 Linux、macOS 和 Windows 隔离自托管 runner。STUN 用例需要对应地址族的原生连通性和可达服务，离线双栈用例不作此要求；Windows runner 还需预装匹配架构的 Wintun。
