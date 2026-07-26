# CLI 控制命令

## 守护进程

```bash
zero run [--status-listen HOST:PORT] [--control-socket PATH] [--ipc-hook-socket PATH] [CONFIG_PATH]
```

| 选项 | 说明 |
|------|------|
| `--status-listen HOST:PORT` | HTTP 控制接口监听地址 |
| `--control-socket PATH` | IPC socket 路径（覆盖默认） |
| `--ipc-hook-socket PATH` | IPC flow hook socket（覆盖配置） |

IPC server 始终启动（不需要额外选项），默认路径：
- Linux/macOS: `~/.zero/control.sock`
- Windows: `\\.\pipe\zero-control`

## 控制命令

所有命令自动发现并连接运行中的 zero 守护进程。

### zero status

```bash
zero status               # 人类可读格式
zero status --json        # JSON 格式
zero status --socket /tmp/zero.sock  # 指定 socket
```

离线模式：指定配置路径时直接读取配置文件（不连接守护进程）：

```bash
zero status config.json
```

### zero select

切换 selector 出站。

```bash
zero select proxy direct           # 将 proxy 组切换到 direct
zero select --socket /tmp/zero.sock proxy server-a
```

### zero flows

查询活动流列表（JSON）。

```bash
zero flows
```

### zero policies

查询所有策略组状态（JSON）。

```bash
zero policies
```

### zero events

实时追踪事件流（JSON-line，Ctrl-C 退出）。

```bash
zero events
```

输出示例：
```json
{"event_type":"flow.snapshot","event_id":"...","occurred_at_unix_ms":...,"payload":{"watermark":1024,"records":[...]}}
{"event_type":"flow.started","event_id":"...","occurred_at_unix_ms":...,"payload":{...}}
{"event_type":"flow.routed","event_id":"...","occurred_at_unix_ms":...,"payload":{...}}
{"event_type":"flow.updated","event_id":"...","occurred_at_unix_ms":...,"payload":{...}}
{"event_type":"flow.completed","event_id":"...","occurred_at_unix_ms":...,"payload":{...}}
```

`zero events` 建立实时订阅后先输出 `flow.snapshot` 活动连接基线，随后输出生命周期增量。每个生命周期 payload 的 `record` 都使用统一 `FlowRecord`；完成记录是自包含事实。

### zero help

```bash
zero help
```

### zero build_info

```bash
zero build_info
zero version
zero -V
zero --version
```

显示构建信息：

```
build_id: <build-id>
build_time: <build-time>
build_profile: <debug-or-release>
features: <comma-separated-compiled-features>
binary_sha256: <sha256-of-this-executable>
git: <git-describe>
git_hash: <source-commit>
```

`git` 保留便于人工识别的 tag/describe；`git_hash` 始终独立输出源码 commit，供发布候选、长稳 manifest 和升级记录做机器核对。`build_profile` 与 `features` 证明产物本身的优化等级和编译能力，不能用外部文件替候选二进制声明。`binary_sha256` 由正在执行的 Zero 进程读取自身可执行文件计算，归档时必须与外部哈希复核。

### zero validate

校验配置文件有效性（离线，不连接守护进程）：

```bash
zero validate config.json
```

成功输出：

```
config valid: 2 inbounds, 3 outbounds, 1 groups, 5 rules
```

失败时打印错误详情并以退出码 1 退出。

### zero mode

运行时模式热切换，即时生效：

```bash
zero mode rule              # 规则模式
zero mode direct            # 全部直连
zero mode global proxy      # 全局走 proxy 出站
```

IPC 等价命令：

```json
{ "method": "mode.set", "params": { "mode": "global", "outbound": "proxy" } }
```

### zero reload

热重载配置文件：

```bash
zero reload config.json
```

支持热换的部分：
- route 规则、mode、DNS 配置
- outbound_groups 调整

需要重启后生效：
- inbounds/outbounds 增删改

### zero tun

TUN 虚拟网卡管理：

```bash
zero tun start --addr 10.0.0.1 --tag my-tun    # 启动
zero tun start --addr 10.0.0.1 --tag my-tun --name tun0 --mask 255.255.255.0 --mtu 1500
zero tun stop                                   # 停止
zero tun status                                 # 查看状态
```

参数说明：
- `--addr` — 必填，虚拟网卡 IP 地址
- `--tag` — 必填，入站标签，用于路由决策
- `--name` — 可选，OS 级设备名（如 `tun0`、`utun8`），省略自动分配
- `--mask` — 可选，子网掩码，默认 `255.255.255.0`
- `--mtu` — 可选，MTU 字节数，默认 `1500`

## 退出码

| 码 | 说明 |
|-----|------|
| 0 | 成功 |
| 1 | 错误（socket 不存在、命令失败等） |
