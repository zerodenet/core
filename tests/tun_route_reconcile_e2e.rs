#![cfg(target_os = "windows")]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const DNS_EXCLUSION: &str = "1.1.1.1/32";

#[test]
#[ignore = "requires Administrator privileges, wintun.dll, and an alternate connected interface"]
fn windows_reconciles_runtime_egress_and_dns_exclusion_without_restarting_tun() {
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary E2E directory");
    let socket = PathBuf::from(format!(
        r"\\.\pipe\zero-tun-route-reconcile-e2e-{}",
        std::process::id()
    ));
    let running_path = directory.path().join("running.json");
    let stopped_path = directory.path().join("stopped.json");
    let port = free_tcp_port();
    std::fs::write(&running_path, config_json(true, port)).unwrap();
    std::fs::write(&stopped_path, config_json(false, port)).unwrap();

    let mut zero = ManagedZero::start(binary, &running_path, &stopped_path, &socket);
    let initial = wait_for_healthy_egress(binary, &socket, None);
    let candidate = alternate_interface(&initial)
        .expect("a lower-metric connected non-default Hyper-V interface is required");
    let temporary_route = TemporaryDefaultRoute::install(candidate.index);

    let selected = wait_for_healthy_egress(binary, &socket, Some(&candidate.name));
    assert_eq!(selected, candidate.name);
    assert_dns_exclusion_uses(candidate.index);
    assert!(
        zero.is_running(),
        "TUN process restarted or exited during reconciliation"
    );

    drop(temporary_route);
    let restored = wait_for_healthy_egress(binary, &socket, Some(&initial));
    assert_eq!(restored, initial);
    assert_dns_exclusion_does_not_use(candidate.index);
    assert!(
        zero.is_running(),
        "TUN process exited while restoring the original egress"
    );

    zero.stop();
}

struct AlternateInterface {
    index: u32,
    name: String,
}

fn alternate_interface(initial: &str) -> Option<AlternateInterface> {
    let script = r#"
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new()
$initial = $env:ZERO_TUN_INITIAL_EGRESS
$defaultIndexes = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' | ForEach-Object InterfaceIndex)
$initialRoute = Get-NetRoute -AddressFamily IPv4 -DestinationPrefix '0.0.0.0/0' |
  Where-Object InterfaceAlias -eq $initial |
  Select-Object -First 1
if ($null -eq $initialRoute) { throw "initial default route not found for $initial" }
$initialInterface = Get-NetIPInterface -AddressFamily IPv4 -InterfaceIndex $initialRoute.InterfaceIndex
$initialMetric = [int]$initialRoute.RouteMetric + [int]$initialInterface.InterfaceMetric
$candidate = Get-NetIPInterface -AddressFamily IPv4 -ConnectionState Connected |
  Where-Object {
    $_.InterfaceAlias -ne $initial -and
    $_.InterfaceIndex -ne 1 -and
    $defaultIndexes -notcontains $_.InterfaceIndex -and
    [int]$_.InterfaceMetric -lt $initialMetric
  } |
  ForEach-Object {
    $adapter = Get-NetAdapter -InterfaceIndex $_.InterfaceIndex -ErrorAction SilentlyContinue
    [pscustomobject]@{
      Index = $_.InterfaceIndex
      Name = $_.InterfaceAlias
      Metric = [int]$_.InterfaceMetric
      Preferred = if ($adapter.InterfaceDescription -match 'Hyper-V' -or $_.InterfaceAlias -like 'vEthernet*') { 0 } else { 1 }
      WireGuard = $adapter.InterfaceDescription -match 'WireGuard'
    }
  } |
  Where-Object { -not $_.WireGuard } |
  Sort-Object Preferred,Metric |
  Select-Object -First 1
if ($null -eq $candidate) { exit 3 }
Write-Output "$($candidate.Index)`t$($candidate.Name)"
"#;
    let output = powershell(script, &[("ZERO_TUN_INITIAL_EGRESS", initial)]);
    if output.status.code() == Some(3) {
        return None;
    }
    assert!(
        output.status.success(),
        "select alternate interface failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let line = String::from_utf8(output.stdout).expect("PowerShell output must be UTF-8");
    let (index, name) = line
        .trim()
        .split_once('\t')
        .expect("alternate interface output");
    Some(AlternateInterface {
        index: index.parse().expect("alternate interface index"),
        name: name.to_owned(),
    })
}

struct TemporaryDefaultRoute {
    interface_index: u32,
}

impl TemporaryDefaultRoute {
    fn install(interface_index: u32) -> Self {
        let script = r#"
$ErrorActionPreference = 'Stop'
$index = [int]$env:ZERO_TUN_ALTERNATE_INDEX
$existing = @(Get-NetRoute -AddressFamily IPv4 -InterfaceIndex $index -DestinationPrefix '0.0.0.0/0' -ErrorAction SilentlyContinue)
if ($existing.Count -ne 0) { throw "alternate interface already has a default route" }
New-NetRoute -AddressFamily IPv4 -InterfaceIndex $index -DestinationPrefix '0.0.0.0/0' -NextHop '0.0.0.0' -RouteMetric 0 -PolicyStore ActiveStore -Confirm:$false | Out-Null
"#;
        let index = interface_index.to_string();
        checked_powershell(script, &[("ZERO_TUN_ALTERNATE_INDEX", &index)]);
        Self { interface_index }
    }
}

impl Drop for TemporaryDefaultRoute {
    fn drop(&mut self) {
        let script = r#"
$index = [int]$env:ZERO_TUN_ALTERNATE_INDEX
Get-NetRoute -AddressFamily IPv4 -InterfaceIndex $index -DestinationPrefix '0.0.0.0/0' -ErrorAction SilentlyContinue |
  Where-Object { $_.NextHop -eq '0.0.0.0' -and $_.RouteMetric -eq 0 } |
  Remove-NetRoute -Confirm:$false -ErrorAction SilentlyContinue
"#;
        let index = self.interface_index.to_string();
        let _ = powershell(script, &[("ZERO_TUN_ALTERNATE_INDEX", &index)]);
    }
}

struct ManagedZero<'a> {
    child: Option<Child>,
    binary: &'a str,
    stopped_config: &'a Path,
    socket: &'a Path,
}

impl<'a> ManagedZero<'a> {
    fn start(binary: &'a str, config: &Path, stopped_config: &'a Path, socket: &'a Path) -> Self {
        let child = Command::new(binary)
            .args(["run", "--control-socket", path(socket), path(config)])
            .env(
                "ZERO_TUN_STATE_DIR",
                config.parent().unwrap().join("tun-route-state"),
            )
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn Zero");
        Self {
            child: Some(child),
            binary,
            stopped_config,
            socket,
        }
    }

    fn is_running(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|child| child.try_wait().expect("query Zero process").is_none())
    }

    fn stop(&mut self) {
        let output = Command::new(self.binary)
            .args([
                "reload",
                path(self.stopped_config),
                "--socket",
                path(self.socket),
            ])
            .output()
            .expect("stop configured TUN");
        assert!(
            output.status.success(),
            "stop configured TUN failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        wait_for_tun_stopped(self.binary, self.socket);
        self.kill();
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ManagedZero<'_> {
    fn drop(&mut self) {
        if self.child.is_some() {
            let _ = Command::new(self.binary)
                .args([
                    "reload",
                    path(self.stopped_config),
                    "--socket",
                    path(self.socket),
                ])
                .output();
            self.kill();
        }
    }
}

fn config_json(running: bool, port: u16) -> String {
    let tun = running.then(|| {
        serde_json::json!({
            "name": "ZeroTunRouteReconcileTest",
            "addr": "10.68.0.1/24",
            "tag": "tun-route-reconcile-e2e",
            "auto_route": true,
            "dual_stack": false,
            "strict_route": true,
            "dns_hijack": true
        })
    });
    serde_json::to_string_pretty(&serde_json::json!({
        "runtime": {
            "tun": tun,
            "dns": {
                "servers": { "global": { "type": "udp", "host": "1.1.1.1", "port": 53 } },
                "default_server": "global"
            }
        },
        "inbounds": [{
            "tag": "control-inbound",
            "listen": { "address": "127.0.0.1", "port": port },
            "protocol": { "type": "socks5" }
        }],
        "route": { "rules": [], "final": { "type": "direct" } }
    }))
    .unwrap()
}

fn wait_for_healthy_egress(binary: &str, socket: &Path, expected: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = tun_status(binary, socket) {
            let healthy = status.contains("tun: running") && status.contains("healthy=true");
            if healthy {
                if let Some(egress) = status_field(&status, "egress_v4") {
                    if expected.is_none_or(|expected| egress == expected) {
                        return egress;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for TUN egress {expected:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_tun_stopped(binary: &str, socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if tun_status(binary, socket).is_some_and(|status| status.contains("tun: not running")) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for TUN stop");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn tun_status(binary: &str, socket: &Path) -> Option<String> {
    Command::new(binary)
        .args(["tun", "status", "--socket", path(socket)])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
}

fn status_field(status: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    status
        .trim()
        .split(", ")
        .find_map(|part| part.strip_prefix(&prefix).map(str::to_owned))
}

fn assert_dns_exclusion_uses(interface_index: u32) {
    let script = r#"
$index = [int]$env:ZERO_TUN_ALTERNATE_INDEX
$routes = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix $env:ZERO_TUN_DNS_EXCLUSION -ErrorAction SilentlyContinue |
  Where-Object InterfaceIndex -eq $index)
if ($routes.Count -ne 1) { throw "expected one DNS exclusion on interface $index, got $($routes.Count)" }
"#;
    let index = interface_index.to_string();
    checked_powershell(
        script,
        &[
            ("ZERO_TUN_ALTERNATE_INDEX", &index),
            ("ZERO_TUN_DNS_EXCLUSION", DNS_EXCLUSION),
        ],
    );
}

fn assert_dns_exclusion_does_not_use(interface_index: u32) {
    let script = r#"
$index = [int]$env:ZERO_TUN_ALTERNATE_INDEX
$routes = @(Get-NetRoute -AddressFamily IPv4 -DestinationPrefix $env:ZERO_TUN_DNS_EXCLUSION -ErrorAction SilentlyContinue |
  Where-Object InterfaceIndex -eq $index)
if ($routes.Count -ne 0) { throw "stale DNS exclusion remains on interface $index" }
"#;
    let index = interface_index.to_string();
    checked_powershell(
        script,
        &[
            ("ZERO_TUN_ALTERNATE_INDEX", &index),
            ("ZERO_TUN_DNS_EXCLUSION", DNS_EXCLUSION),
        ],
    );
}

fn checked_powershell(script: &str, environment: &[(&str, &str)]) {
    let output = powershell(script, environment);
    assert!(
        output.status.success(),
        "PowerShell assertion failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn powershell(script: &str, environment: &[(&str, &str)]) -> std::process::Output {
    let mut command = Command::new("powershell");
    command.args(["-NoProfile", "-NonInteractive", "-Command", script]);
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("run PowerShell")
}

fn free_tcp_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn path(path: &Path) -> &str {
    path.to_str().expect("path must be UTF-8")
}
