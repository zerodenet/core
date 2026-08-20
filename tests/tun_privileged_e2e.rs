use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use socket2::{Domain, Protocol, Socket, Type};

static TUN_E2E_LOCK: Mutex<()> = Mutex::new(());

#[test]
#[ignore = "requires administrator/root, a TUN backend, and internet access"]
fn privileged_tun_ipv4_smoke_tcp_dns_and_crash_recovery() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let tcp_target = resolve_tcp_target(false);
    let direct_config = config_json(false, listen_port, None, true, false);
    let stopped_config = config_json(false, listen_port, None, false, false);
    let direct_path = directory.path().join("direct.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let _initial_name = assert_tun_os_configured(binary, &socket, false, false);
        for _ in 0..8 {
            assert_tcp_through_tun(tcp_target);
        }
        assert_dns_hijack_through_tun(false);

        // A hard kill leaves the route journal behind. The next process must
        // recover it before re-installing the same TUN routes.
        process.kill_and_wait();
        assert_route_journal_present(&direct_path, 1);
        std::fs::write(&direct_path, &direct_config).unwrap();

        let mut recovered = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let recovered_name = assert_tun_os_configured(binary, &socket, false, false);
        for _ in 0..8 {
            assert_tcp_through_tun(tcp_target);
        }

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&recovered_name);
        assert_route_journals_clean(&direct_path);
        recovered.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires administrator/root, a TUN backend, internet access, and ZERO_TUN_E2E_STUN_ADDR"]
fn privileged_tun_ipv4_config_reload_stun_block_and_crash_recovery() {
    run_family(false);
}

#[test]
#[ignore = "requires administrator/root, IPv6, a TUN backend, internet access, and ZERO_TUN_E2E_STUN_ADDR_V6"]
fn privileged_tun_ipv6_config_reload_stun_block_and_crash_recovery() {
    run_family(true);
}

#[test]
#[ignore = "requires administrator/root and a TUN backend"]
fn privileged_tun_dual_stack_configuration_traffic_and_crash_recovery() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let mock_socks = MockSocks5::start();
    let mock_dns = MockDns::start();
    let tcp_v4 = "1.1.1.1:80".parse().unwrap();
    let tcp_v6 = "[2606:4700:4700::1111]:80".parse().unwrap();
    let direct_config =
        dual_stack_config_json(listen_port, true, mock_socks.address, mock_dns.address);
    let stopped_config =
        dual_stack_config_json(listen_port, false, mock_socks.address, mock_dns.address);
    let direct_path = directory.path().join("dual.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, true);
        let initial_name = assert_tun_os_configured(binary, &socket, false, true);
        assert_tun_route_selected(&initial_name, tcp_v4);
        assert_tun_route_selected(&initial_name, tcp_v6);
        assert_dns_hijack_through_tun(false);
        assert_dns_hijack_through_tun(true);
        assert_tcp_through_tun(tcp_v4);
        assert_tcp_through_tun(tcp_v6);

        process.kill_and_wait();
        assert_route_journal_present(&direct_path, 2);
        std::fs::write(&direct_path, &direct_config).unwrap();

        let mut recovered = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, true);
        let recovered_name = assert_tun_os_configured(binary, &socket, false, true);
        assert_tcp_through_tun(tcp_v4);
        assert_tcp_through_tun(tcp_v6);

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, true);
        assert_tun_os_cleanup(&recovered_name);
        assert_route_journals_clean(&direct_path);
        recovered.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

fn run_family(ipv6: bool) {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary E2E directory");
    let socket = control_socket(directory.path(), ipv6);
    let listen_port = free_tcp_port();
    let tcp_target = resolve_tcp_target(ipv6);
    let stun_env = if ipv6 {
        "ZERO_TUN_E2E_STUN_ADDR_V6"
    } else {
        "ZERO_TUN_E2E_STUN_ADDR"
    };
    let stun: SocketAddr = std::env::var(stun_env)
        .unwrap_or_else(|_| panic!("{stun_env} must contain a reachable STUN server socket"))
        .parse()
        .expect("parse STUN server socket");
    assert_eq!(stun.is_ipv6(), ipv6, "STUN target family mismatch");

    let direct_config = config_json(ipv6, listen_port, None, true, false);
    let blocked_config = config_json(ipv6, listen_port, Some(stun.ip()), true, false);
    let stopped_config = config_json(ipv6, listen_port, None, false, false);
    let direct_path = directory.path().join("direct.json");
    let blocked_path = directory.path().join("blocked.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&blocked_path, blocked_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_scenario(
            binary,
            ipv6,
            false,
            stun,
            tcp_target,
            &socket,
            &direct_config,
            &direct_path,
            &blocked_path,
            &stopped_path,
        );
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

#[allow(clippy::too_many_arguments)]
fn run_scenario(
    binary: &str,
    ipv6: bool,
    dual_stack: bool,
    stun: SocketAddr,
    tcp_target: SocketAddr,
    socket: &std::path::Path,
    direct_config: &str,
    direct_path: &std::path::Path,
    blocked_path: &std::path::Path,
    stopped_path: &std::path::Path,
) {
    let mut process = spawn_zero(binary, direct_path, socket);
    wait_for_tun(binary, socket, true, dual_stack);
    let initial_name = assert_tun_os_configured(binary, socket, ipv6, dual_stack);
    assert_tcp_through_tun(tcp_target);
    assert_dns_hijack_through_tun(ipv6);
    assert_stun_round_trip(stun);

    run_cli(
        binary,
        ["reload", path(blocked_path), "--socket", path(socket)],
    );
    wait_for_tun(binary, socket, true, dual_stack);
    assert_eq!(
        assert_tun_os_configured(binary, socket, ipv6, dual_stack),
        initial_name
    );
    assert_stun_blocked(stun);

    // Simulate an ungraceful process crash. The next start must consume the
    // route journal and recover stale host exclusions before installing routes.
    process.kill_and_wait();
    assert_route_journal_present(direct_path, if dual_stack { 2 } else { 1 });
    std::fs::write(direct_path, direct_config).unwrap();

    let mut recovered = spawn_zero(binary, direct_path, socket);
    wait_for_tun(binary, socket, true, dual_stack);
    let recovered_name = assert_tun_os_configured(binary, socket, ipv6, dual_stack);
    assert_tcp_through_tun(tcp_target);

    run_cli(
        binary,
        ["reload", path(stopped_path), "--socket", path(socket)],
    );
    wait_for_tun(binary, socket, false, dual_stack);
    assert_tun_os_cleanup(&recovered_name);
    assert_route_journals_clean(direct_path);
    recovered.kill_and_wait();
}

fn best_effort_route_recovery(
    binary: &str,
    socket: &std::path::Path,
    direct_path: &std::path::Path,
    stopped_path: &std::path::Path,
) {
    eprintln!("TUN E2E failed; attempting journal-based route cleanup");
    let Ok(mut child) = Command::new(binary)
        .args(["run", "--control-socket", path(socket), path(direct_path)])
        .env("ZERO_TUN_STATE_DIR", route_state_dir(direct_path))
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
    else {
        return;
    };
    if try_wait_for_tun(binary, socket, true, None, tun_state_timeout()) {
        let _ = Command::new(binary)
            .args(["reload", path(stopped_path), "--socket", path(socket)])
            .output();
        let _ = try_wait_for_tun(binary, socket, false, None, tun_state_timeout());
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn config_json(
    ipv6: bool,
    listen_port: u16,
    blocked: Option<IpAddr>,
    tun: bool,
    dual_stack: bool,
) -> String {
    let tun = tun.then(|| {
        serde_json::json!({
            "name": if cfg!(target_os = "macos") { serde_json::Value::Null } else { serde_json::Value::String(if ipv6 { "ZeroTun6" } else { "ZeroTun4" }.to_owned()) },
            "addr": if ipv6 { "fd66::1/64" } else { "10.66.0.1/24" },
            "secondary_addr": dual_stack.then_some(if ipv6 { "10.66.0.1/24" } else { "fd66::1/64" }),
            "tag": "tun-e2e",
            "auto_route": true,
            "dual_stack": dual_stack,
            "strict_route": true,
            "dns_hijack": true
        })
    });
    let dns_address = if ipv6 {
        "2606:4700:4700::1111"
    } else {
        "1.1.1.1"
    };
    let rules = blocked
        .into_iter()
        .map(|address| {
            serde_json::json!({
                "condition": {
                    "type": "ip",
                    "values": [format!("{address}/{}", if address.is_ipv4() { 32 } else { 128 })]
                },
                "action": { "type": "reject" }
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&serde_json::json!({
        "runtime": {
            "network": { "mtu": 1400 },
            "tun": tun,
            "dns": {
                "servers": [{ "type": "udp", "address": dns_address, "port": 53 }]
            }
        },
        "inbounds": [{
            "tag": "control-inbound",
            "listen": { "address": "127.0.0.1", "port": listen_port },
            "protocol": { "type": "socks5" }
        }],
        "route": { "rules": rules, "final": { "type": "direct" } }
    }))
    .unwrap()
}

fn dual_stack_config_json(
    listen_port: u16,
    tun: bool,
    socks: SocketAddr,
    dns: SocketAddr,
) -> String {
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json(false, listen_port, None, tun, true)).unwrap();
    config["runtime"]["dns"]["servers"] = serde_json::json!([{
        "type": "udp",
        "address": dns.ip().to_string(),
        "port": dns.port()
    }]);
    config["outbounds"] = serde_json::json!([{
        "tag": "mock-socks",
        "protocol": {
            "type": "socks5",
            "server": socks.ip().to_string(),
            "port": socks.port()
        }
    }]);
    config["route"]["rules"] = serde_json::json!([
        {
            "condition": { "type": "ip", "values": ["1.1.1.1/32"] },
            "action": { "type": "route", "outbound": "mock-socks" }
        },
        {
            "condition": { "type": "ip", "values": ["2606:4700:4700::1111/128"] },
            "action": { "type": "route", "outbound": "mock-socks" }
        }
    ]);
    config["route"]["final"] = serde_json::json!({ "type": "reject" });
    serde_json::to_string_pretty(&config).unwrap()
}

fn spawn_zero(binary: &str, config: &std::path::Path, socket: &std::path::Path) -> ManagedChild {
    let child = Command::new(binary)
        .args(["run", "--control-socket", path(socket), path(config)])
        .env("ZERO_TUN_STATE_DIR", route_state_dir(config))
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn Zero");
    ManagedChild { child: Some(child) }
}

fn route_state_dir(config: &std::path::Path) -> std::path::PathBuf {
    config
        .parent()
        .expect("E2E config must have a parent")
        .join("tun-state")
}

fn route_journals(config: &std::path::Path) -> Vec<std::path::PathBuf> {
    let Ok(entries) = std::fs::read_dir(route_state_dir(config)) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect()
}

fn assert_route_journal_present(config: &std::path::Path, expected: usize) {
    let journals = route_journals(config);
    assert_eq!(
        journals.len(),
        expected,
        "hard kill must leave one recovery journal per managed address family: {journals:?}"
    );
}

fn assert_route_journals_clean(config: &std::path::Path) {
    let journals = route_journals(config);
    assert!(
        journals.is_empty(),
        "graceful TUN stop left recovery journals behind: {journals:?}"
    );
}

struct ManagedChild {
    child: Option<Child>,
}

impl ManagedChild {
    fn kill_and_wait(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().expect("kill Zero process");
            child.wait().expect("wait for Zero process");
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_tun(binary: &str, socket: &std::path::Path, running: bool, dual_stack: bool) {
    assert!(
        try_wait_for_tun(
            binary,
            socket,
            running,
            running.then_some(dual_stack),
            tun_state_timeout(),
        ),
        "timed out waiting for TUN state"
    );
}

fn tun_state_timeout() -> Duration {
    if cfg!(windows) {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(20)
    }
}

fn try_wait_for_tun(
    binary: &str,
    socket: &std::path::Path,
    running: bool,
    dual_stack: Option<bool>,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let output = Command::new(binary)
            .args(["tun", "status", "--socket", path(socket)])
            .output();
        if let Ok(output) = output {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if output.status.success()
                && stdout.contains(if running {
                    "tun: running"
                } else {
                    "tun: not running"
                })
            {
                if running
                    && (!stdout.contains("healthy=true")
                        || !stdout.contains("managed_by_config=true")
                        || dual_stack.is_some_and(|dual_stack| {
                            !stdout.contains(&format!("dual_stack={dual_stack}"))
                                || (dual_stack
                                    && (!stdout.contains("10.66.0.1/24")
                                        || !stdout.contains("fd66::1/64")))
                        }))
                {
                    return false;
                }
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn assert_tun_os_configured(
    binary: &str,
    socket: &std::path::Path,
    ipv6: bool,
    dual_stack: bool,
) -> String {
    let status = tun_status(binary, socket);
    let name = status_field(&status, "name").expect("TUN status must expose its device name");
    if !ipv6 || dual_stack {
        assert!(
            status.contains("10.66.0.1/24"),
            "TUN status is missing its IPv4 address: {status}"
        );
    }
    if ipv6 || dual_stack {
        assert!(
            status.contains("fd66::1/64"),
            "TUN status is missing its IPv6 address: {status}"
        );
    }
    let egress_v4 = status_field(&status, "egress_v4");
    let egress_v6 = status_field(&status, "egress_v6");
    assert_platform_tun_configured(
        &name,
        ipv6,
        dual_stack,
        egress_v4.as_deref(),
        egress_v6.as_deref(),
    );
    name
}

fn tun_status(binary: &str, socket: &std::path::Path) -> String {
    let output = Command::new(binary)
        .args(["tun", "status", "--socket", path(socket)])
        .output()
        .expect("query TUN status");
    assert!(
        output.status.success(),
        "query TUN status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("TUN status must be UTF-8")
}

fn status_field(status: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    status
        .trim()
        .split(", ")
        .find_map(|part| part.strip_prefix(&prefix).map(str::to_owned))
}

#[cfg(target_os = "linux")]
fn assert_platform_tun_configured(
    name: &str,
    ipv6: bool,
    dual_stack: bool,
    _egress_v4: Option<&str>,
    _egress_v6: Option<&str>,
) {
    let link = checked_command("ip", &["-o", "link", "show", "dev", name]);
    assert!(link.contains("mtu 1400"), "unexpected TUN link: {link}");

    let addresses = checked_command("ip", &["-o", "address", "show", "dev", name]);
    if !ipv6 || dual_stack {
        assert!(
            addresses.contains("10.66.0.1/24"),
            "IPv4 TUN address is missing: {addresses}"
        );
        for prefix in ["0.0.0.0/1", "128.0.0.0/1"] {
            let route = checked_command("ip", &["-4", "route", "show", prefix, "dev", name]);
            assert!(route.contains(prefix), "TUN route is missing: {prefix}");
        }
    }
    if ipv6 || dual_stack {
        assert!(
            addresses.contains("fd66::1/64"),
            "IPv6 TUN address is missing: {addresses}"
        );
        for prefix in ["::/1", "8000::/1"] {
            let route = checked_command("ip", &["-6", "route", "show", prefix, "dev", name]);
            assert!(route.contains(prefix), "TUN route is missing: {prefix}");
        }
    }
}

#[cfg(target_os = "macos")]
fn assert_platform_tun_configured(
    name: &str,
    ipv6: bool,
    dual_stack: bool,
    egress_v4: Option<&str>,
    _egress_v6: Option<&str>,
) {
    let interface = checked_command("/sbin/ifconfig", &[name]);
    assert!(
        interface.contains("mtu 1400"),
        "unexpected TUN interface: {interface}"
    );
    if !ipv6 || dual_stack {
        assert!(
            interface.contains("inet 10.66.0.1"),
            "IPv4 TUN address is missing: {interface}"
        );
        for probe in ["64.0.0.1", "192.0.2.1"] {
            let route = checked_command("/sbin/route", &["-n", "get", "-inet", probe]);
            assert!(
                route.contains(&format!("interface: {name}")),
                "IPv4 split route does not use {name}: {route}"
            );
        }
        let egress = egress_v4
            .filter(|egress| *egress != "-")
            .expect("macOS IPv4 TUN status must expose its physical egress");
        let bypass = checked_command(
            "/sbin/route",
            &["-n", "get", "-inet", "-ifscope", egress, "default"],
        );
        assert!(
            bypass.contains(&format!("interface: {egress}")) && bypass.contains("IFSCOPE"),
            "macOS scoped physical bypass route is missing for {egress}: {bypass}"
        );
    }
    if ipv6 || dual_stack {
        assert!(
            interface.contains("inet6 fd66::1"),
            "IPv6 TUN address is missing: {interface}"
        );
        for probe in ["2001:db8::1", "9000::1"] {
            let route = checked_command("/sbin/route", &["-n", "get", "-inet6", probe]);
            assert!(
                route.contains(&format!("interface: {name}")),
                "IPv6 split route does not use {name}: {route}"
            );
        }
    }
}

#[cfg(windows)]
fn assert_platform_tun_configured(
    name: &str,
    ipv6: bool,
    dual_stack: bool,
    _egress_v4: Option<&str>,
    _egress_v6: Option<&str>,
) {
    let script = r#"
$ErrorActionPreference = 'Stop'
$name = $env:ZERO_TUN_E2E_NAME
$dual = $env:ZERO_TUN_E2E_DUAL -eq 'true'
$primary6 = $env:ZERO_TUN_E2E_PRIMARY_V6 -eq 'true'
$check4 = (-not $primary6) -or $dual
$check6 = $primary6 -or $dual
$adapter = Get-NetAdapter -Name $name
if ($adapter.Status -eq 'Disabled') { throw "TUN adapter is disabled" }
if ($check4) {
  $ipv4 = @(Get-NetIPAddress -InterfaceAlias $name -AddressFamily IPv4)
  if (@($ipv4 | Where-Object { $_.IPAddress -eq '10.66.0.1' -and $_.PrefixLength -eq 24 }).Count -ne 1) { throw "IPv4 TUN address is missing" }
  $mtu4 = Get-NetIPInterface -InterfaceAlias $name -AddressFamily IPv4
  if ($mtu4.NlMtu -ne 1400) { throw "IPv4 TUN MTU is not 1400" }
  $routes4 = @(Get-NetRoute -InterfaceAlias $name -AddressFamily IPv4)
  foreach ($prefix in @('0.0.0.0/1', '128.0.0.0/1')) {
    if (@($routes4 | Where-Object { $_.DestinationPrefix -eq $prefix }).Count -ne 1) { throw "missing route $prefix" }
  }
}
if ($check6) {
  $ipv6 = @(Get-NetIPAddress -InterfaceAlias $name -AddressFamily IPv6)
  if (@($ipv6 | Where-Object { $_.IPAddress -eq 'fd66::1' -and $_.PrefixLength -eq 64 }).Count -ne 1) { throw "IPv6 TUN address is missing" }
  $mtu6 = Get-NetIPInterface -InterfaceAlias $name -AddressFamily IPv6
  if ($mtu6.NlMtu -ne 1400) { throw "IPv6 TUN MTU is not 1400" }
  $routes6 = @(Get-NetRoute -InterfaceAlias $name -AddressFamily IPv6)
  foreach ($prefix in @('::/1', '8000::/1')) {
    if (@($routes6 | Where-Object { $_.DestinationPrefix -eq $prefix }).Count -ne 1) { throw "missing route $prefix" }
  }
}
"#;
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("ZERO_TUN_E2E_NAME", name)
        .env("ZERO_TUN_E2E_DUAL", dual_stack.to_string())
        .env("ZERO_TUN_E2E_PRIMARY_V6", ipv6.to_string())
        .output()
        .expect("inspect Windows TUN interface");
    assert!(
        output.status.success(),
        "Windows TUN OS-state assertion failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_tun_os_cleanup(name: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while tun_device_exists(name) {
        assert!(
            Instant::now() < deadline,
            "TUN device `{name}` remained after graceful stop"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(windows)]
fn assert_tun_route_selected(name: &str, target: SocketAddr) {
    let script = r#"
$routes = @(Find-NetRoute -RemoteIPAddress $env:ZERO_TUN_E2E_TARGET -ErrorAction Stop |
  Where-Object { $_.CimClass.CimClassName -eq 'MSFT_NetRoute' })
if ($routes.Count -ne 1) { throw "expected one selected route, got $($routes.Count)" }
if ($routes[0].InterfaceAlias -ne $env:ZERO_TUN_E2E_NAME) {
  throw "selected interface '$($routes[0].InterfaceAlias)' instead of '$env:ZERO_TUN_E2E_NAME'; prefix=$($routes[0].DestinationPrefix); next-hop=$($routes[0].NextHop); state=$($routes[0].State)"
}
"#;
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("ZERO_TUN_E2E_NAME", name)
        .env("ZERO_TUN_E2E_TARGET", target.ip().to_string())
        .output()
        .expect("inspect selected Windows route");
    assert!(
        output.status.success(),
        "Windows selected-route assertion failed for {target}:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(not(windows))]
fn assert_tun_route_selected(_name: &str, _target: SocketAddr) {}

#[cfg(target_os = "linux")]
fn tun_device_exists(name: &str) -> bool {
    Command::new("ip")
        .args(["link", "show", "dev", name])
        .output()
        .is_ok_and(|output| output.status.success())
}

#[cfg(target_os = "macos")]
fn tun_device_exists(name: &str) -> bool {
    Command::new("/sbin/ifconfig")
        .arg(name)
        .output()
        .is_ok_and(|output| output.status.success())
}

#[cfg(windows)]
fn tun_device_exists(name: &str) -> bool {
    let script = "if (Get-NetAdapter -Name $env:ZERO_TUN_E2E_NAME -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }";
    Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("ZERO_TUN_E2E_NAME", name)
        .output()
        .is_ok_and(|output| output.status.success())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn checked_command(program: &str, arguments: &[&str]) -> String {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("run `{program}`: {error}"));
    assert!(
        output.status.success(),
        "`{program} {}` failed:\n{}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("platform network command output must be UTF-8")
}

fn resolve_tcp_target(ipv6: bool) -> SocketAddr {
    let override_name = if ipv6 {
        "ZERO_TUN_E2E_TCP_ADDR_V6"
    } else {
        "ZERO_TUN_E2E_TCP_ADDR"
    };
    std::env::var(override_name)
        .ok()
        .map(|target| target.parse().expect("parse TCP E2E target"))
        .unwrap_or_else(|| {
            ("example.com", 80)
                .to_socket_addrs()
                .expect("resolve TCP E2E target through TUN DNS")
                .find(|target| target.is_ipv6() == ipv6)
                .expect("example.com has an address for the requested family")
        })
}

fn assert_tcp_through_tun(target: SocketAddr) {
    let socket = Socket::new(
        Domain::for_address(target),
        Type::STREAM,
        Some(Protocol::TCP),
    )
    .expect("create TUN E2E TCP socket");
    socket
        .bind(&SocketAddr::new(tun_source(target.is_ipv6()), 0).into())
        .expect("bind TCP client to TUN address");
    socket
        .connect_timeout(&target.into(), Duration::from_secs(10))
        .unwrap_or_else(|error| {
            #[cfg(windows)]
            dump_windows_tun_network_state();
            panic!("TCP request through TUN: {error}");
        });
    let mut stream: TcpStream = socket.into();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = [0_u8; 32];
    let size = stream
        .read(&mut response)
        .unwrap_or_else(|error| panic!("read HTTP response from {target}: {error}"));
    assert!(size > 0, "TCP target returned no bytes through TUN");
}

#[cfg(windows)]
fn dump_windows_tun_network_state() {
    let script = r#"
Get-NetAdapter -Name 'ZeroTun4' -ErrorAction SilentlyContinue | Format-List Name,InterfaceIndex,Status,MediaConnectionState
Get-NetIPAddress -InterfaceAlias 'ZeroTun4' -AddressFamily IPv4 -ErrorAction SilentlyContinue | Format-List IPAddress,PrefixLength,AddressState,PrefixOrigin,SuffixOrigin
Get-NetIPInterface -InterfaceAlias 'ZeroTun4' -AddressFamily IPv4 -ErrorAction SilentlyContinue | Format-List InterfaceIndex,ConnectionState,InterfaceMetric,NlMtu,Forwarding,WeakHostSend,WeakHostReceive
Get-NetRoute -InterfaceAlias 'ZeroTun4' -AddressFamily IPv4 -ErrorAction SilentlyContinue | Sort-Object DestinationPrefix | Format-List DestinationPrefix,NextHop,RouteMetric,InterfaceMetric,Protocol,State,ValidLifetime,PreferredLifetime,Publish,Store
"#;
    if let Ok(output) = Command::new("powershell")
        .args(["-NoProfile", "-Command", script])
        .output()
    {
        eprintln!(
            "Windows TUN network state:\n{}",
            String::from_utf8_lossy(&output.stdout)
        );
        eprintln!(
            "Windows TUN network diagnostics:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn assert_dns_hijack_through_tun(ipv6: bool) {
    let target: SocketAddr = if ipv6 {
        "[2001:4860:4860::8888]:53"
    } else {
        "8.8.8.8:53"
    }
    .parse()
    .unwrap();
    let socket = UdpSocket::bind(SocketAddr::new(tun_source(ipv6), 0)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let query = dns_query(0x6611, "example.com");
    socket
        .send_to(&query, target)
        .expect("send hijacked DNS query");
    let mut response = [0_u8; 2048];
    let (size, _) = socket
        .recv_from(&mut response)
        .expect("receive hijacked DNS reply");
    assert!(size >= 12);
    assert_eq!(&response[..2], &0x6611_u16.to_be_bytes());
    assert_ne!(response[2] & 0x80, 0, "DNS response bit must be set");
}

fn assert_stun_round_trip(target: SocketAddr) {
    let socket = udp_for(target);
    socket.send_to(&stun_request(), target).unwrap();
    let mut response = [0_u8; 2048];
    let (size, _) = socket
        .recv_from(&mut response)
        .expect("baseline STUN response");
    assert!(size >= 20);
    assert_eq!(&response[4..8], &[0x21, 0x12, 0xa4, 0x42]);
    assert_eq!(&response[8..20], &stun_request()[8..20]);
}

fn assert_stun_blocked(target: SocketAddr) {
    let socket = udp_for(target);
    socket.send_to(&stun_request(), target).unwrap();
    let mut response = [0_u8; 2048];
    let error = socket
        .recv_from(&mut response)
        .expect_err("blocked STUN must not receive a network response");
    assert!(matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ));
}

fn udp_for(target: SocketAddr) -> UdpSocket {
    let socket = UdpSocket::bind(SocketAddr::new(tun_source(target.is_ipv6()), 0)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket
}

fn tun_source(ipv6: bool) -> IpAddr {
    if ipv6 {
        "fd66::1".parse().unwrap()
    } else {
        "10.66.0.1".parse().unwrap()
    }
}

fn stun_request() -> [u8; 20] {
    [
        0x00, 0x01, 0x00, 0x00, 0x21, 0x12, 0xa4, 0x42, 0x66, 0x00, 0x00, 0x01, 0x66, 0x00, 0x00,
        0x02, 0x66, 0x00, 0x00, 0x03,
    ]
}

fn dns_query(id: u16, name: &str) -> Vec<u8> {
    let mut query = Vec::from(id.to_be_bytes());
    query.extend_from_slice(&[0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);
    for label in name.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.extend_from_slice(&[0, 0, 1, 0, 1]);
    query
}

fn run_cli<const N: usize>(binary: &str, arguments: [&str; N]) {
    let output = Command::new(binary).args(arguments).output().unwrap();
    assert!(
        output.status.success(),
        "Zero CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn free_tcp_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn control_socket(_directory: &std::path::Path, ipv6: bool) -> std::path::PathBuf {
    #[cfg(windows)]
    return std::path::PathBuf::from(format!(
        r"\\.\pipe\zero-tun-e2e-{}-{}",
        std::process::id(),
        if ipv6 { 6 } else { 4 }
    ));
    #[cfg(unix)]
    _directory.join(if ipv6 {
        "control-v6.sock"
    } else {
        "control-v4.sock"
    })
}

fn path(path: &std::path::Path) -> &str {
    path.to_str().expect("E2E path must be UTF-8")
}

struct MockSocks5 {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl MockSocks5 {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock SOCKS5");
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if worker_stop.load(Ordering::Relaxed) {
                            break;
                        }
                        stream
                            .set_nonblocking(false)
                            .expect("make mock SOCKS5 client blocking");
                        if let Err(error) = serve_mock_socks5_connection(&mut stream) {
                            eprintln!("mock SOCKS5 connection failed: {error}");
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept mock SOCKS5 connection: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for MockSocks5 {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("join mock SOCKS5 worker");
        }
    }
}

fn serve_mock_socks5_connection(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut greeting = [0_u8; 2];
    stream.read_exact(&mut greeting)?;
    if greeting[0] != 5 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid SOCKS5 greeting",
        ));
    }
    let mut methods = vec![0_u8; greeting[1] as usize];
    stream.read_exact(&mut methods)?;
    stream.write_all(&[5, 0])?;

    let mut request = [0_u8; 4];
    stream.read_exact(&mut request)?;
    if request[..3] != [5, 1, 0] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "mock SOCKS5 expected CONNECT",
        ));
    }
    let address_size = match request[3] {
        1 => 4,
        4 => 16,
        3 => {
            let mut length = [0_u8; 1];
            stream.read_exact(&mut length)?;
            length[0] as usize
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid SOCKS5 address type",
            ));
        }
    };
    let mut destination = vec![0_u8; address_size + 2];
    stream.read_exact(&mut destination)?;
    stream.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])?;

    let mut request_body = [0_u8; 1024];
    let size = stream.read(&mut request_body)?;
    if size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "mock SOCKS5 received no tunneled request",
        ));
    }
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
}

struct MockDns {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl MockDns {
    fn start() -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind mock DNS");
        let address = socket.local_addr().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::spawn(move || {
            let mut packet = [0_u8; 2048];
            while !worker_stop.load(Ordering::Relaxed) {
                match socket.recv_from(&mut packet) {
                    Ok((size, peer)) => {
                        if let Some(response) = mock_dns_response(&packet[..size]) {
                            socket
                                .send_to(&response, peer)
                                .expect("send mock DNS response");
                        }
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) => {}
                    Err(error) => panic!("receive mock DNS query: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for MockDns {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("join mock DNS worker");
        }
    }
}

fn mock_dns_response(query: &[u8]) -> Option<Vec<u8>> {
    if query.len() < 17 {
        return None;
    }
    let qtype = u16::from_be_bytes([query[query.len() - 4], query[query.len() - 3]]);
    let (record, data): (u16, &[u8]) = match qtype {
        28 => (
            28,
            &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        ),
        _ => (1, &[127, 0, 0, 1]),
    };
    let mut response = Vec::with_capacity(query.len() + 32);
    response.extend_from_slice(&query[..2]);
    response.extend_from_slice(&[0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0]);
    response.extend_from_slice(&query[12..]);
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&record.to_be_bytes());
    response.extend_from_slice(&[0, 1, 0, 0, 0, 60]);
    response.extend_from_slice(&(data.len() as u16).to_be_bytes());
    response.extend_from_slice(data);
    Some(response)
}
