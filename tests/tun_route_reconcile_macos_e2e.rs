use std::net::{Ipv4Addr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const SECONDARY_GATEWAY_ENV: &str = "ZERO_TUN_E2E_MACOS_SECONDARY_GATEWAY";
const SECONDARY_INTERFACE_ENV: &str = "ZERO_TUN_E2E_MACOS_SECONDARY_INTERFACE";

#[test]
#[ignore = "requires root and an isolated macOS runner with a connected secondary gateway"]
fn macos_reconciles_runtime_egress_and_dns_exclusion_without_restarting_tun() {
    assert_eq!(
        std::env::consts::OS,
        "macos",
        "this privileged E2E is only valid on macOS"
    );
    assert_root();
    let secondary_gateway = required_ipv4_env(SECONDARY_GATEWAY_ENV);
    let secondary_interface = required_env(SECONDARY_INTERFACE_ENV);
    assert_route_uses(secondary_gateway, &secondary_interface);

    let original = default_route();
    assert_ne!(
        original.interface, secondary_interface,
        "secondary interface is already the default egress"
    );
    assert_ne!(
        original.gateway, secondary_gateway,
        "secondary gateway is already the default gateway"
    );

    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary E2E directory");
    let socket = directory.path().join("control.sock");
    let running_path = directory.path().join("running.json");
    let stopped_path = directory.path().join("stopped.json");
    let port = free_tcp_port();
    std::fs::write(&running_path, config_json(true, port)).unwrap();
    std::fs::write(&stopped_path, config_json(false, port)).unwrap();

    let mut zero = ManagedZero::start(binary, &running_path, &stopped_path, &socket);
    assert_eq!(
        wait_for_healthy_egress(binary, &socket, Some(&original.interface)),
        original.interface
    );
    assert_dns_exclusion_uses(&original.interface);

    let mut route = DefaultRouteTransaction::new(original.clone());
    route.switch(secondary_gateway, &secondary_interface);
    assert_eq!(
        wait_for_healthy_egress(binary, &socket, Some(&secondary_interface)),
        secondary_interface
    );
    assert_dns_exclusion_uses(&secondary_interface);
    assert!(
        zero.is_running(),
        "Zero exited during macOS route reconciliation"
    );

    route.restore();
    assert_eq!(
        wait_for_healthy_egress(binary, &socket, Some(&original.interface)),
        original.interface
    );
    assert_dns_exclusion_uses(&original.interface);
    assert!(
        zero.is_running(),
        "Zero exited while restoring the macOS egress"
    );

    zero.stop();
}

#[derive(Clone)]
struct DefaultRoute {
    interface: String,
    gateway: Ipv4Addr,
}

struct DefaultRouteTransaction {
    original: DefaultRoute,
    changed: bool,
}

impl DefaultRouteTransaction {
    fn new(original: DefaultRoute) -> Self {
        Self {
            original,
            changed: false,
        }
    }

    fn switch(&mut self, gateway: Ipv4Addr, expected_interface: &str) {
        // Mark first so an assertion or command failure still restores the
        // captured route during unwinding.
        self.changed = true;
        checked_route_change(gateway);
        wait_for_default_interface(expected_interface);
    }

    fn restore(&mut self) {
        if !self.changed {
            return;
        }
        checked_route_change(self.original.gateway);
        wait_for_default_interface(&self.original.interface);
        self.changed = false;
    }
}

impl Drop for DefaultRouteTransaction {
    fn drop(&mut self) {
        if self.changed {
            let _ = route_change(self.original.gateway);
        }
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
            .expect("spawn Zero on macOS");
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
        let output = zero_command(
            self.binary,
            &[
                "reload",
                path(self.stopped_config),
                "--socket",
                path(self.socket),
            ],
        );
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
            let _ = zero_command(
                self.binary,
                &[
                    "reload",
                    path(self.stopped_config),
                    "--socket",
                    path(self.socket),
                ],
            );
            self.kill();
        }
    }
}

fn config_json(running: bool, port: u16) -> String {
    let tun = running.then(|| {
        serde_json::json!({
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
            "timed out waiting for macOS TUN egress {expected:?}"
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
        assert!(
            Instant::now() < deadline,
            "timed out waiting for macOS TUN stop"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn tun_status(binary: &str, socket: &Path) -> Option<String> {
    let output = zero_command(binary, &["tun", "status", "--socket", path(socket)]);
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
}

fn status_field(status: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    status
        .trim()
        .split(", ")
        .find_map(|part| part.strip_prefix(&prefix).map(str::to_owned))
}

fn default_route() -> DefaultRoute {
    let output = route_get("default");
    assert!(
        output.status.success(),
        "query macOS default route failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8(output.stdout).expect("route output must be UTF-8");
    DefaultRoute {
        interface: route_field(&output, "interface").expect("default route has an interface"),
        gateway: route_field(&output, "gateway")
            .expect("default route has a gateway")
            .parse()
            .expect("default gateway must be IPv4"),
    }
}

fn assert_route_uses(address: Ipv4Addr, expected_interface: &str) {
    let output = route_get(&address.to_string());
    assert!(
        output.status.success(),
        "query secondary gateway route failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8(output.stdout).expect("route output must be UTF-8");
    assert_eq!(
        route_field(&output, "interface").as_deref(),
        Some(expected_interface),
        "secondary gateway is not connected through the configured interface"
    );
}

fn assert_dns_exclusion_uses(expected_interface: &str) {
    let output = route_get("1.1.1.1");
    assert!(output.status.success(), "query DNS exclusion route failed");
    let output = String::from_utf8(output.stdout).expect("route output must be UTF-8");
    assert_eq!(
        route_field(&output, "interface").as_deref(),
        Some(expected_interface),
        "DNS exclusion did not use the expected macOS interface"
    );
}

fn wait_for_default_interface(expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if default_route().interface == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "macOS default route did not switch to {expected}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn route_field(output: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}:");
    output
        .lines()
        .find_map(|line| line.trim().strip_prefix(&prefix).map(str::trim))
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn route_get(destination: &str) -> Output {
    Command::new("/sbin/route")
        .args(["-n", "get", "-inet", destination])
        .output()
        .expect("execute `/sbin/route get`")
}

fn route_change(gateway: Ipv4Addr) -> Output {
    Command::new("/sbin/route")
        .args(["-n", "change", "-inet", "default", &gateway.to_string()])
        .output()
        .expect("execute `/sbin/route change`")
}

fn checked_route_change(gateway: Ipv4Addr) {
    let output = route_change(gateway);
    assert!(
        output.status.success(),
        "change macOS default route to {gateway} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn zero_command(binary: &str, arguments: &[&str]) -> Output {
    Command::new(binary)
        .args(arguments)
        .output()
        .expect("execute Zero control command")
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set on the isolated macOS runner"))
}

fn required_ipv4_env(name: &str) -> Ipv4Addr {
    required_env(name)
        .parse()
        .unwrap_or_else(|error| panic!("{name} must contain an IPv4 address: {error}"))
}

fn assert_root() {
    let output = Command::new("id").arg("-u").output().expect("run id -u");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "0");
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
