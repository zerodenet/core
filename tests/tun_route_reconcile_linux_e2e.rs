#![cfg(target_os = "linux")]

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const DNS_EXCLUSION: &str = "1.1.1.1/32";

#[test]
#[ignore = "requires root, iproute2, and /dev/net/tun"]
fn linux_reconciles_runtime_egress_and_dns_exclusion_inside_network_namespace() {
    assert_root();
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary E2E directory");
    let namespace = NetworkNamespace::create();
    namespace.configure();

    let socket = directory.path().join("control.sock");
    let running_path = directory.path().join("running.json");
    let stopped_path = directory.path().join("stopped.json");
    let port = free_tcp_port();
    std::fs::write(&running_path, config_json(true, true, port)).unwrap();
    std::fs::write(&stopped_path, config_json(false, true, port)).unwrap();

    let mut zero = ManagedZero::start(
        namespace.name(),
        binary,
        &running_path,
        &stopped_path,
        &socket,
    );
    assert_eq!(
        wait_for_healthy_egress(namespace.name(), binary, &socket, None),
        "physical0"
    );
    assert_exclusion_uses(namespace.name(), "physical0");

    namespace.add_secondary_default();
    assert_eq!(
        wait_for_healthy_egress(namespace.name(), binary, &socket, Some("physical1")),
        "physical1"
    );
    assert_exclusion_uses(namespace.name(), "physical1");
    assert!(
        zero.is_running(),
        "Zero exited during Linux route reconciliation"
    );

    namespace.remove_secondary_default();
    assert_eq!(
        wait_for_healthy_egress(namespace.name(), binary, &socket, Some("physical0")),
        "physical0"
    );
    assert_exclusion_uses(namespace.name(), "physical0");
    assert!(
        zero.is_running(),
        "Zero exited while restoring the Linux egress"
    );

    zero.stop();

    let unmanaged_path = directory.path().join("unmanaged.json");
    std::fs::write(&unmanaged_path, config_json(true, false, port)).unwrap();
    let mut unmanaged = ManagedZero::start(
        namespace.name(),
        binary,
        &unmanaged_path,
        &stopped_path,
        &socket,
    );
    wait_for_unmanaged_tun(namespace.name(), binary, &socket);
    assert_no_zero_managed_routes(namespace.name());

    namespace.add_secondary_default();
    std::thread::sleep(Duration::from_secs(1));
    wait_for_unmanaged_tun(namespace.name(), binary, &socket);
    assert_no_zero_managed_routes(namespace.name());
    assert!(
        unmanaged.is_running(),
        "Zero exited after an externally managed route change"
    );

    namespace.remove_secondary_default();
    unmanaged.stop();
}

struct NetworkNamespace {
    name: String,
}

impl NetworkNamespace {
    fn create() -> Self {
        let name = format!("zero-tun-route-e2e-{}", std::process::id());
        checked_ip(&["netns", "add", &name]);
        Self { name }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn configure(&self) {
        checked_ip(&["-n", &self.name, "link", "set", "lo", "up"]);
        for (name, address) in [
            ("physical0", "192.0.2.2/24"),
            ("physical1", "198.51.100.2/24"),
        ] {
            checked_ip(&["-n", &self.name, "link", "add", name, "type", "dummy"]);
            checked_ip(&["-n", &self.name, "address", "add", address, "dev", name]);
            checked_ip(&["-n", &self.name, "link", "set", name, "up"]);
        }
        checked_ip(&[
            "-n",
            &self.name,
            "route",
            "add",
            "default",
            "dev",
            "physical0",
            "metric",
            "100",
        ]);
    }

    fn add_secondary_default(&self) {
        checked_ip(&[
            "-n",
            &self.name,
            "route",
            "add",
            "default",
            "dev",
            "physical1",
            "metric",
            "50",
        ]);
    }

    fn remove_secondary_default(&self) {
        checked_ip(&[
            "-n",
            &self.name,
            "route",
            "del",
            "default",
            "dev",
            "physical1",
            "metric",
            "50",
        ]);
    }
}

impl Drop for NetworkNamespace {
    fn drop(&mut self) {
        let _ = Command::new("ip")
            .args(["netns", "delete", &self.name])
            .output();
    }
}

struct ManagedZero<'a> {
    child: Option<Child>,
    namespace: &'a str,
    binary: &'a str,
    stopped_config: &'a Path,
    socket: &'a Path,
}

impl<'a> ManagedZero<'a> {
    fn start(
        namespace: &'a str,
        binary: &'a str,
        config: &Path,
        stopped_config: &'a Path,
        socket: &'a Path,
    ) -> Self {
        let child = Command::new("ip")
            .args([
                "netns",
                "exec",
                namespace,
                binary,
                "run",
                "--control-socket",
                path(socket),
                path(config),
            ])
            .env(
                "ZERO_TUN_STATE_DIR",
                config.parent().unwrap().join("tun-route-state"),
            )
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn Zero inside network namespace");
        Self {
            child: Some(child),
            namespace,
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
        let output = netns_command(
            self.namespace,
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
        wait_for_tun_stopped(self.namespace, self.binary, self.socket);
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
            let _ = netns_command(
                self.namespace,
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

fn config_json(running: bool, auto_route: bool, port: u16) -> String {
    let tun = running.then(|| {
        serde_json::json!({
            "name": "zero-route-e2e",
            "addr": "10.68.0.1/24",
            "tag": "tun-route-reconcile-e2e",
            "auto_route": auto_route,
            "dual_stack": false,
            "strict_route": true,
            "dns_hijack": true
        })
    });
    serde_json::to_string_pretty(&serde_json::json!({
        "runtime": {
            "tun": tun,
            "dns": { "servers": [{ "type": "udp", "address": "1.1.1.1", "port": 53 }] }
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

fn wait_for_healthy_egress(
    namespace: &str,
    binary: &str,
    socket: &Path,
    expected: Option<&str>,
) -> String {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = tun_status(namespace, binary, socket) {
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
            "timed out waiting for Linux TUN egress {expected:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_tun_stopped(namespace: &str, binary: &str, socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if tun_status(namespace, binary, socket)
            .is_some_and(|status| status.contains("tun: not running"))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for Linux TUN stop"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_for_unmanaged_tun(namespace: &str, binary: &str, socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if tun_status(namespace, binary, socket).is_some_and(|status| {
            status.contains("tun: running")
                && status.contains("healthy=true")
                && status.contains("auto_route=false")
                && status.contains("egress_v4=-")
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for externally managed Linux TUN"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn tun_status(namespace: &str, binary: &str, socket: &Path) -> Option<String> {
    let output = netns_command(
        namespace,
        binary,
        &["tun", "status", "--socket", path(socket)],
    );
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

fn assert_exclusion_uses(namespace: &str, interface: &str) {
    let output = Command::new("ip")
        .args(["-n", namespace, "-4", "route", "show", DNS_EXCLUSION])
        .output()
        .expect("inspect Linux DNS exclusion route");
    assert!(output.status.success());
    let route = String::from_utf8(output.stdout).expect("route output must be UTF-8");
    assert!(
        route.contains(&format!("dev {interface}")),
        "DNS exclusion route did not use {interface}: {route}"
    );
}

fn assert_no_zero_managed_routes(namespace: &str) {
    for prefix in ["0.0.0.0/1", "128.0.0.0/1", DNS_EXCLUSION] {
        let output = Command::new("ip")
            .args(["-n", namespace, "-4", "route", "show", prefix])
            .output()
            .expect("inspect externally managed Linux routes");
        assert!(output.status.success());
        assert!(
            output.stdout.is_empty(),
            "auto_route=false unexpectedly installed {prefix}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

fn netns_command(namespace: &str, program: &str, arguments: &[&str]) -> std::process::Output {
    Command::new("ip")
        .args(["netns", "exec", namespace, program])
        .args(arguments)
        .output()
        .expect("run command inside network namespace")
}

fn checked_ip(arguments: &[&str]) {
    let output = Command::new("ip")
        .args(arguments)
        .output()
        .expect("run iproute2 command");
    assert!(
        output.status.success(),
        "ip {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
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
