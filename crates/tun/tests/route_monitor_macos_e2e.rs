#![cfg(target_os = "macos")]

use std::process::{Command, Output};
use std::time::Duration;

use zero_tun::RouteChangeMonitor;

#[tokio::test]
#[ignore = "requires root and an isolated macOS runner"]
async fn macos_route_monitor_observes_repeated_route_changes_and_releases_cleanly() {
    assert_eq!(unsafe { libc::geteuid() }, 0, "test must run as root");
    let address = unique_test_address();
    let mut monitor = RouteChangeMonitor::new().expect("register PF_ROUTE monitor");

    let mut route = TemporaryLoopbackRoute::install(&address);
    wait_for_change(&mut monitor, "route addition").await;
    monitor.coalesce().expect("coalesce route additions");

    route.close();
    wait_for_change(&mut monitor, "route deletion").await;
    monitor.coalesce().expect("coalesce route deletions");
}

async fn wait_for_change(monitor: &mut RouteChangeMonitor, operation: &str) {
    tokio::time::timeout(Duration::from_secs(5), monitor.changed())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for PF_ROUTE {operation} notification"))
        .unwrap_or_else(|error| panic!("PF_ROUTE {operation} notification failed: {error}"));
}

struct TemporaryLoopbackRoute {
    address: String,
    active: bool,
}

impl TemporaryLoopbackRoute {
    fn install(address: &str) -> Self {
        let output = route_command("add", address);
        assert_success(&output, "add", address);
        Self {
            address: address.to_owned(),
            active: true,
        }
    }

    fn close(&mut self) {
        if !self.active {
            return;
        }
        let output = route_command("delete", &self.address);
        assert_success(&output, "delete", &self.address);
        self.active = false;
    }
}

impl Drop for TemporaryLoopbackRoute {
    fn drop(&mut self) {
        if self.active {
            let _ = route_command("delete", &self.address);
        }
    }
}

fn route_command(operation: &str, address: &str) -> Output {
    Command::new("/sbin/route")
        .args([
            "-n",
            operation,
            "-inet",
            "-host",
            address,
            "-interface",
            "lo0",
        ])
        .output()
        .unwrap_or_else(|error| panic!("execute `/sbin/route`: {error}"))
}

fn assert_success(output: &Output, operation: &str, address: &str) {
    assert!(
        output.status.success(),
        "route {operation} {address} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn unique_test_address() -> String {
    let pid = std::process::id();
    format!("198.19.{}.{}", (pid >> 8) as u8, pid as u8)
}
