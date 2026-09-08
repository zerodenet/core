use super::*;

#[test]
#[ignore = "requires root and an isolated macOS runner; removes active TUN routes"]
fn same_interface_route_loss_recovers_automatically_and_on_manual_request() {
    assert_root();
    let original = default_route();
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("control.sock");
    let running = directory.path().join("running.json");
    let stopped = directory.path().join("stopped.json");
    let port = free_tcp_port();
    std::fs::write(&running, config_json(true, port, original.gateway)).unwrap();
    std::fs::write(&stopped, config_json(false, port, original.gateway)).unwrap();
    let mut zero = ManagedZero::start(binary, &running, &stopped, &socket);
    wait_for_healthy_egress(binary, &socket, Some(&original.interface));
    let status = tun_status(binary, &socket).unwrap();
    let name = status_field(&status, "name").expect("TUN device name");
    let pid = zero.child.as_ref().unwrap().id();

    // A pre-existing OS scoped default is borrowed, never delete it in a test.
    let owns_scoped = std::fs::read_dir(directory.path().join("tun-route-state"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with("-v4.json"))
        .any(|entry| {
            let journal: serde_json::Value =
                serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
            journal["scoped_bypass"] == true
        });
    for manual in [false, true] {
        delete(&["-n", "delete", "-inet", "128.0.0.0/1"]);
        delete(&["-n", "delete", "-inet", "-host", "1.1.1.1"]);
        if owns_scoped {
            delete(&[
                "-n",
                "delete",
                "-inet",
                "-ifscope",
                &original.interface,
                "default",
            ]);
        }
        if manual {
            let result = zero_command(binary, &["tun", "recover", "--socket", path(&socket)]);
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        // Inspect actual OS routes, not just an unchanged healthy status.
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            if intact(&name, &original.interface) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "missing routes were not repaired (manual={manual})"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        wait_for_healthy_egress(binary, &socket, Some(&original.interface));
        assert_eq!(
            status_field(&tun_status(binary, &socket).unwrap(), "name"),
            Some(name.clone())
        );
        assert_eq!(default_route().interface, original.interface);
        assert_eq!(default_route().gateway, original.gateway);
        assert!(zero.is_running());
        assert_eq!(zero.child.as_ref().unwrap().id(), pid);
    }
    zero.stop();
}

fn delete(arguments: &[&str]) {
    let output = Command::new("/sbin/route")
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn intact(tun: &str, egress: &str) -> bool {
    let capture = route_get("128.0.0.0/1");
    let capture = String::from_utf8_lossy(&capture.stdout);
    let bypass = route_get("1.1.1.1");
    let bypass = String::from_utf8_lossy(&bypass.stdout);
    let scoped = Command::new("/sbin/route")
        .args(["-n", "get", "-inet", "-ifscope", egress, "default"])
        .output()
        .unwrap();
    let scoped = String::from_utf8_lossy(&scoped.stdout);
    route_field(&capture, "interface").as_deref() == Some(tun)
        && route_field(&capture, "mask").as_deref() == Some("128.0.0.0")
        && route_field(&bypass, "interface").as_deref() == Some(egress)
        && bypass.contains("HOST")
        && route_field(&scoped, "interface").as_deref() == Some(egress)
        && scoped.contains("IFSCOPE")
}
