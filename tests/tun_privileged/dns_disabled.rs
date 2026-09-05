use super::*;

#[test]
#[ignore = "requires an isolated administrator/root runner, TUN, and working system DNS/internet"]
fn privileged_tun_without_builtin_dns_preserves_system_resolution() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().unwrap();
    let socket = control_socket(directory.path(), false);
    let port = free_tcp_port();
    let servers = zero_platform_tokio::system_dns_servers().expect("discover host DNS");
    let server = servers
        .into_iter()
        .find(|server| system_dns_answer(*server).is_ok())
        .expect("baseline system DNS must answer before capture");
    let mut config: serde_json::Value =
        serde_json::from_str(&direct_udp_config_json(port, true)).unwrap();
    config["runtime"].as_object_mut().unwrap().remove("dns");
    config["inbounds"][0]["protocol"] = serde_json::json!({"type":"mixed"});
    let active = directory.path().join("dns-disabled.json");
    let stopped = directory.path().join("stopped.json");
    std::fs::write(&active, serde_json::to_vec(&config).unwrap()).unwrap();
    config["runtime"]["tun"] = serde_json::Value::Null;
    std::fs::write(&stopped, serde_json::to_vec(&config).unwrap()).unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &active, &socket);
        wait_for_tun(binary, &socket, true, false);
        let name = assert_tun_os_configured(binary, &socket, false, false);
        for _ in 0..3 {
            system_dns_answer(server).expect("host DNS remains reachable during capture");
            // Forces the kernel's fallback OS resolver through a domain target.
            assert_http_connect_domain_through_mixed_inbound(port, "example.com");
            wait_for_tun(binary, &socket, true, false);
            thread::sleep(Duration::from_secs(2));
        }
        run_cli(
            binary,
            ["reload", path(&stopped), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&name);
        assert_route_journals_clean(&active);
        process.kill_and_wait();
        system_dns_answer(server).expect("host DNS remains reachable after cleanup");
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &active, &stopped);
        std::panic::resume_unwind(payload);
    }
}

fn system_dns_answer(server: IpAddr) -> std::io::Result<()> {
    let bind = if server.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind)?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    socket.connect(SocketAddr::new(server, 53))?;
    socket.send(&dns_query(0x7142, "example.com"))?;
    let mut response = [0; 2048];
    let length = socket.recv(&mut response)?;
    if length < 12
        || response[..2] != 0x7142_u16.to_be_bytes()
        || first_a_answer(&response[..length]).is_none()
    {
        return Err(std::io::Error::other(
            "system DNS did not return the requested A answer",
        ));
    }
    Ok(())
}
