use super::super::*;
use super::{client, peer};

#[cfg(windows)]
#[test]
#[ignore = "requires an isolated Windows Administrator runner, Wintun, and a physical IPv4 interface"]
fn privileged_windows_http_direct_and_tun_control() {
    run_control();
}

fn run_control() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let probe = UdpSocket::bind("0.0.0.0:0").unwrap();
    // Route lookup only; this does not send a packet to an external peer.
    probe.connect("1.1.1.1:53").unwrap();
    let physical = probe.local_addr().unwrap().ip();
    assert!(physical.is_ipv4() && !physical.is_loopback());
    // The runner is shared with setup helpers and diagnostics. Do not assume
    // the conventional HTTP-alt port is free; an ephemeral listener keeps the
    // controlled peer independent of the runner image and resident services.
    let mut peer = peer::Peer::start(SocketAddr::new(physical, 0));
    let physical_target = peer.address;
    client::run_suite("direct-before", || connect_from(physical, physical_target));

    let IpAddr::V4(physical_v4) = physical else {
        unreachable!()
    };
    let dns = MockDns::start_with_ipv4_answer(physical_v4.octets(), Duration::ZERO);
    let directory = tempfile::tempdir().unwrap();
    let socket = control_socket(directory.path(), false);
    let binary = env!("CARGO_BIN_EXE_zero");
    let active = directory.path().join("http-control.json");
    let stopped = directory.path().join("stopped.json");
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json(false, free_tcp_port(), None, true, false)).unwrap();
    config["runtime"]["dns"] = serde_json::json!({
        "servers": { "local": { "type": "udp", "host": dns.address.ip(), "port": dns.address.port() } },
        "default_server": "local",
        "policy": { "address_family": "ipv4_only" },
        "answer": { "type": "fake_ip", "cidr": "198.18.0.0/15", "ttl_seconds": 600 }
    });
    std::fs::write(&active, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
    config["runtime"]["tun"] = serde_json::Value::Null;
    std::fs::write(&stopped, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &active, &socket);
        wait_for_tun(binary, &socket, true, false);
        let name = assert_tun_os_configured(binary, &socket, false, false);
        let synthetic = intercepted_address();
        let target = SocketAddr::new(synthetic, physical_target.port());
        assert_ne!(
            target, physical_target,
            "TUN target must not use the local-address shortcut"
        );
        assert_tun_route_selected(&name, target);
        eprintln!(
            "HTTP control mapping: {target} -> {} -> {physical_target}; TUN={name}",
            peer::HOST
        );
        // Strict-route WFP policy deliberately rejects this test process's
        // physical bypass. Keep the guard enabled and use before/after as
        // the direct baselines instead of weakening policy for the test.
        let blocked = connect_from(physical, physical_target)
            .expect_err("strict route must block the physical bypass during TUN capture");
        assert_eq!(
            blocked.raw_os_error(),
            Some(10013),
            "unexpected bypass failure: {blocked}"
        );
        eprintln!("HTTP control physical bypass correctly blocked during capture: {blocked}");
        client::run_suite("tun", || connect_from(tun_source(false), target));
        run_cli(
            binary,
            ["reload", path(&stopped), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&name);
        assert_route_journals_clean(&active);
        process.kill_and_wait();
        client::run_suite("direct-after", || connect_from(physical, physical_target));
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &active, &stopped);
        std::panic::resume_unwind(payload);
    }
    peer.stop();
    peer.assert_observations(3);
}

fn connect_from(source: IpAddr, target: SocketAddr) -> std::io::Result<TcpStream> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.bind(&SocketAddr::new(source, 0).into())?;
    socket.connect_timeout(&target.into(), Duration::from_secs(5))?;
    Ok(socket.into())
}

fn intercepted_address() -> IpAddr {
    let socket = UdpSocket::bind(SocketAddr::new(tun_source(false), 0)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let query = dns_query(0x7351, peer::HOST);
    socket.send_to(&query, "203.0.113.53:53").unwrap();
    let mut response = [0; 2048];
    let count = socket.recv(&mut response).unwrap();
    assert!(count >= 12 && response[..2] == query[..2]);
    let address = first_a_answer(&response[..count]).expect("controlled DNS A answer");
    let bytes = address.octets();
    assert!(
        bytes[0] == 198 && matches!(bytes[1], 18 | 19),
        "expected Fake-IP, got {address}"
    );
    address.into()
}
