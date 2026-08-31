use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use socket2::{Domain, Protocol, Socket, Type};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::{Engine, RouteDecision};

static TUN_E2E_LOCK: Mutex<()> = Mutex::new(());
const DIRECT_UDP_REUSED_SOURCE_ROUNDS: u16 = 16;
const DIRECT_UDP_SOURCE_CHURN_ROUNDS: u16 = 32;
const DIRECT_UDP_EXPECTED_ACTIVE_SOURCE_TUPLES: usize = DIRECT_UDP_SOURCE_CHURN_ROUNDS as usize + 1;
const ONLY_AAAA_E2E_DOMAIN: &str = "only-aaaa.zero.invalid";

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
    #[cfg(windows)]
    let firewall_profiles = windows_firewall_profile_defaults();
    #[cfg(target_os = "macos")]
    let loopback_before = macos_loopback_diagnostics();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        #[cfg(windows)]
        assert_eq!(firewall_profiles, windows_firewall_profile_defaults());
        let _initial_name = assert_tun_os_configured(binary, &socket, false, false);
        #[cfg(target_os = "macos")]
        assert_macos_loopback_listener_reachable(binary, &socket, listen_port, &loopback_before);
        for _ in 0..8 {
            assert_tcp_through_tun(tcp_target);
        }
        #[cfg(target_os = "linux")]
        assert_same_uid_unmarked_physical_socket_blocked(binary, &socket, tcp_target);
        assert_dns_hijack_through_tun(false);

        // A hard kill leaves the route journal behind. The next process must
        // recover it before re-installing the same TUN routes.
        process.kill_and_wait();
        #[cfg(windows)]
        assert_eq!(firewall_profiles, windows_firewall_profile_defaults());
        #[cfg(target_os = "macos")]
        assert_macos_crash_state_loopback_reachable(&loopback_before);
        assert_route_journal_present(&direct_path, 1);
        std::fs::write(&direct_path, &direct_config).unwrap();

        let mut recovered = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        #[cfg(windows)]
        assert_eq!(firewall_profiles, windows_firewall_profile_defaults());
        let recovered_name = assert_tun_os_configured(binary, &socket, false, false);
        #[cfg(target_os = "macos")]
        assert_macos_loopback_listener_reachable(binary, &socket, listen_port, &loopback_before);
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
        #[cfg(target_os = "macos")]
        assert_macos_loopback_routes_restored(&loopback_before);
        #[cfg(windows)]
        assert_eq!(firewall_profiles, windows_firewall_profile_defaults());
        recovered.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires root and the macOS utun backend"]
fn privileged_macos_tun_loopback_remains_reachable_across_crash_recovery() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary macOS loopback E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let direct_config = direct_udp_config_json(listen_port, true);
    let non_strict_config = direct_udp_config_json_with_strict_route(listen_port, false);
    let stopped_config = direct_udp_config_json(listen_port, false);
    let direct_path = directory.path().join("macos-loopback.json");
    let non_strict_path = directory.path().join("macos-loopback-non-strict.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&non_strict_path, non_strict_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();
    let loopback_before = macos_loopback_diagnostics();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut baseline = spawn_zero(binary, &stopped_path, &socket);
        wait_for_tun(binary, &socket, false, false);
        assert_macos_loopback_listener_connects(listen_port, "before TUN activation");
        baseline.kill_and_wait();
        eprintln!("macOS loopback baseline is reachable before TUN activation");

        let mut non_strict = spawn_zero(binary, &non_strict_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let non_strict_name = assert_tun_os_configured(binary, &socket, false, false);
        assert_macos_loopback_listener_reachable(binary, &socket, listen_port, &loopback_before);
        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&non_strict_name);
        non_strict.kill_and_wait();
        eprintln!("macOS loopback is reachable with TUN strict_route=false");

        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let _initial_name = assert_tun_os_configured(binary, &socket, false, false);
        assert_macos_loopback_listener_reachable(binary, &socket, listen_port, &loopback_before);

        process.kill_and_wait();
        assert_macos_crash_state_loopback_reachable(&loopback_before);
        assert_route_journal_present(&direct_path, 1);

        let mut recovered = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let recovered_name = assert_tun_os_configured(binary, &socket, false, false);
        assert_macos_loopback_listener_reachable(binary, &socket, listen_port, &loopback_before);

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&recovered_name);
        assert_route_journals_clean(&direct_path);
        assert_macos_loopback_routes_restored(&loopback_before);
        recovered.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires administrator/root, a TUN backend, and a reachable external DNS server"]
fn privileged_tun_ipv4_direct_udp_dns_does_not_self_capture() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary UDP E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let dns_target = std::env::var("ZERO_TUN_E2E_DNS_ADDR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "223.5.5.5:53".to_owned())
        .parse::<SocketAddr>()
        .expect("ZERO_TUN_E2E_DNS_ADDR must be an IPv4 DNS socket");
    assert!(dns_target.is_ipv4(), "DNS target must be IPv4");
    let direct_config = direct_udp_config_json(listen_port, true);
    let stopped_config = direct_udp_config_json(listen_port, false);
    let direct_path = directory.path().join("direct-udp.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let tun_name = assert_tun_os_configured(binary, &socket, false, false);
        let flow_ids_before = runtime_tun_udp_flow_ids(binary, &socket, dns_target);
        assert_direct_udp_dns_through_tun(dns_target);
        assert_tun_udp_flow_growth_bounded(binary, &socket, dns_target, &flow_ids_before);

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&tun_name);
        assert_route_journals_clean(&direct_path);
        process.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires administrator/root, a TUN backend, internet access, and reachable Cloudflare DoH"]
fn privileged_tun_ipv4_fake_ip_doh_direct_domain_does_not_self_capture() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary DoH E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let direct_config = fake_ip_doh_config_json(listen_port, true);
    let stopped_config = fake_ip_doh_config_json(listen_port, false);
    let direct_path = directory.path().join("fake-ip-doh.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();
    #[cfg(target_os = "macos")]
    let loopback_before = macos_loopback_diagnostics();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, false);
        let tun_name = assert_tun_os_configured(binary, &socket, false, false);
        #[cfg(target_os = "macos")]
        assert_macos_loopback_listener_reachable(binary, &socket, listen_port, &loopback_before);
        assert_http_connect_domain_through_mixed_inbound(listen_port, "example.com");
        assert_http_domain_through_fake_ip("example.com");
        assert_dns_underlay_not_captured(binary, &socket);

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&tun_name);
        assert_route_journals_clean(&direct_path);
        #[cfg(target_os = "macos")]
        assert_macos_loopback_routes_restored(&loopback_before);
        process.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        best_effort_route_recovery(binary, &socket, &direct_path, &stopped_path);
        std::panic::resume_unwind(payload);
    }
}

#[test]
#[ignore = "requires administrator/root, a TUN backend, internet access, and reachable Cloudflare DoH"]
fn privileged_command_managed_tun_publishes_egress_before_capture() {
    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary command TUN E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let config_path = directory.path().join("command-tun.json");
    std::fs::write(&config_path, fake_ip_doh_config_json(listen_port, false)).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &config_path, &socket);
        wait_for_tun(binary, &socket, false, false);
        run_cli(
            binary,
            [
                "tun",
                "start",
                "--name",
                "ZeroTunCmd",
                "--addr",
                "10.66.0.1/24",
                "--mask",
                "255.255.255.0",
                "--tag",
                "tun-command-e2e",
                "--single-stack",
                "--socket",
                path(&socket),
            ],
        );
        assert!(
            try_wait_for_tun(
                binary,
                &socket,
                true,
                Some(false),
                Some(false),
                tun_state_timeout(),
            ),
            "timed out waiting for command-managed TUN state"
        );
        let tun_name = assert_tun_os_configured(binary, &socket, false, false);
        assert_http_connect_domain_through_mixed_inbound(listen_port, "example.com");
        assert_http_domain_through_fake_ip("example.com");
        assert_dns_underlay_not_captured(binary, &socket);
        assert_active_flows_have_safe_egress(binary, &socket);

        run_cli(binary, ["tun", "stop", "--socket", path(&socket)]);
        wait_for_tun(binary, &socket, false, false);
        assert_tun_os_cleanup(&tun_name);
        assert_route_journals_clean(&config_path);
        process.kill_and_wait();
    }));
    if let Err(payload) = outcome {
        let _ = Command::new(binary)
            .args(["tun", "stop", "--socket", path(&socket)])
            .output();
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

#[test]
fn offline_dual_stack_config_routes_original_ips_and_recovered_domain() {
    let config = RuntimeConfig::parse(&dual_stack_config_json(
        1080,
        true,
        "127.0.0.1:1081".parse().unwrap(),
        "127.0.0.1:5353".parse().unwrap(),
    ))
    .expect("parse offline dual-stack E2E config");
    let engine = Engine::new(config).expect("build offline dual-stack E2E router");

    for target in [
        Address::Ipv4([1, 1, 1, 1]),
        Address::Ipv6([
            0x26, 0x06, 0x47, 0x00, 0x47, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x11, 0x11,
        ]),
        Address::Domain("example.com".to_owned()),
    ] {
        assert_eq!(
            engine.route_decision(&target, None),
            RouteDecision::Route("mock-socks".to_owned()),
            "offline dual-stack E2E target must reach the deterministic mock outbound: {target:?}"
        );
    }
}

#[cfg(windows)]
#[test]
#[ignore = "requires Administrator privileges, wintun.dll, IPv4 internet, and no native IPv6 default route"]
fn privileged_windows_ipv4_only_tun_falls_back_trusted_ipv6_domains() {
    if windows_has_native_ipv6_default_route() {
        eprintln!("skipping IPv4-only TUN E2E because this host has a native IPv6 default route");
        return;
    }

    let _guard = TUN_E2E_LOCK.lock().expect("TUN E2E lock poisoned");
    let binary = env!("CARGO_BIN_EXE_zero");
    let directory = tempfile::tempdir().expect("temporary IPv4-only TUN E2E directory");
    let socket = control_socket(directory.path(), false);
    let listen_port = free_tcp_port();
    let fallback_server = MockTcpResponder::start();
    let default_domain = "fallback.zero.test";
    let domain =
        std::env::var("ZERO_TUN_E2E_FALLBACK_DOMAIN").unwrap_or_else(|_| default_domain.to_owned());
    // Windows `getaddrinfo` may suppress AAAA answers under AI_ADDRCONFIG on
    // the exact IPv4-only hosts covered by this test. Use a deterministic
    // original IPv6 destination while the trusted SNI supplies the domain
    // that Zero must re-resolve to A records.
    let original_ipv6 = std::env::var("ZERO_TUN_E2E_FALLBACK_IPV6")
        .unwrap_or_else(|_| format!("[2001:db8::f]:{}", fallback_server.address.port()))
        .parse::<SocketAddr>()
        .expect("ZERO_TUN_E2E_FALLBACK_IPV6 must be an IPv6 socket address");
    assert!(original_ipv6.is_ipv6());
    let fallback_ipv4 = std::env::var("ZERO_TUN_E2E_FALLBACK_IPV4")
        .ok()
        .map(|address| {
            address
                .parse::<SocketAddr>()
                .expect("ZERO_TUN_E2E_FALLBACK_IPV4 must be an IPv4 socket address")
        })
        .unwrap_or(fallback_server.address);
    assert!(fallback_ipv4.is_ipv4(), "fallback E2E target must be IPv4");
    let tcp_v4 = std::env::var("ZERO_TUN_E2E_TCP_ADDR")
        .unwrap_or_else(|_| "1.1.1.1:80".to_owned())
        .parse::<SocketAddr>()
        .expect("ZERO_TUN_E2E_TCP_ADDR must be an IPv4 socket address");
    assert!(tcp_v4.is_ipv4(), "TCP E2E target must be IPv4");
    let mock_dns = MockDns::start_for_ipv4_only(fallback_ipv4.ip());

    let direct_config =
        config_json_with_dns(false, listen_port, None, true, true, mock_dns.address);
    let stopped_config =
        config_json_with_dns(false, listen_port, None, false, true, mock_dns.address);
    let direct_path = directory.path().join("ipv4-only-dual-stack.json");
    let stopped_path = directory.path().join("stopped.json");
    std::fs::write(&direct_path, &direct_config).unwrap();
    std::fs::write(&stopped_path, stopped_config).unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut process = spawn_zero(binary, &direct_path, &socket);
        wait_for_tun(binary, &socket, true, true);
        let tun_name = assert_tun_os_configured(binary, &socket, false, true);
        let status = tun_status(binary, &socket);
        assert_ne!(
            status_field(&status, "egress_v4").as_deref(),
            Some("-"),
            "IPv4-only TUN must publish native IPv4 egress: {status}"
        );
        assert_eq!(
            status_field(&status, "egress_v6").as_deref(),
            Some("-"),
            "IPv4-only TUN must not publish its IPv4 carrier as native IPv6 egress: {status}"
        );

        assert_tun_route_selected(&tun_name, tcp_v4);
        assert_tcp_through_tun(tcp_v4);
        assert_dns_hijack_through_tun(false);
        assert_concurrent_dns_queries_are_coalesced(&mock_dns);
        assert_tls_sni_ipv6_falls_back_to_ipv4(original_ipv6, &domain);
        assert_eq!(
            fallback_server.accept_count(),
            1,
            "trusted IPv6 domain did not create exactly one IPv4 fallback connection"
        );
        assert_only_aaaa_fallback_is_bounded(original_ipv6, &mock_dns);
        assert_literal_ipv6_fails_quickly_without_dns_fallback();
        let runtime_status =
            run_cli_output(binary, ["status", "--json", "--socket", path(&socket)]);
        assert!(
            runtime_status.contains("tun_ipv6_egress_unavailable"),
            "trusted-domain fallback observation is missing: {runtime_status}"
        );
        assert!(
            runtime_status.contains("2001:db8::1")
                && runtime_status.contains("tun_egress_unavailable"),
            "literal IPv6 failure is not explicitly observable: {runtime_status}"
        );
        let tun_runtime_status = tun_status(binary, &socket);
        assert_eq!(
            status_field(&tun_runtime_status, "ipv4_state").as_deref(),
            Some("available")
        );
        assert_eq!(
            status_field(&tun_runtime_status, "ipv6_state").as_deref(),
            Some("unavailable")
        );
        assert_eq!(
            status_field(&tun_runtime_status, "ipv6_reason").as_deref(),
            Some("no_default_route")
        );
        assert_eq!(
            status_field(&tun_runtime_status, "ipv6_to_ipv4_fallbacks").as_deref(),
            Some("1")
        );

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, true);
        assert_tun_os_cleanup(&tun_name);
        assert_route_journals_clean(&direct_path);

        std::fs::write(&direct_path, &direct_config).unwrap();
        run_cli(
            binary,
            ["reload", path(&direct_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, true, true);
        let restarted_name = assert_tun_os_configured(binary, &socket, false, true);
        let restarted_status = tun_status(binary, &socket);
        assert_eq!(
            status_field(&restarted_status, "ipv6_to_ipv4_fallbacks").as_deref(),
            Some("0"),
            "a new TUN lifecycle retained the previous fallback count: {restarted_status}"
        );
        assert_only_aaaa_fallback_is_bounded(original_ipv6, &mock_dns);

        run_cli(
            binary,
            ["reload", path(&stopped_path), "--socket", path(&socket)],
        );
        wait_for_tun(binary, &socket, false, true);
        assert_tun_os_cleanup(&restarted_name);
        assert_route_journals_clean(&direct_path);
        process.kill_and_wait();
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
    if try_wait_for_tun(binary, socket, true, Some(true), None, tun_state_timeout()) {
        let _ = Command::new(binary)
            .args(["reload", path(stopped_path), "--socket", path(socket)])
            .output();
        let _ = try_wait_for_tun(binary, socket, false, None, None, tun_state_timeout());
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
                "servers": {
                    "test": { "type": "udp", "host": dns_address, "port": 53 }
                },
                "default_server": "test"
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

#[cfg(windows)]
fn config_json_with_dns(
    ipv6: bool,
    listen_port: u16,
    blocked: Option<IpAddr>,
    tun: bool,
    dual_stack: bool,
    dns: SocketAddr,
) -> String {
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json(ipv6, listen_port, blocked, tun, dual_stack)).unwrap();
    config["runtime"]["dns"]["servers"]["test"]["host"] =
        serde_json::Value::String(dns.ip().to_string());
    config["runtime"]["dns"]["servers"]["test"]["port"] = serde_json::Value::from(dns.port());
    serde_json::to_string_pretty(&config).unwrap()
}

fn direct_udp_config_json(listen_port: u16, tun: bool) -> String {
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json(false, listen_port, None, tun, false)).unwrap();
    if tun {
        config["runtime"]["tun"]["dns_hijack"] = serde_json::Value::Bool(false);
    }
    serde_json::to_string_pretty(&config).unwrap()
}

#[cfg(target_os = "macos")]
fn direct_udp_config_json_with_strict_route(listen_port: u16, strict_route: bool) -> String {
    let mut config: serde_json::Value =
        serde_json::from_str(&direct_udp_config_json(listen_port, true)).unwrap();
    config["runtime"]["tun"]["strict_route"] = serde_json::Value::Bool(strict_route);
    serde_json::to_string_pretty(&config).unwrap()
}

fn fake_ip_doh_config_json(listen_port: u16, tun: bool) -> String {
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json(false, listen_port, None, tun, false)).unwrap();
    config["runtime"]["dns"] = serde_json::json!({
        "servers": {
            "global": {
                "type": "doh",
                "host": "cloudflare-dns.com",
                "port": 443,
                "path": "/dns-query",
                "bootstrap": ["1.1.1.1", "1.0.0.1"]
            }
        },
        "default_server": "global",
        "answer": {
            "type": "fake_ip",
            "cidr": "198.18.0.0/15",
            "ttl_seconds": 60
        }
    });
    config["inbounds"][0]["protocol"] = serde_json::json!({ "type": "mixed" });
    serde_json::to_string_pretty(&config).unwrap()
}

fn dual_stack_config_json(
    listen_port: u16,
    tun: bool,
    socks: SocketAddr,
    dns: SocketAddr,
) -> String {
    let mut config: serde_json::Value =
        serde_json::from_str(&config_json(false, listen_port, None, tun, true)).unwrap();
    config["runtime"]["dns"]["servers"] = serde_json::json!({
        "test": {
            "type": "udp",
            "host": dns.ip().to_string(),
            "port": dns.port()
        }
    });
    config["outbounds"] = serde_json::json!([{
        "tag": "mock-socks",
        "protocol": {
            "type": "socks5",
            "server": socks.ip().to_string(),
            "port": socks.port()
        }
    }]);
    config["route"]["rules"] = serde_json::json!([{
        "condition": {
            "type": "or",
            "items": [
                { "type": "ip", "values": ["1.1.1.1/32", "2606:4700:4700::1111/128"] },
                { "type": "domain", "values": ["example.com"] }
            ]
        },
        "action": { "type": "route", "outbound": "mock-socks" }
    }]);
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
            running.then_some(true),
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
    managed_by_config: Option<bool>,
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
                        || managed_by_config.is_some_and(|managed| {
                            !stdout.contains(&format!("managed_by_config={managed}"))
                        })
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
  if (@($ipv4 | Where-Object { $_.IPAddress -eq '10.66.0.1' -and $_.PrefixLength -eq 24 -and $_.AddressState -eq 'Preferred' }).Count -ne 1) { throw "IPv4 TUN address is not preferred" }
  $mtu4 = Get-NetIPInterface -InterfaceAlias $name -AddressFamily IPv4
  if ($mtu4.NlMtu -ne 1400) { throw "IPv4 TUN MTU is not 1400" }
  $routes4 = @(Get-NetRoute -InterfaceAlias $name -AddressFamily IPv4)
  foreach ($prefix in @('0.0.0.0/1', '128.0.0.0/1')) {
    if (@($routes4 | Where-Object { $_.DestinationPrefix -eq $prefix }).Count -ne 1) { throw "missing route $prefix" }
  }
}
if ($check6) {
  $ipv6 = @(Get-NetIPAddress -InterfaceAlias $name -AddressFamily IPv6)
  if (@($ipv6 | Where-Object { $_.IPAddress -eq 'fd66::1' -and $_.PrefixLength -eq 64 -and $_.AddressState -eq 'Preferred' }).Count -ne 1) { throw "IPv6 TUN address is not preferred" }
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

#[cfg(windows)]
fn windows_firewall_profile_defaults() -> String {
    let script = r#"
$ErrorActionPreference='Stop'
$profiles=@(foreach($name in @('Domain','Private','Public')) {
  $profile=Get-NetFirewallProfile -Name $name -ErrorAction Stop
  [pscustomobject]@{name=$profile.Name;action=$profile.DefaultOutboundAction.ToString()}
})
ConvertTo-Json -InputObject $profiles -Compress
"#;
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .output()
        .expect("snapshot Windows Firewall profile defaults");
    assert!(
        output.status.success(),
        "snapshot Windows Firewall profile defaults:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Windows Firewall profile snapshot must be UTF-8")
        .trim()
        .to_owned()
}

#[cfg(windows)]
fn windows_has_native_ipv6_default_route() -> bool {
    let script = r#"
$routes = @(Get-NetRoute -AddressFamily IPv6 -DestinationPrefix '::/0' -ErrorAction SilentlyContinue |
  Where-Object { $_.State -eq 'Alive' -and $_.InterfaceAlias -notlike 'ZeroTun*' })
if ($routes.Count -gt 0) { exit 0 } else { exit 1 }
"#;
    Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
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
            panic!("TCP request through TUN to {target}: {error}");
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
    assert!(
        size > 0,
        "TCP target {target} returned no bytes through TUN"
    );
}

#[cfg(windows)]
fn assert_tls_sni_ipv6_falls_back_to_ipv4(target: SocketAddr, domain: &str) {
    assert!(
        target.is_ipv6(),
        "fallback probe requires an original IPv6 target"
    );
    let client_hello = tls_client_hello(domain);
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))
        .expect("create IPv6 fallback socket");
    socket
        .bind(&SocketAddr::new(tun_source(true), 0).into())
        .expect("bind fallback socket to the TUN IPv6 address");
    socket
        .connect_timeout(&target.into(), Duration::from_secs(5))
        .expect("connect original IPv6 target through the TUN stack");
    let mut stream: TcpStream = socket.into();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .write_all(&client_hello)
        .expect("write TLS ClientHello through TUN");
    let mut response = [0_u8; 64];
    let size = stream
        .read(&mut response)
        .expect("trusted IPv6 target should receive a TLS response through IPv4 fallback");
    assert!(size > 0, "IPv4 fallback target returned no TLS bytes");
    assert!(
        matches!(response[0], 20..=23),
        "IPv4 fallback returned an unexpected TLS content type: {}",
        response[0]
    );
}

#[cfg(windows)]
fn tls_client_hello(domain: &str) -> Vec<u8> {
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, ClientConnection, RootCertStore};

    let config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .expect("supported TLS versions")
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
    let server_name = ServerName::try_from(domain.to_owned()).expect("valid fallback SNI");
    let mut connection =
        ClientConnection::new(Arc::new(config), server_name).expect("create TLS fallback probe");
    let mut client_hello = Vec::new();
    connection
        .write_tls(&mut client_hello)
        .expect("serialize fallback ClientHello");
    client_hello
}

#[cfg(windows)]
fn assert_concurrent_dns_queries_are_coalesced(mock_dns: &MockDns) {
    const DOMAIN: &str = "coalesced.zero.test";
    const CLIENTS: u16 = 16;
    let before = mock_dns.query_count(DOMAIN, 1);
    let barrier = Arc::new(std::sync::Barrier::new(usize::from(CLIENTS) + 1));
    let mut workers = Vec::new();
    for id in 1..=CLIENTS {
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let socket = UdpSocket::bind(SocketAddr::new(tun_source(false), 0)).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let target = "8.8.8.8:53".parse::<SocketAddr>().unwrap();
            let query = dns_query(0x7200 + id, DOMAIN);
            barrier.wait();
            socket.send_to(&query, target).unwrap();
            let mut response = [0_u8; 2048];
            let (size, _) = socket.recv_from(&mut response).unwrap();
            assert!(size >= 12);
            assert_eq!(&response[..2], &(0x7200 + id).to_be_bytes());
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().expect("join concurrent DNS E2E client");
    }
    assert_eq!(
        mock_dns.query_count(DOMAIN, 1) - before,
        1,
        "concurrent intercepted DNS queries were amplified upstream"
    );
}

#[cfg(windows)]
fn assert_only_aaaa_fallback_is_bounded(target: SocketAddr, mock_dns: &MockDns) {
    const CLIENTS: usize = 8;
    let before = mock_dns.query_count(ONLY_AAAA_E2E_DOMAIN, 1);
    let client_hello = Arc::new(tls_client_hello(ONLY_AAAA_E2E_DOMAIN));
    let barrier = Arc::new(std::sync::Barrier::new(CLIENTS + 1));
    let mut workers = Vec::new();
    for _ in 0..CLIENTS {
        let barrier = Arc::clone(&barrier);
        let client_hello = Arc::clone(&client_hello);
        workers.push(thread::spawn(move || {
            let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP)).unwrap();
            socket
                .bind(&SocketAddr::new(tun_source(true), 0).into())
                .unwrap();
            socket
                .connect_timeout(&target.into(), Duration::from_secs(3))
                .unwrap();
            let mut stream: TcpStream = socket.into();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            barrier.wait();
            stream.write_all(client_hello.as_slice()).unwrap();
            let mut response = [0_u8; 1];
            assert!(
                !matches!(stream.read(&mut response), Ok(size) if size > 0),
                "AAAA-only fallback unexpectedly produced an IPv4 response"
            );
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().expect("join AAAA-only fallback E2E client");
    }
    assert_eq!(
        mock_dns.query_count(ONLY_AAAA_E2E_DOMAIN, 1) - before,
        1,
        "AAAA-only fallback caused repeated A lookups instead of one coalesced negative result"
    );
}

#[cfg(windows)]
fn assert_literal_ipv6_fails_quickly_without_dns_fallback() {
    let target = "[2001:db8::1]:443".parse::<SocketAddr>().unwrap();
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))
        .expect("create literal IPv6 probe socket");
    socket
        .bind(&SocketAddr::new(tun_source(true), 0).into())
        .expect("bind literal IPv6 probe to the TUN address");
    socket
        .connect_timeout(&target.into(), Duration::from_secs(2))
        .expect("TUN stack should accept the local literal IPv6 connection");
    let mut stream: TcpStream = socket.into();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let started = Instant::now();
    stream
        .write_all(b"GET / HTTP/1.0\r\n\r\n")
        .expect("write literal IPv6 probe");
    let mut response = [0_u8; 1];
    let result = stream.read(&mut response);
    assert!(
        !matches!(result, Ok(size) if size > 0),
        "literal IPv6 unexpectedly received a network response"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "literal IPv6 did not fail quickly when native IPv6 egress was unavailable"
    );
}

#[cfg(target_os = "linux")]
fn assert_same_uid_unmarked_physical_socket_blocked(
    binary: &str,
    control_socket: &std::path::Path,
    target: SocketAddr,
) {
    let status = tun_status(binary, control_socket);
    let egress = status_field(&status, "egress_v4")
        .filter(|egress| egress != "-")
        .expect("Linux strict-route status must expose its physical IPv4 egress");
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
        .expect("create unmarked strict-route probe socket");
    socket
        .bind_device(Some(egress.as_bytes()))
        .expect("bind unmarked probe to the physical egress");
    let result = socket.connect_timeout(&target.into(), Duration::from_secs(3));
    assert!(
        result.is_err(),
        "same-UID socket without Zero's strict-route mark bypassed through `{egress}` to {target}"
    );
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

fn assert_http_domain_through_fake_ip(domain: &str) {
    let target: SocketAddr = "8.8.8.8:53".parse().unwrap();
    let socket = UdpSocket::bind(SocketAddr::new(tun_source(false), 0)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let query = dns_query(0x6622, domain);
    socket
        .send_to(&query, target)
        .expect("send hijacked Fake-IP DNS query");
    let mut response = [0_u8; 2048];
    let (size, _) = socket
        .recv_from(&mut response)
        .expect("receive hijacked Fake-IP DNS reply");
    let fake_ip = first_a_answer(&response[..size]).expect("Fake-IP A answer");
    assert!(
        u32::from(fake_ip) >= u32::from(std::net::Ipv4Addr::new(198, 18, 0, 0))
            && u32::from(fake_ip) <= u32::from(std::net::Ipv4Addr::new(198, 19, 255, 255)),
        "DNS answer {fake_ip} is outside the Fake-IP pool"
    );

    let target = SocketAddr::new(IpAddr::V4(fake_ip), 80);
    let mut stream = TcpStream::connect_timeout(&target, Duration::from_secs(15))
        .unwrap_or_else(|error| panic!("connect to {domain} through Fake-IP {fake_ip}: {error}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
        .write_all(
            format!("GET / HTTP/1.1\r\nHost: {domain}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .expect("write HTTP request through Fake-IP");
    let mut reply = Vec::new();
    let size = (&mut stream)
        .take(64 * 1024)
        .read_to_end(&mut reply)
        .unwrap_or_else(|error| panic!("read {domain} response through Fake-IP: {error}"));
    assert!(size > 0, "Fake-IP direct domain returned no bytes");
}

fn assert_http_connect_domain_through_mixed_inbound(port: u16, domain: &str) {
    let proxy = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port);
    let mut stream = TcpStream::connect_timeout(&proxy, Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("connect to mixed inbound {proxy}: {error}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    stream
        .write_all(
            format!(
                "CONNECT {domain}:80 HTTP/1.1\r\nHost: {domain}:80\r\nProxy-Connection: keep-alive\r\n\r\n"
            )
            .as_bytes(),
        )
        .expect("write HTTP CONNECT request");
    let mut response = Vec::new();
    let mut byte = [0_u8; 1];
    while response.len() < 4096 && !response.ends_with(b"\r\n\r\n") {
        stream
            .read_exact(&mut byte)
            .expect("read HTTP CONNECT response");
        response.push(byte[0]);
    }
    assert!(
        response.starts_with(b"HTTP/1.1 200") || response.starts_with(b"HTTP/1.0 200"),
        "mixed inbound rejected HTTP CONNECT: {}",
        String::from_utf8_lossy(&response)
    );

    stream
        .write_all(
            format!("GET / HTTP/1.1\r\nHost: {domain}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .expect("write HTTP request through CONNECT tunnel");
    let mut reply = Vec::new();
    let size = (&mut stream)
        .take(64 * 1024)
        .read_to_end(&mut reply)
        .unwrap_or_else(|error| panic!("read {domain} response through mixed inbound: {error}"));
    assert!(size > 0, "mixed inbound direct domain returned no bytes");
}

#[cfg(target_os = "macos")]
fn assert_macos_loopback_listener_reachable(
    binary: &str,
    control_socket: &std::path::Path,
    port: u16,
    before: &str,
) {
    let target = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port);
    let flow_ids_before = runtime_tun_tcp_flow_ids(binary, control_socket, target);
    let after = macos_loopback_diagnostics();
    assert!(
        route_lookup_uses_unscoped_loopback(&after, "IPv4 127.0.0.1")
            && route_lookup_uses_loopback(&after, "IPv6 ::1"),
        "macOS loopback routes are not preserved while TUN is active:\nBEFORE\n{before}\nAFTER\n{after}"
    );

    assert_macos_loopback_listener_connects(port, "while TUN is active");
    thread::sleep(Duration::from_millis(100));

    let flow_ids_after = runtime_tun_tcp_flow_ids(binary, control_socket, target);
    assert_eq!(
        flow_ids_after, flow_ids_before,
        "macOS loopback connection created a TUN session for {target}"
    );
}

#[cfg(target_os = "macos")]
fn assert_macos_loopback_listener_connects(port: u16, stage: &str) {
    let target = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port);
    let stream =
        TcpStream::connect_timeout(&target, Duration::from_secs(5)).unwrap_or_else(|error| {
            panic!("macOS loopback listener {target} is unreachable {stage}: {error}")
        });
    drop(stream);
}

#[cfg(target_os = "macos")]
fn assert_macos_crash_state_loopback_reachable(before: &str) {
    let after = macos_loopback_diagnostics();
    assert!(
        route_lookup_uses_unscoped_loopback(&after, "IPv4 127.0.0.1"),
        "macOS loopback bypass disappeared while crash-left TUN routes remained:\nBEFORE\n{before}\nAFTER\n{after}"
    );
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .expect("bind crash-state loopback listener");
    let target = listener
        .local_addr()
        .expect("read loopback listener address");
    TcpStream::connect_timeout(&target, Duration::from_secs(5)).unwrap_or_else(|error| {
        panic!(
            "macOS loopback is unreachable while crash-left TUN routes remain: {error}\nBEFORE\n{before}\nAFTER\n{after}"
        )
    });
}

#[cfg(target_os = "macos")]
fn assert_macos_loopback_routes_restored(before: &str) {
    let after = macos_loopback_diagnostics();
    for heading in ["IPv4 127.0.0.1", "IPv6 ::1"] {
        assert_eq!(
            route_lookup_signature(&after, heading),
            route_lookup_signature(before, heading),
            "macOS loopback route was not restored after TUN cleanup ({heading}):\nBEFORE\n{before}\nAFTER\n{after}"
        );
    }
}

#[cfg(target_os = "macos")]
fn route_lookup_uses_loopback(diagnostics: &str, heading: &str) -> bool {
    route_lookup_signature(diagnostics, heading).is_some_and(|(loopback, _)| loopback)
}

#[cfg(target_os = "macos")]
fn route_lookup_uses_unscoped_loopback(diagnostics: &str, heading: &str) -> bool {
    route_lookup_signature(diagnostics, heading)
        .is_some_and(|(loopback, scoped)| loopback && !scoped)
}

#[cfg(target_os = "macos")]
fn route_lookup_signature(diagnostics: &str, heading: &str) -> Option<(bool, bool)> {
    let marker = format!("-- {heading}\n");
    let (_, tail) = diagnostics.split_once(&marker)?;
    let route = tail.split_once("\n-- ").map_or(tail, |(route, _)| route);
    Some((
        route.lines().any(|line| line.trim() == "interface: lo0"),
        route.lines().any(|line| {
            line.trim()
                .strip_prefix("flags:")
                .is_some_and(|flags| flags.contains("IFSCOPE"))
        }),
    ))
}

#[cfg(target_os = "macos")]
fn macos_loopback_diagnostics() -> String {
    let mut diagnostics = String::new();
    for (heading, arguments) in [
        ("IPv4 127.0.0.1", &["-n", "get", "-inet", "127.0.0.1"][..]),
        ("IPv6 ::1", &["-n", "get", "-inet6", "::1"][..]),
    ] {
        diagnostics.push_str(&format!("-- {heading}\n"));
        diagnostics.push_str(&command_diagnostics("/sbin/route", arguments));
    }
    diagnostics.push_str("-- IPv4 routing table\n");
    diagnostics.push_str(&command_diagnostics(
        "/usr/sbin/netstat",
        &["-rn", "-f", "inet"],
    ));
    diagnostics.push_str("-- IPv6 routing table\n");
    diagnostics.push_str(&command_diagnostics(
        "/usr/sbin/netstat",
        &["-rn", "-f", "inet6"],
    ));
    diagnostics.push_str("-- PF status\n");
    diagnostics.push_str(&command_diagnostics("/sbin/pfctl", &["-s", "info"]));
    diagnostics.push_str("-- PF main rules\n");
    diagnostics.push_str(&command_diagnostics("/sbin/pfctl", &["-s", "rules"]));
    diagnostics.push_str("-- com.apple PF anchors\n");
    diagnostics.push_str(&command_diagnostics(
        "/sbin/pfctl",
        &["-a", "com.apple", "-s", "Anchors"],
    ));
    diagnostics.push_str("-- Zero PF anchor rules\n");
    diagnostics.push_str(&command_diagnostics(
        "/sbin/pfctl",
        &["-a", "com.apple/zero_tun-e2e", "-s", "rules"],
    ));
    diagnostics
}

#[cfg(target_os = "macos")]
fn command_diagnostics(program: &str, arguments: &[&str]) -> String {
    match Command::new(program).args(arguments).output() {
        Ok(output) => format!(
            "status={}\nstdout:\n{}\nstderr:\n{}\n",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(error) => format!("execute {program}: {error}\n"),
    }
}

fn first_a_answer(response: &[u8]) -> Option<std::net::Ipv4Addr> {
    if response.len() < 12 || u16::from_be_bytes([response[6], response[7]]) == 0 {
        return None;
    }
    let mut offset = 12;
    while *response.get(offset)? != 0 {
        let length = *response.get(offset)? as usize;
        offset = offset.checked_add(length + 1)?;
    }
    offset = offset.checked_add(5)?;
    if response.get(offset)? & 0xc0 == 0xc0 {
        offset = offset.checked_add(2)?;
    } else {
        while *response.get(offset)? != 0 {
            let length = *response.get(offset)? as usize;
            offset = offset.checked_add(length + 1)?;
        }
        offset = offset.checked_add(1)?;
    }
    let record_type = u16::from_be_bytes([*response.get(offset)?, *response.get(offset + 1)?]);
    let data_length = u16::from_be_bytes([*response.get(offset + 8)?, *response.get(offset + 9)?]);
    if record_type != 1 || data_length != 4 {
        return None;
    }
    Some(std::net::Ipv4Addr::new(
        *response.get(offset + 10)?,
        *response.get(offset + 11)?,
        *response.get(offset + 12)?,
        *response.get(offset + 13)?,
    ))
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

fn assert_direct_udp_dns_through_tun(target: SocketAddr) {
    let socket = udp_for(target);
    for sequence in 0..DIRECT_UDP_REUSED_SOURCE_ROUNDS {
        assert_direct_dns_round_trip(&socket, target, 0x7000 + sequence);
    }
    drop(socket);

    for sequence in 0..DIRECT_UDP_SOURCE_CHURN_ROUNDS {
        let socket = udp_for(target);
        assert_direct_dns_round_trip(&socket, target, 0x7100 + sequence);
    }
}

fn assert_tun_udp_flow_growth_bounded(
    binary: &str,
    socket: &std::path::Path,
    target: SocketAddr,
    flow_ids_before: &HashSet<u64>,
) {
    let output = run_cli_output(binary, ["status", "--json", "--socket", path(socket)]);
    let snapshot: serde_json::Value =
        serde_json::from_str(&output).expect("runtime status response must be JSON");
    let active_flow_count = tun_udp_active_flow_count(&snapshot, target);
    let flow_ids_after = tun_udp_flow_ids(&snapshot, target);
    let started_flow_count = flow_ids_after.difference(flow_ids_before).count();

    assert!(
        active_flow_count > 0,
        "direct UDP workload did not create an observable TUN flow: {output}"
    );
    assert!(
        active_flow_count <= DIRECT_UDP_EXPECTED_ACTIVE_SOURCE_TUPLES,
        "direct UDP workload exceeded its active source-tuple ceiling ({active_flow_count} > {DIRECT_UDP_EXPECTED_ACTIVE_SOURCE_TUPLES}); recursive self-capture or per-packet association growth is likely: {output}"
    );
    assert!(
        started_flow_count > 0
            && started_flow_count <= DIRECT_UDP_EXPECTED_ACTIVE_SOURCE_TUPLES,
        "direct UDP workload started an unexpected number of scoped flows ({started_flow_count}; expected 1..={DIRECT_UDP_EXPECTED_ACTIVE_SOURCE_TUPLES}); rapidly completed self-capture sessions are likely: {output}"
    );
}

fn tun_udp_active_flow_count(snapshot: &serde_json::Value, target: SocketAddr) -> usize {
    snapshot["runtime"]["active_sessions"]
        .as_array()
        .expect("runtime status must contain active sessions")
        .iter()
        .filter(|flow| is_target_tun_udp_flow(flow, target))
        .count()
}

fn runtime_tun_udp_flow_ids(
    binary: &str,
    socket: &std::path::Path,
    target: SocketAddr,
) -> HashSet<u64> {
    let output = run_cli_output(binary, ["status", "--json", "--socket", path(socket)]);
    let snapshot: serde_json::Value =
        serde_json::from_str(&output).expect("runtime status response must be JSON");
    tun_udp_flow_ids(&snapshot, target)
}

#[cfg(target_os = "macos")]
fn runtime_tun_tcp_flow_ids(
    binary: &str,
    socket: &std::path::Path,
    target: SocketAddr,
) -> HashSet<u64> {
    let output = run_cli_output(binary, ["status", "--json", "--socket", path(socket)]);
    let snapshot: serde_json::Value =
        serde_json::from_str(&output).expect("runtime status response must be JSON");
    ["active_sessions", "recent_completed_sessions"]
        .into_iter()
        .flat_map(|field| {
            snapshot["runtime"][field]
                .as_array()
                .unwrap_or_else(|| panic!("runtime status must contain {field}"))
        })
        .filter(|flow| {
            flow["inbound_tag"] == "tun-e2e"
                && flow["network"] == "tcp"
                && flow["target"]["value"] == target.ip().to_string()
                && flow["port"] == u64::from(target.port())
        })
        .map(|flow| flow["id"].as_u64().expect("flow must expose a numeric id"))
        .collect()
}

fn tun_udp_flow_ids(snapshot: &serde_json::Value, target: SocketAddr) -> HashSet<u64> {
    ["active_sessions", "recent_completed_sessions"]
        .into_iter()
        .flat_map(|field| {
            snapshot["runtime"][field]
                .as_array()
                .unwrap_or_else(|| panic!("runtime status must contain {field}"))
        })
        .filter(|flow| is_target_tun_udp_flow(flow, target))
        .map(|flow| flow["id"].as_u64().expect("flow must expose a numeric id"))
        .collect()
}

fn is_target_tun_udp_flow(flow: &serde_json::Value, target: SocketAddr) -> bool {
    flow["inbound_tag"] == "tun-e2e"
        && flow["network"] == "udp"
        && flow["target"]["value"] == target.ip().to_string()
        && flow["port"] == u64::from(target.port())
}

fn assert_direct_dns_round_trip(socket: &UdpSocket, target: SocketAddr, id: u16) {
    let query = dns_query(id, "example.com");
    socket
        .send_to(&query, target)
        .expect("send direct DNS query through TUN");
    let mut response = [0_u8; 2048];
    let (size, source) = socket
        .recv_from(&mut response)
        .expect("receive direct DNS response through TUN");
    assert_eq!(source, target);
    assert!(size >= 12, "direct DNS response is too short");
    assert_eq!(&response[..2], &id.to_be_bytes());
    assert_ne!(response[2] & 0x80, 0, "DNS response bit must be set");
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

#[test]
fn mock_dns_response_ignores_edns_pseudo_record() {
    for (query_type, expected_data) in [
        (1_u16, &[127, 0, 0, 1][..]),
        (
            28,
            &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1][..],
        ),
    ] {
        let mut query = dns_query(0x6611, "example.com");
        let query_type_offset = query.len() - 4;
        query[query_type_offset..query_type_offset + 2].copy_from_slice(&query_type.to_be_bytes());
        query[10..12].copy_from_slice(&1_u16.to_be_bytes());
        query.extend_from_slice(&[0, 0, 41, 0x10, 0, 0, 0, 0, 0, 0, 0]);

        let response = mock_dns_response(&query).expect("build mock DNS response");
        let (question_end, parsed_type) = dns_question(query.as_slice()).unwrap();
        assert_eq!(parsed_type, query_type);
        assert_eq!(&response[4..12], &[0, 1, 0, 1, 0, 0, 0, 0]);
        assert_eq!(&response[question_end..question_end + 2], &[0xc0, 0x0c]);
        assert_eq!(
            u16::from_be_bytes([response[question_end + 2], response[question_end + 3]]),
            query_type
        );
        assert_eq!(
            &response[response.len() - expected_data.len()..],
            expected_data
        );
    }
}

#[test]
fn flow_ceiling_counts_only_target_tun_udp_sessions_across_lifecycle_states() {
    let target: SocketAddr = "223.5.5.5:53".parse().unwrap();
    let snapshot = serde_json::json!({
        "runtime": {
            "active_sessions": [
                { "id": 1, "inbound_tag": "tun-e2e", "network": "udp", "target": { "value": "223.5.5.5" }, "port": 53 },
                { "id": 2, "inbound_tag": "tun-e2e", "network": "tcp", "target": { "value": "223.5.5.5" }, "port": 53 },
                { "id": 3, "inbound_tag": "control-inbound", "network": "udp", "target": { "value": "223.5.5.5" }, "port": 53 },
                { "id": 4, "inbound_tag": "tun-e2e", "network": "udp", "target": { "value": "1.1.1.1" }, "port": 53 }
            ],
            "recent_completed_sessions": [
                { "id": 5, "inbound_tag": "tun-e2e", "network": "udp", "target": { "value": "223.5.5.5" }, "port": 53 },
                { "id": 1, "inbound_tag": "tun-e2e", "network": "udp", "target": { "value": "223.5.5.5" }, "port": 53 }
            ]
        }
    });

    assert_eq!(tun_udp_active_flow_count(&snapshot, target), 1);
    assert_eq!(tun_udp_flow_ids(&snapshot, target), HashSet::from([1, 5]));
}

fn run_cli<const N: usize>(binary: &str, arguments: [&str; N]) {
    let _ = run_cli_output(binary, arguments);
}

fn run_cli_output<const N: usize>(binary: &str, arguments: [&str; N]) -> String {
    let output = Command::new(binary).args(arguments).output().unwrap();
    assert!(
        output.status.success(),
        "Zero CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("Zero CLI output must be UTF-8")
}

fn assert_dns_underlay_not_captured(binary: &str, socket: &std::path::Path) {
    let flows = run_cli_output(binary, ["flows", "--socket", path(socket)]);
    for endpoint in ["cloudflare-dns.com", "1.1.1.1", "1.0.0.1"] {
        assert!(
            !flows.contains(endpoint),
            "DNS underlay endpoint {endpoint} was captured as an active proxy flow: {flows}"
        );
    }
}

fn assert_active_flows_have_safe_egress(binary: &str, socket: &std::path::Path) {
    let flows = run_cli_output(binary, ["flows", "--socket", path(socket)]);
    for unsafe_reason in ["no_configured_interface", "tun_egress_unavailable"] {
        assert!(
            !flows.contains(unsafe_reason),
            "active flow used unsafe TUN egress state `{unsafe_reason}`: {flows}"
        );
    }
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

#[cfg(windows)]
struct MockTcpResponder {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    accept_count: Arc<std::sync::atomic::AtomicUsize>,
    worker: Option<JoinHandle<()>>,
}

#[cfg(windows)]
impl MockTcpResponder {
    fn start() -> Self {
        let route_probe = UdpSocket::bind("0.0.0.0:0").expect("bind IPv4 route probe");
        route_probe
            .connect("1.1.1.1:53")
            .expect("select physical IPv4 route source");
        let physical_ip = route_probe
            .local_addr()
            .expect("read physical IPv4 route source")
            .ip();
        assert!(
            physical_ip.is_ipv4() && !physical_ip.is_loopback(),
            "IPv4-only TUN E2E requires a non-loopback physical IPv4 address, got {physical_ip}"
        );
        let listener = [8443_u16, 443]
            .into_iter()
            .find_map(|port| TcpListener::bind(SocketAddr::new(physical_ip, port)).ok())
            .expect("bind physical IPv4 fallback responder on a TUN TLS-sniffing port");
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let accept_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker_accept_count = Arc::clone(&accept_count);
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        worker_accept_count.fetch_add(1, Ordering::Relaxed);
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut request = [0_u8; 4096];
                        let _ = stream.read(&mut request);
                        // A syntactically valid TLS fatal alert proves that
                        // the replayed ClientHello reached this controlled
                        // IPv4 endpoint without requiring a certificate.
                        stream
                            .write_all(&[21, 3, 3, 0, 2, 2, 40])
                            .expect("write physical IPv4 fallback TLS alert");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept physical IPv4 fallback connection: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            accept_count,
            worker: Some(worker),
        }
    }

    fn accept_count(&self) -> usize {
        self.accept_count.load(Ordering::Relaxed)
    }
}

#[cfg(windows)]
impl Drop for MockTcpResponder {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .expect("join physical IPv4 fallback responder");
        }
    }
}

struct MockDns {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    #[cfg_attr(not(windows), allow(dead_code))]
    query_counts: Arc<Mutex<HashMap<(String, u16), usize>>>,
    worker: Option<JoinHandle<()>>,
}

impl MockDns {
    fn start() -> Self {
        Self::start_with_ipv4_answer([127, 0, 0, 1], Duration::ZERO)
    }

    #[cfg(windows)]
    fn start_for_ipv4_only(address: IpAddr) -> Self {
        let IpAddr::V4(address) = address else {
            panic!("IPv4-only mock DNS answer must be IPv4");
        };
        Self::start_with_ipv4_answer(address.octets(), Duration::from_millis(250))
    }

    fn start_with_ipv4_answer(ipv4_answer: [u8; 4], response_delay: Duration) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("bind mock DNS");
        let address = socket.local_addr().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let query_counts = Arc::new(Mutex::new(HashMap::new()));
        let worker_query_counts = Arc::clone(&query_counts);
        let worker = thread::spawn(move || {
            let mut packet = [0_u8; 2048];
            while !worker_stop.load(Ordering::Relaxed) {
                match socket.recv_from(&mut packet) {
                    Ok((size, peer)) => {
                        if let Some((_, query_type, domain)) = dns_question_details(&packet[..size])
                        {
                            *worker_query_counts
                                .lock()
                                .expect("mock DNS query-count lock poisoned")
                                .entry((domain, query_type))
                                .or_default() += 1;
                        }
                        if !response_delay.is_zero() {
                            thread::sleep(response_delay);
                        }
                        if let Some(response) =
                            mock_dns_response_with_ipv4(&packet[..size], ipv4_answer)
                        {
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
            query_counts,
            worker: Some(worker),
        }
    }

    #[cfg(windows)]
    fn query_count(&self, domain: &str, query_type: u16) -> usize {
        self.query_counts
            .lock()
            .expect("mock DNS query-count lock poisoned")
            .get(&(domain.to_owned(), query_type))
            .copied()
            .unwrap_or(0)
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
    mock_dns_response_with_ipv4(query, [127, 0, 0, 1])
}

fn mock_dns_response_with_ipv4(query: &[u8], ipv4_answer: [u8; 4]) -> Option<Vec<u8>> {
    let (question_end, qtype, domain) = dns_question_details(query)?;
    if qtype == 1 && domain == ONLY_AAAA_E2E_DOMAIN {
        let mut response = Vec::with_capacity(question_end);
        response.extend_from_slice(&query[..2]);
        response.extend_from_slice(&[0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0]);
        response.extend_from_slice(&query[12..question_end]);
        return Some(response);
    }
    let (record, data): (u16, &[u8]) = match qtype {
        28 => (
            28,
            &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        ),
        _ => (1, &ipv4_answer),
    };
    let mut response = Vec::with_capacity(question_end + 32);
    response.extend_from_slice(&query[..2]);
    response.extend_from_slice(&[0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0]);
    response.extend_from_slice(&query[12..question_end]);
    response.extend_from_slice(&[0xc0, 0x0c]);
    response.extend_from_slice(&record.to_be_bytes());
    response.extend_from_slice(&[0, 1, 0, 0, 0, 60]);
    response.extend_from_slice(&(data.len() as u16).to_be_bytes());
    response.extend_from_slice(data);
    Some(response)
}

fn dns_question(query: &[u8]) -> Option<(usize, u16)> {
    dns_question_details(query).map(|(end, query_type, _)| (end, query_type))
}

fn dns_question_details(query: &[u8]) -> Option<(usize, u16, String)> {
    if query.len() < 17 || query.get(4..6)? != [0, 1] {
        return None;
    }

    let mut offset = 12;
    let mut labels = Vec::new();
    loop {
        let label_length = usize::from(*query.get(offset)?);
        offset += 1;
        if label_length == 0 {
            break;
        }
        if label_length > 63 {
            return None;
        }
        let end = offset.checked_add(label_length)?;
        labels.push(std::str::from_utf8(query.get(offset..end)?).ok()?);
        offset = end;
    }

    let query_type = u16::from_be_bytes([*query.get(offset)?, *query.get(offset + 1)?]);
    let question_end = offset.checked_add(4)?;
    query.get(..question_end)?;
    Some((
        question_end,
        query_type,
        labels.join(".").to_ascii_lowercase(),
    ))
}
