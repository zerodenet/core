//! Real ordinary UDP Sources share the adapter's charged security histories.
use super::*;
use ::shadowsocks::udp::ShadowsocksDatagramCodec;
use tokio::net::UdpSocket;
use zero_traits::DatagramCodec;

async fn exchange(
    client: &UdpSocket,
    codec: &ShadowsocksDatagramCodec,
    endpoint: std::net::SocketAddr,
    target: &UdpSocket,
) {
    let packet = codec
        .encode(
            &zero_core::Address::Ipv4([127, 0, 0, 1]),
            target.local_addr().unwrap().port(),
            b"query",
        )
        .unwrap();
    client.send_to(&packet, endpoint).await.unwrap();
    let mut bytes = [0; 65536];
    let (size, sender) = target.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"query");
    target.send_to(b"answer", sender).await.unwrap();
    let (size, sender) = client.recv_from(&mut bytes).await.unwrap();
    assert_eq!(sender, endpoint);
    assert_eq!(codec.decode(&bytes[..size]).unwrap().2, b"answer");
}

#[tokio::test]
async fn actual_udp_sources_share_all_security_units_and_keep_retired_history_on_budget_shrink() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let reserved_a = crate::socket_fixture::tcp_udp();
        let reserved_b = crate::socket_fixture::tcp_udp();
        let endpoints = [reserved_a.0.local_addr().unwrap(), reserved_b.0.local_addr().unwrap()];
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{
                "ss_udp_replay_max_windows":2, "ss_udp_max_senders":2,
                "ss_udp_max_associations":2, "ss_udp_max_return_bindings":2,
                "ss_udp_max_authenticated_codecs":2
            },
            "inbounds":endpoints.iter().enumerate().map(|(index, endpoint)| serde_json::json!({
                "tag":format!("udp-{index}"), "listen":{"address":"127.0.0.1","port":endpoint.port()},
                "protocol":{"type":"shadowsocks","cipher":METHOD,"password":PASSWORD,
                    "state_limits":{"udp_timeout_secs":1}}
            })).collect::<Vec<_>>(),
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        let registry = proxy.system_tun_source_registry();
        let mut phase = Phase::new("two actual UDP Sources", &proxy);
        phase.set("await both original Source registries and readiness");
        drop(reserved_a); drop(reserved_b);
        let running = proxy.spawn();
        let mut originals = Vec::new();
        for tag in ["udp-0", "udp-1"] {
            let owner = loop {
                if let Some(owner) = registry.owner(tag) { break owner; }
                tokio::task::yield_now().await;
            };
            phase.watch(owner.clone());
            owner.wait_ready().await.unwrap(); originals.push(owner);
        }
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let clients = [UdpSocket::bind("127.0.0.1:0").await.unwrap(), UdpSocket::bind("127.0.0.1:0").await.unwrap()];
        let codecs = [ShadowsocksDatagramCodec::new(::shadowsocks::CipherKind::Blake3Aes128Gcm, PASSWORD), ShadowsocksDatagramCodec::new(::shadowsocks::CipherKind::Blake3Aes128Gcm, PASSWORD)];
        phase.set("both original Sources: actual encrypted UDP exchange");
        for index in 0..2 { exchange(&clients[index], &codecs[index], endpoints[index], &target).await; }
        for original in &originals { phase.watch_peers(original.peer_tasks_for_test()); }
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        // Four different real units, across distinct Source profiles. Source
        // native/worker resources and return-target bindings are separate units.
        phase.set("two retained actual units: cold shrink rejection for each unit");
        for unit in 0..4 {
            let mut candidate = (*proxy.engine().config()).clone();
            match unit {
                0 => candidate.runtime.ss_udp_replay_max_windows = Some(1),
                1 => candidate.runtime.ss_udp_max_senders = Some(1),
                2 => candidate.runtime.ss_udp_max_associations = Some(1),
                _ => candidate.runtime.ss_udp_max_authenticated_codecs = Some(1),
            }
            assert!(handle.apply_runtime_config_and_wait(candidate, Duration::from_secs(5)).await.is_err(), "two real retained UDP units cannot shrink to one");
            for (index, original) in originals.iter().enumerate() {
                assert!(Arc::ptr_eq(original, &registry.owner(&format!("udp-{index}")).unwrap()));
                assert!(original.is_running());
            }
        }
        phase.set("live original UDP users: cold growth rejection");
        let mut grow = (*proxy.engine().config()).clone();
        grow.runtime.ss_udp_replay_max_windows = Some(3);
        assert!(handle.apply_runtime_config_and_wait(grow.clone(), Duration::from_secs(5)).await.is_err(), "growing a shared budget cannot revoke an admitted original UDP user");
        phase.set("Busy rejected: repeat real UDP exchange on both originals");
        for index in 0..2 { exchange(&clients[index], &codecs[index], endpoints[index], &target).await; }
        phase.set("original UDP clients dropped: await actual Source main cleanup idle");
        drop(clients); drop(target);
        loop {
            if originals.iter().all(|original| original.acquisition_idle_for_test()) { break; }
            tokio::task::yield_now().await;
        }
        phase.set("idle UDP growth: await StopOld and replacement ACK");
        handle.apply_runtime_config_and_wait(grow, Duration::from_secs(5)).await.unwrap();
        for original in &originals {
            let closed = original.terminal_for_test().expect("old actual Source retired before budget ACK");
            assert!(closed.failure().is_none());
            assert!(closed.closed_io_receipts() >= 2, "same actual TCP and UDP listeners closed");
            assert!(closed.joined_tasks() >= 2, "Source and its actual replay Scope task joined");
        }
        phase.set("UDP growth ACK: matching original Scope/native receipts");
        let current = registry.owner("udp-0").unwrap();
        let mut remove = (*proxy.engine().config()).clone();
        remove.inbounds.clear(); remove.runtime.ss_udp_replay_max_windows = Some(1);
        assert!(handle.apply_runtime_config_and_wait(remove, Duration::from_secs(5)).await.is_err(), "native retirement cannot erase two valid original 61-second replay histories");
        assert!(Arc::ptr_eq(&current, &registry.owner("udp-0").unwrap()));
        let current = [registry.owner("udp-0").unwrap(), registry.owner("udp-1").unwrap()];
        phase.set("final UDP originals shutdown");
        running.shutdown().await.unwrap();
        for owner in current {
            let closed = owner.terminal_for_test().unwrap();
            assert!(closed.failure().is_none()); assert!(closed.closed_io_receipts() >= 2);
        }
    }).await.unwrap();
}
