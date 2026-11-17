//! Existing Stream-only donor, Associated root and pure codec, on actual Hosts.
use super::*;
const ID: &str = "51515151-5151-5151-5151-515151515151";

async fn remote(protocol: serde_json::Value) -> (crate::Proxy, crate::RunningProxy, SocketAddr) {
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = crate::Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    handle(&proxy)
        .apply_runtime_config_and_wait(config, Duration::from_secs(10))
        .await
        .unwrap();
    let source = proxy.system_tun_source_registry().owner("remote").unwrap();
    source.wait_ready().await.unwrap();
    let endpoint = *source.original_listener_address.get().unwrap();
    (proxy, running, endpoint)
}
fn physical(
    proxy: &crate::Proxy,
) -> Vec<zero_platform_tokio::controlled_carrier::CarrierOriginObservation> {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    ["donor", "receiver", "codec"]
        .into_iter()
        .flat_map(|tag| {
            let owner = plan.configured(tag).unwrap();
            (0..owner.recipe.spec().resources.len())
                .filter_map(|ordinal| owner.original_carrier_owner(ordinal))
                .flat_map(|owner| {
                    owner
                        .observe()
                        .unwrap()
                        .slots()
                        .iter()
                        .map(|slot| slot.original().clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn stream_only_outer_donor_and_codec_tail_deliver_wire_and_retire_originals_before_replacement(
) {
    let (vmess, vmess_running, vmess_endpoint) = remote(serde_json::json!({
        "type":"vmess","users":[{"id":ID,"cipher":"aes-128-gcm"}]
    }))
    .await;
    let (vless, vless_running, vless_endpoint) = remote(serde_json::json!({
        "type":"vless","users":[{"id":ID}]
    }))
    .await;
    let (ss, ss_running, ss_endpoint) = remote(serde_json::json!({
        "type":"shadowsocks","cipher":"aes-128-gcm","password":"same-wire-secret"
    }))
    .await;
    for linked in [false, true] {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut candidate =
            serde_json::to_value(config(peer.local_addr().unwrap(), None, linked)).unwrap();
        candidate["outbounds"].as_array_mut().unwrap().extend([
            serde_json::json!({"tag":"donor","protocol":{"type":"vmess","server":"127.0.0.1","port":vmess_endpoint.port(),"id":ID,"cipher":"aes-128-gcm"}}),
            serde_json::json!({"tag":"receiver","protocol":{"type":"vless","server":"127.0.0.1","port":vless_endpoint.port(),"id":ID}}),
            serde_json::json!({"tag":"codec","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":ss_endpoint.port(),"cipher":"aes-128-gcm","password":"same-wire-secret"}}),
        ]);
        candidate["outbounds"][0]["protocol"]["outer_udp_proxy"] = serde_json::json!("outer-relay");
        candidate["outbound_groups"] = serde_json::json!([{"tag":"outer-relay","type":"relay","proxies":["donor","receiver","codec"]}]);
        let candidate = RuntimeConfig::parse(&candidate.to_string()).unwrap();
        let proxy = crate::Proxy::new(candidate.clone()).unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(candidate.clone(), Duration::from_secs(10))
            .await
            .unwrap();
        let mut packet = [0; 2048];
        let (size, _) = tokio::time::timeout(Duration::from_secs(10), peer.recv_from(&mut packet))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&packet[..4], &[1, 0, 0, 0]);
        assert_eq!(
            size, 148,
            "real WireGuard initiation through all three actual protocols"
        );
        let originals = physical(&proxy);
        assert!(!originals.is_empty());
        assert!(originals.iter().all(|origin| origin.closed().is_none()));
        let old = devices(&proxy).remove(0);
        handle(&proxy)
            .apply_runtime_config_and_wait(changed(candidate), Duration::from_secs(10))
            .await
            .unwrap();
        joined(&old);
        for original in originals {
            let closed = original
                .closed()
                .expect("same original before replacement ACK");
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        }
        running.shutdown().await.unwrap();
    }
    for (proxy, running) in [
        (vmess, vmess_running),
        (vless, vless_running),
        (ss, ss_running),
    ] {
        let source = proxy.system_tun_source_registry().owner("remote").unwrap();
        running.shutdown().await.unwrap();
        let terminal = source.terminal_for_test().unwrap();
        assert!(terminal.failure().is_none() && terminal.closed_io_receipts() > 0);
    }
}
