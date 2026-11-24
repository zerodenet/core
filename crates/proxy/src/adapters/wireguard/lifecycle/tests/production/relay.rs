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
struct Original {
    slot: zero_platform_tokio::controlled_carrier::CarrierOriginObservation,
    native: bool,
    donor: Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>,
}

fn carriers(proxy: &crate::Proxy) -> Vec<Original> {
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
                        .map(|slot| Original {
                            slot: slot.original().clone(),
                            native: slot.native().is_some(),
                            donor: slot.borrowed_origin().cloned(),
                        })
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
        let mut candidate = active_outer_config(peer.local_addr().unwrap(), linked);
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
        let originals = carriers(&proxy);
        assert!(originals.iter().any(|original| original.native));
        assert!(originals.iter().any(|original| original.donor.is_some()));
        assert!(originals
            .iter()
            .all(|original| original.slot.closed().is_none()));
        let old = devices(&proxy).remove(0);
        let old_workers = worker_census(&old);
        handle(&proxy)
            .apply_runtime_config_and_wait(changed(candidate), Duration::from_secs(10))
            .await
            .unwrap();
        joined(&old, &old_workers);
        for original in originals {
            let closed = original
                .slot
                .closed()
                .expect("same original before replacement ACK");
            assert!(original.slot.matches_closed(&closed));
            assert_eq!(closed.had_native_acquisition(), original.native);
            if let Some(donor) = &original.donor {
                let receipt = donor.closed().expect("exact lower carrier retired");
                assert!(receipt.matches(donor));
            } else {
                assert!(closed.had_native_acquisition());
            }
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
