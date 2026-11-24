//! A real outer Source borrows the existing authenticated HY2 pool and Root.
//! No legacy HY2 projection, synthetic task or close-by-port probe is used.
use super::*;
use zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin;

fn physical(proxy: &crate::Proxy) -> MetadataPoolOrigin {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let role = plan.configured("hy").unwrap();
    let originals = role
        .dependency_instances()
        .iter()
        .flat_map(|id| {
            let owner = plan.component(id.component).unwrap();
            assert_eq!(owner.instance_id(), *id);
            (0..owner.recipe.spec().resources.len())
                .filter_map(|ordinal| {
                    owner
                        .original_metadata_owner(ordinal)
                        .and_then(|owner| owner.current_original())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        originals.len(),
        1,
        "one actual HY2 native authentication, not a second pool"
    );
    originals.into_iter().next().unwrap()
}

async fn wire(peer: &tokio::net::UdpSocket) {
    let mut bytes = [0; 2048];
    let (size, _) = tokio::time::timeout(Duration::from_secs(10), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(size, 148);
    assert_eq!(
        &bytes[..4],
        &[1, 0, 0, 0],
        "actual WG initiation over the selected HY2 Datagram role"
    );
}

#[tokio::test]
async fn linked_and_standalone_hy2_outer_use_same_pool_then_close_originals_before_replacement() {
    let material = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = material.path().join("cert.pem");
    let key = material.path().join("key.pem");
    std::fs::write(&certificate, cert.cert.pem()).unwrap();
    std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
    let remote_config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},
            "protocol":{"type":"hysteria2","password":"actual-secret","cert_path":certificate,"key_path":key}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let remote = crate::Proxy::new(remote_config.clone()).unwrap();
    let remote_running = remote.spawn();
    handle(&remote)
        .apply_runtime_config_and_wait(remote_config, Duration::from_secs(10))
        .await
        .unwrap();
    let remote_owner = remote.system_tun_source_registry().owner("remote").unwrap();
    remote_owner.wait_ready().await.unwrap();
    let endpoint = *remote_owner.original_listener_address.get().unwrap();
    for (linked, hopping) in [(false, false), (true, false), (false, true), (true, true)] {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut candidate = active_outer_config(peer.local_addr().unwrap(), linked);
        candidate["outbounds"].as_array_mut().unwrap().push(serde_json::json!({"tag":"hy","protocol":{
            "type":"hysteria2","server":"127.0.0.1","port":endpoint.port(),"server_name":"localhost",
            "password":"actual-secret","insecure":true
        }}));
        if hopping {
            candidate["outbounds"][1]["protocol"]["transport"] = serde_json::json!({
                "udp_hop":{"ports":[endpoint.port()],"interval_min_secs":5,"interval_max_secs":5},
                "quic":{"keep_alive_interval_secs":2}
            });
        }
        // The actual single HY2 provider is the target; a one-member relay
        // is invalid configuration and adds no carrier to this production path.
        candidate["outbounds"][0]["protocol"]["outer_udp_proxy"] = "hy".into();
        let candidate = RuntimeConfig::parse(&candidate.to_string()).unwrap();
        let proxy = crate::Proxy::new(candidate.clone()).unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(candidate.clone(), Duration::from_secs(10))
            .await
            .unwrap();
        wire(&peer).await;
        let original = physical(&proxy);
        let receiver = original
            .retained_protocol_receiver()
            .expect("same original native packet/endpoint driver handoff");
        assert!(!receiver.protocol_driver().is_joined());
        let old = devices(&proxy).remove(0);
        let old_workers = worker_census(&old);
        let old_source = originals(&proxy).remove(0);
        handle(&proxy)
            .apply_runtime_config_and_wait(candidate.clone(), Duration::from_secs(10))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&old, &devices(&proxy)[0]));
        assert!(original.same_original(&physical(&proxy)));
        assert!(original.closed().is_none());

        if hopping {
            let first_driver = original.retained_driver().unwrap();
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    if original.native_wire_closed().is_some()
                        && original.retained_driver().is_some_and(|current| {
                            !current
                                .packet_driver()
                                .same_registration(first_driver.packet_driver())
                        })
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            let native = original.native_wire_closed().unwrap();
            assert!(
                native.matches(&original)
                    && native.had_native_acquisition()
                    && native.is_complete()
            );
            assert!(first_driver
                .packet_driver()
                .try_joined()
                .unwrap()
                .registration_released());
            assert!(!first_driver.protocol_driver().is_joined());
            assert!(
                original.closed().is_none(),
                "actual same protocol/root remains live across native wire rotation"
            );
        }
        let mut replacement = candidate;
        replacement.runtime.hysteria2_max_entries = Some(2);
        handle(&proxy)
            .apply_runtime_config_and_wait(replacement, Duration::from_secs(10))
            .await
            .unwrap();
        joined(&old, &old_workers);
        assert!(old_source.is_retired());
        assert!(!old_source.same_original(&originals(&proxy)[0]));
        if let Some(logical) = old_source.logical_packet() {
            for driver in logical.drivers() {
                let exit = driver
                    .try_joined()
                    .expect("original software Source driver joined");
                assert!(driver.matches_joined(&exit) && exit.registration_released());
                exit.result().unwrap();
            }
            if linked {
                let native = old_source
                    .packet_source_admission()
                    .expect("linked Source retained its original UDP listener");
                assert!(native.closed().unwrap().matches(native.owner()));
            }
        } else {
            assert!(linked && old_source.closed().unwrap().matches(&old_source));
        }
        let closed = original
            .closed()
            .expect("actual old HY2 raw native original before replacement ACK");
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        let driver = receiver.protocol_driver();
        let joined = driver
            .try_joined()
            .expect("actual old Quinn endpoint task joined");
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        let fresh = physical(&proxy);
        assert!(!fresh.same_original(&original));
        assert!(fresh.closed().is_none());
        wire(&peer).await;
        running.shutdown().await.unwrap();
        let closed = fresh.closed().expect("actual final HY2 original closed");
        assert!(closed.matches(&fresh) && closed.had_native_acquisition());
    }
    remote_running.shutdown().await.unwrap();
    let terminal = remote_owner.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none() && terminal.closed_io_receipts() > 0);
}
