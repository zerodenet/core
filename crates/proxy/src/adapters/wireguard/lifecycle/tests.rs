use super::*;
use zero_config::RuntimeConfig;

pub(super) fn context(
    proxy: &crate::runtime::Proxy,
    config: &RuntimeConfig,
) -> OutboundDevicePreparationContext {
    let services = proxy.tcp_runtime_services();
    OutboundDevicePreparationContext {
        traffic: proxy.engine().clone(),
        direct_packet_device: None,
        mtu: config.runtime.network.mtu,
        stack_options: Default::default(),
        endpoint_bindings: Arc::new(config.endpoint_bindings()),
        disabled_outbounds: Arc::new(Default::default()),
        upstream: services.upstream(),
        packet_path_services:
            crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(
                &services.execution(),
            ),
        packet_paths: Arc::new(Default::default()),
    }
}

#[tokio::test]
async fn standalone_replacement_confirms_original_native_close_and_retains_discarded_candidate() {
    let server =
        zero_platform_tokio::TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let endpoint = server.local_addr().unwrap();
    let config = RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"wg","protocol":{"type":"wireguard","private_key":"01".repeat(32),
            "addresses":["10.0.0.1/32"],"peers":[{"public_key":"02".repeat(32),"endpoint":endpoint.to_string(),"allowed_ips":["0.0.0.0/0"]}],"mtu":1420}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = crate::runtime::Proxy::new(config.clone()).unwrap();
    let owner = WireguardAdapter::default();
    let outbounds = config.outbounds.iter().collect::<Vec<_>>();
    owner
        .prepare_devices(&outbounds, &[], context(&proxy, &config))
        .await
        .unwrap()
        .publish()
        .await;
    let first = owner
        .claim_outbound_leaf_impl(crate::protocol_registry::OutboundLeafInput::Virtual {
            outbound: &config.outbounds[0],
        })
        .unwrap()
        .packet
        .unwrap();
    let second = owner
        .claim_outbound_leaf_impl(crate::protocol_registry::OutboundLeafInput::Virtual {
            outbound: &config.outbounds[0],
        })
        .unwrap()
        .packet
        .unwrap();
    assert!(Arc::ptr_eq(&first.native, &second.native));
    assert!(Arc::ptr_eq(
        first.translated.as_ref().unwrap(),
        second.translated.as_ref().unwrap()
    ));
    assert!(Arc::ptr_eq(
        first.datagram.as_ref().unwrap(),
        second.datagram.as_ref().unwrap()
    ));
    let mut bytes = [0; 2048];
    let (n, _) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        server.recv_from_addr(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        n > 0,
        "real protocol handshake reached the retained peer socket"
    );
    let identity = owner.profiles.lock().unwrap()["wg"].device_identity;
    let generation = proxy.tcp_runtime_services().upstream().egress_generation();
    let original = owner
        .pool
        .cell_for("wg", 0, identity, generation)
        .unwrap()
        .get()
        .unwrap()
        .clone();
    let mut borrowed = owner
        .prepare_devices(&outbounds, &[], context(&proxy, &config))
        .await
        .unwrap();
    borrowed.publish_sync();
    drop(borrowed); // A failed commit owns no cleanup right over this old socket.
    owner.close_discarded_devices().await.unwrap();
    assert!(original.is_usable());
    assert!(owner
        .pool
        .is_current("wg", 0, identity, generation, &original));
    let mut changed = config.clone();
    if let OutboundProtocolConfig::Wireguard { mtu, .. } = &mut changed.outbounds[0].protocol {
        *mtu = 1380;
    }
    let outbounds = changed.outbounds.iter().collect::<Vec<_>>();
    let mut candidate = owner
        .prepare_devices(&outbounds, &[], context(&proxy, &changed))
        .await
        .unwrap();
    assert!(original.is_closed());
    original.wait_closed().await.unwrap(); // Original worker + original native close, not a rebind probe.
    assert!(owner.pool.cell_for("wg", 0, identity, generation).is_err());
    candidate.publish_sync();
    let candidate_identity = owner.profiles.lock().unwrap()["wg"].device_identity;
    let rejected = owner
        .pool
        .cell_for("wg", 0, candidate_identity, generation)
        .unwrap()
        .get()
        .unwrap()
        .clone();
    drop(candidate);
    assert!(rejected.is_closed());
    assert!(owner
        .pool
        .cell_for("wg", 0, candidate_identity, generation)
        .is_err());
    owner.close_discarded_devices().await.unwrap();
    rejected.wait_closed().await.unwrap(); // same native original, not Drop
                                           // The next pass must drain that exact discarded original before rebuilding.
    let outbounds = config.outbounds.iter().collect::<Vec<_>>();
    owner
        .prepare_devices(&outbounds, &[], context(&proxy, &config))
        .await
        .unwrap()
        .publish()
        .await;
    let restored = owner
        .pool
        .cell_for("wg", 0, identity, generation)
        .unwrap()
        .get()
        .unwrap()
        .clone();
    assert!(!Arc::ptr_eq(&original, &restored));
    assert!(restored.is_usable());
    owner.close_devices().await.unwrap();
    restored.wait_closed().await.unwrap();
}
