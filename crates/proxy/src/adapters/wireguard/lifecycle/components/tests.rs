use super::*;
use crate::component::catalog::ComponentCatalog;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::{future::Future, task::Poll};

fn configuration() -> zero_config::RuntimeConfig {
    let peer = STANDARD.encode(PublicKey::from(&StaticSecret::from([2; 32])).as_bytes());
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"wg-in","listen":{"address":"127.0.0.1","port":51820},
            "protocol":{"type":"wireguard","private_key":STANDARD.encode([1;32]),
            "addresses":["10.0.0.1/24"],"mtu":1420,
            "peers":[{"public_key":peer,"allowed_ips":["0.0.0.0/0"]}]}}],
        "outbounds":[{"tag":"wg-out","protocol":{"type":"wireguard","private_key":STANDARD.encode([1;32]),
            "inbound_tag":"wg-in","addresses":["10.0.0.1/24"],"mtu":1420,
            "peers":[{"public_key":peer,"endpoint":"127.0.0.1:51821","allowed_ips":["0.0.0.0/0"]}]}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn domain_peer_declares_the_actual_node_dns_resource_before_any_stack_start() {
    let config = Arc::new(
        zero_config::RuntimeConfig::parse(
            &serde_json::to_string(&configuration())
                .unwrap()
                .replace("127.0.0.1:51821", "peer.example:51821"),
        )
        .unwrap(),
    );
    let network = Arc::new(crate::component::background::BackgroundNetwork::new(
        zero_platform_tokio::EgressInterfaceControl::default(),
    ));
    let (_, factories, _) =
        crate::register::compiled_registries_with_background(Some(network.clone()));
    let adapter = WireguardAdapter::default();
    let catalog = ComponentCatalog::with_factories(factories);
    let mut actual = None;
    let plan = catalog
        .prepare_with_device_resources(config.clone(), |resources| {
            let ids = adapter
                .prepare_component_resources(
                    &config,
                    resources,
                    Some(network.egress.generation()),
                    Some(network.clone()),
                )
                .map_err(|source| {
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::Io,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source)
                })?;
            assert_eq!(ids.len(), 1);
            let dns = crate::component::native_dns::prepare_node(resources, network.clone())
                .map_err(|source| {
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::Io,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source)
                })?;
            // Fresh preparation offers no numeric substitute for an actual
            // healthy previous DNS owner; first use must create new Options.
            assert!(resources.reusable_resource_instance(dns).is_none());
            actual = Some((*ids.first().unwrap(), dns));
            Ok(ids)
        })
        .unwrap();
    let (device, dns) = actual.unwrap();
    assert_eq!(
        plan.device_component(device)
            .unwrap()
            .recipe
            .spec()
            .dependencies,
        vec![dns]
    );
    let pending = PendingEndpoints::staged(&adapter.pending_endpoints, &config).unwrap();
    assert!(pending
        .get("wg-in")
        .unwrap()
        .peer_stack(0)
        .unwrap()
        .device()
        .is_none());
    adapter.release_component_resources(&config).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn production_candidate_owns_cold_invocation_then_installs_only_its_started_scope_graph() {
    let proxy = crate::Proxy::new(configuration()).unwrap();
    let services = proxy.tcp_runtime_services();
    let config = services.snapshot().config().clone();
    let adapter = WireguardAdapter::default();
    let catalog = ComponentCatalog::with_factories(crate::register::component_registry());
    catalog
        .prepare_with_device_resources(config.clone(), |resources| {
            adapter
                .prepare_component_resources(
                    &config,
                    resources,
                    Some(services.upstream().egress_generation()),
                    None,
                )
                .map_err(|source| {
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::Io,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source)
                })
        })
        .unwrap();
    let pending = PendingEndpoints::staged(&adapter.pending_endpoints, &config).unwrap();
    assert!(matches!(
        pending.invocations.lock().await.get("wg-in").unwrap(),
        recipe::retained::RetainedInvocation::Prepared(_)
    ));
    assert!(pending
        .get("wg-in")
        .unwrap()
        .peer_stack(0)
        .unwrap()
        .device()
        .is_none());
    catalog.start(config.clone(), deadline()).await.unwrap();
    adapter
        .resolve_component_resources(
            &config,
            &services.upstream(),
            services.engine(),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(matches!(
        pending.invocations.lock().await.get("wg-in").unwrap(),
        recipe::retained::RetainedInvocation::Resolved(_)
    ));
    let traffic_count = pending.traffic.lock().unwrap().len();
    // Resuming a pass after another device suspended must keep this original
    // resolution, receiver and meter, rather than resolving a fresh invocation.
    adapter
        .resolve_component_resources(
            &config,
            &services.upstream(),
            services.engine(),
            deadline(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(pending.traffic.lock().unwrap().len(), traffic_count);
    assert!(pending.get("wg-in").unwrap().ready().is_none());
    catalog
        .start_device_components(config.clone(), deadline())
        .await
        .unwrap();
    let prepared = adapter
        .prepare_devices_for_candidate(
            &config,
            &config.outbounds.iter().collect::<Vec<_>>(),
            &config.inbounds,
            super::super::tests::context(&proxy, &config),
        )
        .await
        .unwrap();
    let link = pending.get("wg-in").unwrap().ready().unwrap();
    let device = link.state.lock().unwrap().devices[0].clone();
    prepared.publish().await;
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
    assert!(Arc::ptr_eq(
        &link,
        adapter
            .linked_endpoints
            .lock()
            .unwrap()
            .get("wg-in")
            .unwrap()
    ));
    let (sender, mut packets) = tokio::sync::mpsc::channel(4);
    drop(link.sender.send_replace(sender));
    let mut connecting = Box::pin(device.open_tcp_observed(
        "10.0.0.1".parse().unwrap(),
        "192.0.2.2:443".parse().unwrap(),
        None,
    ));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let syn = tokio::time::timeout_at(deadline(), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(zero_stack::packet::parse_tcp(&syn.packet).unwrap().syn);
    let witnesses = pending
        .get("wg-in")
        .unwrap()
        .peer_stack(0)
        .unwrap()
        .test_original_witnesses();
    catalog.commit(&config, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
    assert!(witnesses.iter().all(|witness| witness.is_joined()));
    assert!(connecting.await.is_err());
    adapter.close_devices().await.unwrap();
}

#[tokio::test]
async fn ordinary_device_preparation_cannot_fall_back_to_private_plain_linked_workers() {
    let proxy = crate::Proxy::new(configuration()).unwrap();
    let services = proxy.tcp_runtime_services();
    let config = services.snapshot().config().clone();
    let adapter = WireguardAdapter::default();
    let result = adapter
        .prepare_devices_for_candidate(
            &config,
            &config.outbounds.iter().collect::<Vec<_>>(),
            &config.inbounds,
            super::super::tests::context(&proxy, &config),
        )
        .await;
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("actual cold component candidate"));
    assert!(adapter.pending_endpoints.lock().unwrap().is_none());
    assert!(adapter.linked_endpoints.lock().unwrap().is_empty());
}
