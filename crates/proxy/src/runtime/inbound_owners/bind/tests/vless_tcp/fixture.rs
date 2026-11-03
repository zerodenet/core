use super::*;
use crate::runtime::inbound_owners::InboundOwner;
pub(super) async fn proxy(protocol: serde_json::Value) -> Proxy {
    let reverse = protocol["users"].as_array().is_some_and(|users| {
        users
            .iter()
            .any(|user| user["reverse_tag"].as_str().is_some())
    });
    let mut document = serde_json::json!({
        "inbounds": [{"tag": "first", "listen": {"address": "127.0.0.1", "port": 0}, "protocol": protocol}],
        "route": {"final": {"type": "direct"}}
    });
    if reverse {
        document["outbounds"] =
            serde_json::json!([{"tag": "portal", "protocol": {"type": "vless_reverse"}}]);
    }
    let config = RuntimeConfig::parse(&document.to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let config = proxy.engine().config();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    proxy
}
pub(super) fn install(
    proxy: &Proxy,
) -> (Arc<InboundOwner>, watch::Sender<bool>, std::net::SocketAddr) {
    let config = proxy.engine().config();
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(config.inbounds[0].clone(), None)
        .unwrap()
        .expect("selected standard production registration");
    let (stop, _) = watch::channel(false);
    let mut lease = proxy
        .inbound_owners
        .reserve_listener(
            "first",
            &config,
            &proxy.protocols,
            prepared,
            stop.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.bind_tcp().unwrap();
    let (definition, listener, installed) = promote_tcp(lease.finish().unwrap());
    let address = listener.local_addr().unwrap();
    let owner = installed.owner();
    let recipe = definition;
    let factory = crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(
        crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
            proxy.tcp_runtime_services(),
        ),
        proxy.inbound_owners.clone(),
    );
    let upstream = factory
        .for_inbound("first".into())
        .route_factory()
        .for_connection(None)
        .upstream_services();
    let (source, peers) = installed
        .build_inbound(recipe, NativeListenerView::Tcp(listener), Some(upstream))
        .unwrap();
    let runtime = factory
        .for_inbound("first".into())
        .with_inbound_instance(owner.identity())
        .with_readiness(owner.readiness())
        .with_datagram_source(peers);
    owner
        .spawn_driver(crate::runtime::inbound_operation::source::run(
            source,
            runtime,
            stop.subscribe(),
            NonZeroUsize::new(4096).unwrap(),
            "vless",
        ))
        .unwrap();
    owner.seal_listener_installation();
    (owner, stop, address)
}
pub(super) async fn retire(proxy: &Proxy, minimum_tasks: usize, minimum_io: usize) {
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("first", Instant::now() + Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert!(
        receipt.terminal().failure().is_none(),
        "{:?}",
        receipt.terminal().failure()
    );
    assert!(
        receipt.terminal().joined_tasks() >= minimum_tasks,
        "actual task joins: {}",
        receipt.terminal().joined_tasks()
    );
    assert!(
        receipt.terminal().closed_io_receipts() >= minimum_io,
        "original receipts: {}",
        receipt.terminal().closed_io_receipts()
    );
    assert!(
        proxy.inbound_owners.open_bind_admission().is_err(),
        "receipt still outstanding"
    );
    proxy
        .inbound_owners
        .release_closed(receipt, |_| Ok(()))
        .unwrap();
}
