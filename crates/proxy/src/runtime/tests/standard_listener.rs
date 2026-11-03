//! Test fixtures use the real installed source and original host receipts.
use super::Proxy;
use crate::runtime::inbound_owners::InboundOwner;
use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Duration, Instant},
};
pub(crate) struct Listener {
    pub address: SocketAddr,
    pub auxiliary: Vec<SocketAddr>,
    pub stop: watch::Sender<bool>,
    pub worker: JoinHandle<Result<(), zero_engine::EngineError>>,
    pub owner: Arc<InboundOwner>,
}
pub(crate) async fn start(proxy: &Proxy, inbound: zero_config::InboundConfig) -> Listener {
    start_with_handoff(proxy, inbound, || {}).await
}
pub(crate) async fn start_with_handoff(
    proxy: &Proxy,
    inbound: zero_config::InboundConfig,
    handoff: impl FnOnce(),
) -> Listener {
    start_inner(proxy, inbound, handoff, |_| {}).await
}
pub(crate) async fn start_with_original(
    proxy: &Proxy,
    inbound: zero_config::InboundConfig,
    handoff: impl FnOnce(&crate::runtime::inbound_owners::NativeListenerView),
) -> Listener {
    start_inner(proxy, inbound, || {}, handoff).await
}
async fn start_inner(
    proxy: &Proxy,
    inbound: zero_config::InboundConfig,
    before_bind: impl FnOnce(),
    after_bind: impl FnOnce(&crate::runtime::inbound_owners::NativeListenerView),
) -> Listener {
    #[cfg(feature = "shadowsocks")]
    let mut inbound = inbound;
    #[cfg(feature = "shadowsocks")]
    let paired = if inbound.protocol.protocol_name() == "shadowsocks" && inbound.listen.port == 0 {
        let paired = crate::socket_fixture::tcp_udp();
        inbound.listen.port = paired.0.local_addr().unwrap().port();
        Some(paired)
    } else {
        None
    };
    let config = proxy.engine().config();
    let tag = inbound.tag.clone();
    let protocol = inbound.protocol.protocol_name();
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(inbound, config.source_dir())
        .unwrap()
        .expect("fixture requires an actual standard source");
    let (stop, _) = watch::channel(false);
    let mut lease = proxy
        .inbound_owners
        .reserve_listener(
            &tag,
            &config,
            &proxy.protocols,
            prepared,
            stop.clone(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.resolve_native().await.unwrap();
    before_bind(); // release the actual test reservations immediately before native ownership
    #[cfg(feature = "shadowsocks")]
    drop(paired);
    lease.bind_native().unwrap();
    let (definition, view, installed) = lease.finish().unwrap().promote_definition().unwrap();
    after_bind(&view);
    let auxiliary = match &view {
        crate::runtime::inbound_owners::NativeListenerView::Metadata(views) => views
            .tcp
            .iter()
            .map(|(_, tcp)| tcp.local_addr().unwrap())
            .collect(),
        _ => Vec::new(),
    };
    let address = match &view {
        crate::runtime::inbound_owners::NativeListenerView::Handoff { grant, .. } => {
            Some(grant.endpoints.get().unwrap().external)
        }
        crate::runtime::inbound_owners::NativeListenerView::Tcp(socket) => {
            Some(socket.local_addr().unwrap())
        }
        #[cfg(feature = "datagram-route-runtime")]
        crate::runtime::inbound_owners::NativeListenerView::Datagram(socket) => {
            Some(socket.rx.local_addr().unwrap())
        }
        _ => None,
    };
    let owner = installed.owner();
    let recipe = definition;
    let factory = crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(
        crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
            proxy.tcp_runtime_services(),
        ),
        proxy.inbound_owners.clone(),
    );
    let upstream = factory
        .for_inbound(tag.clone())
        .route_factory()
        .for_connection(None)
        .upstream_services();
    let (source, peers) = installed
        .build_inbound(recipe, view, Some(upstream))
        .unwrap();
    let carrier = peers.as_ref().and_then(|peers| peers.carrier.clone());
    let runtime = factory
        .for_inbound(tag)
        .with_inbound_instance(owner.identity())
        .with_readiness(owner.readiness())
        .with_datagram_source(peers);
    owner
        .spawn_driver(crate::runtime::inbound_operation::source::run(
            source,
            runtime,
            stop.subscribe(),
            NonZeroUsize::new(4096).unwrap(),
            protocol,
        ))
        .unwrap();
    owner.seal_listener_installation();
    let observed = owner.clone();
    let worker = tokio::spawn(async move { observed.observe_listener().await });
    tokio::time::timeout(Duration::from_secs(5), owner.wait_ready())
        .await
        .unwrap()
        .unwrap();
    let address = address
        .or_else(|| carrier.as_ref().and_then(|local| local.get().copied()))
        .expect("actual source published its original endpoint");
    Listener {
        address,
        auxiliary,
        stop,
        worker,
        owner,
    }
}
pub(crate) async fn close(proxy: &Proxy, tag: &str, listener: Listener) {
    listener.owner.task_access().request_stop();
    listener.stop.send_replace(true);
    listener.worker.await.unwrap().unwrap();
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown(tag, Instant::now() + Duration::from_secs(3))
        .await
        .unwrap()
        .unwrap();
    assert!(
        receipt.terminal().failure().is_none(),
        "{:?}",
        receipt.terminal().failure()
    );
    assert!(receipt.terminal().joined_tasks() > 0);
    assert!(receipt.terminal().closed_io_receipts() > 0);
    assert!(proxy.inbound_owners.open_bind_admission().is_err());
    proxy
        .inbound_owners
        .release_closed(receipt, |_| Ok(()))
        .unwrap();
}
