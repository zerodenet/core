use super::*;
use crate::runtime::{inbound_owners::InboundOwner, Proxy};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
pub(crate) fn config(family: &str) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"actual-direct","protocol":{"type":"direct"},"dial":{"address_family":family}}],
        "inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"udp":{"enabled":true},
            "protocol":{"type":"direct","target":"127.0.0.1","port":9}}],
        "route":{"final":{"type":"route","outbound":"actual-direct"}}
    }).to_string()).unwrap()
}
pub(crate) struct Live {
    pub(crate) proxy: Proxy,
    pub(crate) runtime: UdpIngressRuntime,
    pub(crate) context: PeerResourceContext,
    pub(crate) session: Session,
    pub(crate) operation: DirectUdpFlowOperation,
    pub(crate) driver_slot: crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot,
    owner: Arc<InboundOwner>,
    monitor: tokio::task::JoinHandle<Result<(), zero_engine::EngineError>>,
    _read_peer: TcpStream,
    handle: zero_engine::SessionHandle,
}
impl Live {
    pub(crate) async fn new() -> Self {
        let proxy = Proxy::new(config("auto")).unwrap();
        proxy
            .protocols
            .start_components(
                proxy.engine().config(),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&proxy.engine().config(), || Ok(()))
            .unwrap();
        let tcp = proxy.tcp_runtime_services().for_inbound("source".into());
        let ingress = tcp.execution().ingress.unwrap().role;
        let (stop, mut stopping) = tokio::sync::watch::channel(false);
        let installed = proxy
            .inbound_owners
            .install(
                "source",
                &proxy.protocols,
                crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
                stop,
            )
            .unwrap();
        let owner = installed.owner();
        let group = installed.peer_tasks();
        let host = installed.tasks();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let read_peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut read, _) = listener.accept().await.unwrap();
        let (binding_tx, binding_rx) = tokio::sync::oneshot::channel();
        let pending = Arc::new(tokio::sync::Notify::new());
        let (driver_slot, driver_job) =
            crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot::channel();
        owner.spawn_driver({let group=group.clone();let pending=pending.clone();async move {
            assert!(group.spawn_bound(&host, move |binding| {
                binding_tx.send(binding.clone()).unwrap();
                async move {
                    let cancel = binding.cancellation();
                    crate::runtime::udp_socket::endpoint::peer::job::run_peer_with_driver(async move {
                    let read = read.read_u8(); tokio::pin!(read);
                    tokio::select! {
                        _=cancel.cancelled()=>Ok(()),
                        result=std::future::poll_fn(|cx| {
                            let result=std::future::Future::poll(read.as_mut(),cx);
                            if result.is_pending(){pending.notify_one();} result
                        })=>Err(std::io::Error::other(format!("fixture actual read unexpectedly completed: {result:?}")).into()),
                    }
                    }, driver_job).await
                }
            })?);
            let _=stopping.wait_for(|stop|*stop).await;
            group.wait_closed().await
        }}).unwrap();
        let monitor = {
            let owner = owner.clone();
            tokio::spawn(async move { owner.observe_driver().await })
        };
        let binding = binding_rx.await.unwrap();
        pending.notified().await;
        let context = group.resource_context(binding, ingress);
        let runtime = UdpIngressRuntime::new(tcp)
            .with_peer_resources(context.clone())
            .with_direct_peer_driver(driver_slot.clone());
        let mut session = Session::new(
            0,
            zero_core::Address::Ipv4([127, 0, 0, 1]),
            9,
            Network::Udp,
            zero_core::ProtocolType::new("direct"),
        );
        runtime
            .prepare_udp_session(&mut session, "source")
            .await
            .unwrap();
        let handle = runtime.track_session(session.id);
        let trace = runtime.route_trace(&session).await;
        let (resolved, _) = runtime.resolve_outbound(&trace.decision, &session).unwrap();
        let zero_engine::ResolvedOutbound::Single(zero_engine::ResolvedLeafOutbound::Direct {
            tag,
            dial_policy,
            dial_generation,
        }) = resolved
        else {
            panic!("actual resolved Direct leaf");
        };
        let operation = DirectUdpFlowOperation {
            tag: tag.unwrap_or("direct").into(),
            policy_tag: tag.map(str::to_owned),
            dial_policy,
            dial_generation,
        };
        Self {
            proxy,
            runtime,
            context,
            session,
            operation,
            driver_slot,
            owner,
            monitor,
            _read_peer: read_peer,
            handle,
        }
    }
    pub(crate) async fn source(&self) -> DirectSocketSource {
        self.operation
            .socket_source(
                self.proxy.current_service_source(),
                &self.runtime,
                &self.session,
                &self.context,
            )
            .await
            .unwrap()
    }
    pub(crate) async fn finish(mut self) -> Result<(), zero_engine::EngineError> {
        self.handle.finish(zero_engine::SessionOutcome::Cancelled);
        self.owner.stop();
        // A monitor error is a task fact, never a substitute for native cleanup.
        let monitored = self
            .monitor
            .await
            .map_err(|source| zero_engine::EngineError::Io(std::io::Error::other(source)))
            .and_then(|result| result);
        let Some(receipt) = self
            .proxy
            .inbound_owners
            .confirm_shutdown("source", Instant::now() + Duration::from_secs(2))
            .await?
        else {
            return monitored;
        };
        let fault = self
            .proxy
            .inbound_owners
            .release_closed(receipt, |terminal| Ok(terminal.failure()))?;
        if let Some(source) = fault {
            return Err(std::io::Error::other(crate::component::instance::retained(source)).into());
        }
        monitored
    }
    pub(crate) async fn close(self) {
        self.finish().await.unwrap();
    }
}
