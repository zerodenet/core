//! The real PacketTcpOperation transfers provenance from the actual B caller.
use super::*;
use crate::{
    component::business_demand::AdmittedTcpDemand,
    runtime::{
        inbound_operation::raw_ip::native_obligations::{ChildKind, NativeObligations},
        inbound_owners::InboundOwnerRegistry,
        packet_flow::{
            PacketFlowAdapter, PacketFlowBindError, PacketFlowBinding, PacketFlowSource,
        },
        raw_ip::{EndpointStackOwner, EndpointStackRecipe, SharedRawIpDevice},
    },
    Proxy,
};
use std::net::IpAddr;
use tokio::sync::{mpsc, oneshot, watch};
use zero_core::{component::SessionId, Address, Network, ProtocolType, Session};
use zero_engine::EngineError;

struct Source(Arc<SharedRawIpDevice>);
impl PacketFlowSource for Source {
    fn bind(&self, _: IpAddr, _: u64) -> Result<PacketFlowBinding, PacketFlowBindError> {
        Ok(PacketFlowBinding {
            local_ip: "10.0.0.1".parse().unwrap(),
            peer_id: None,
            endpoint: self.0.clone(),
        })
    }
}
#[tokio::test]
async fn pending_packet_connect_retires_its_view_but_waits_for_the_original_business_caller_join() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut graph_scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(EndpointStackOwner::TASK_LIMIT),
    );
    graph_scope.enable_registration().unwrap();
    let graph = EndpointStackOwner::suspended();
    graph
        .bind_scope(graph_scope.registrar().downgrade())
        .unwrap();
    let (packets, mut submitted) = mpsc::channel(4);
    let (_publication, endpoint) = watch::channel(packets);
    let device = graph
        .start(
            EndpointStackRecipe::prepare(
                vec!["10.0.0.1".parse().unwrap()],
                1420,
                0,
                endpoint,
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
    let users = device.flow_users().unwrap();
    let adapter = PacketFlowAdapter::new("packet-only".into(), Arc::new(Source(device)));
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("actual-packet-caller", &proxy.protocols, 1, stop)
        .unwrap();
    let children = installed.child_tasks(limit()).unwrap();
    let host = installed.tasks();
    let native = NativeObligations::new(1, 1, 1);
    installed.register(native.resource()).unwrap();
    let mut native_user = native.reserve(ChildKind::Tcp).unwrap();
    let original_registration = native_user.registration();
    let registration = original_registration.clone();
    let (parent_stop, parent_stopped) = oneshot::channel();
    installed
        .owner()
        .spawn_driver(async move {
            parent_stopped.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    let (complete, completed) = oneshot::channel();
    let (returned, returned_result) = oneshot::channel();
    let engine = proxy.engine().clone();
    let services = proxy.tcp_runtime_services().for_inbound("in".into());
    children
        .spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            native_user
                .bind_caller(binding)
                .map_err(std::io::Error::other)?;
            native_user.enter().map_err(std::io::Error::other)?;
            let mut session = Session::new(
                0,
                Address::Ipv4([192, 0, 2, 2]),
                443,
                Network::Tcp,
                ProtocolType::new("verified"),
            );
            let receipt = engine.prepare_session_admitted(&mut session, "in")?;
            let demand = AdmittedTcpDemand::new(&engine, SessionId(session.id), receipt)?;
            let services = services
                .with_admitted_business_identity(&session, demand)?
                .with_caller_native(Some(registration))?
                .execution();
            let result = adapter.prepare_tcp().execute(services, &session).await;
            returned
                .send(result.err().map(|failure| failure.stage))
                .unwrap_or_else(|_| panic!("actual packet result receiver"));
            completed.await.map_err(std::io::Error::other)?;
            Ok::<(), EngineError>(())
        })
        .unwrap();
    let syn = tokio::time::timeout_at(deadline(), submitted.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(zero_stack::packet::parse_tcp(&syn.packet).unwrap().syn);
    let witness = original_registration.caller_witness().unwrap();
    assert_eq!(users.slots.active(), 1);
    assert!(!users.state.lock().unwrap().unaccounted);
    // A real SYN has reached the original endpoint. The handshake is pending;
    // cancelling this shutdown waiter does not remove its installed user.
    pending(users.shutdown(deadline())).await;
    assert!(matches!(
        tokio::time::timeout_at(deadline(), returned_result)
            .await
            .unwrap()
            .unwrap(),
        Some("raw_ip_user")
    ));
    assert_eq!(users.slots.active(), 1);
    assert!(!witness.is_joined());
    complete.send(()).unwrap();
    parent_stop.send(()).unwrap();
    // Caller join is real, but its *other*, native gateway proof remains absent.
    // User closure neither acknowledges nor fabricates that separate receipt.
    assert!(registry
        .confirm_shutdown("actual-packet-caller", deadline())
        .await
        .is_err());
    assert!(witness.is_joined());
    let users_closed = graph.stop_users(deadline()).await.unwrap();
    assert!(users_closed.is_complete());
    assert!(users_closed.closed_io.is_empty());
    assert_eq!(users.slots.active(), 0);
    let stopping = graph.stop(deadline());
    tokio::pin!(stopping);
    let report = loop {
        tokio::select! {
            result = &mut stopping => break result.unwrap(),
            receipt = graph_scope.next_receipt() => {
                let receipt = receipt.expect("original graph tasks await their real consumer");
                assert!(receipt.result().is_ok());
                graph_scope.acknowledge(&receipt).unwrap();
            }
        }
    };
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 3); // original SYN retransmission/control/driver
    assert!(report.closed_io.is_empty());
}
