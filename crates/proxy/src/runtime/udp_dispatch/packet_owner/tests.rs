#![cfg(feature = "managed-datagram-runtime")]
use super::*;
use crate::{
    protocol_registry::PacketPathExecutionServices,
    runtime::udp_flow::packet_path::PacketPathCarrier, Proxy,
};
use std::{future::Future, task::Poll, time::Duration};
use zero_core::{
    component::{NetworkGeneration, SessionId},
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::{CarrierContext, UpstreamBinding},
};
use zero_traits::DatagramCodec;

struct Codec;
impl zero_traits::DatagramCodec<Address> for Codec {
    type Error = zero_core::Error;
    fn encode(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let Address::Ipv4(ip) = target else {
            panic!("fixture uses literal IPv4 frames")
        };
        let mut wire = ip.to_vec();
        wire.extend_from_slice(&port.to_be_bytes());
        wire.extend_from_slice(payload);
        Ok(wire)
    }
    fn decode(&self, wire: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        if wire.len() < 6 {
            return None;
        }
        Some((
            Address::Ipv4(wire[..4].try_into().ok()?),
            u16::from_be_bytes(wire[4..6].try_into().ok()?),
            wire[6..].to_vec(),
        ))
    }
}
fn services() -> (Proxy, crate::protocol_registry::UdpNetworkServices) {
    let proxy = Proxy::new(zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
    let services =
        PacketPathExecutionServices::from_tcp_execution(&proxy.tcp_runtime_services().execution())
            .network();
    (proxy, services)
}
fn demand(generation: u64) -> PacketCarrierDemand {
    PacketCarrierDemand {
        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::DiagnosticProbe,
        })),
        context: CarrierContext {
            session: None,
            deadline: Some(tokio::time::Instant::now() + Duration::from_secs(3)),
            cancel: CancellationToken::new(),
        },
        binding: UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(generation),
        },
    }
}
async fn build(
    services: &crate::protocol_registry::UdpNetworkServices,
    owner: Arc<PacketCarrierOwnerSlot>,
    demand: PacketCarrierDemand,
    port: u16,
) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
    crate::runtime::udp_flow::packet_path_chain::carriers::udp_socket_carrier::owned::build(
        services,
        owner,
        demand,
        "127.0.0.1",
        port,
        Arc::new(Codec),
    )
    .await
}
#[tokio::test]
async fn actual_wire_keeps_original_in_candidate_after_demand_cancel_and_view_drop() {
    let (_proxy, services) = services();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let slot = PacketCarrierOwnerSlot::new();
    let request = demand(services.egress_generation());
    let expected_binding = request.binding.clone();
    let cancelled = request.context.cancel.clone();
    let path = build(
        &services,
        slot.clone(),
        request,
        peer.local_addr().unwrap().port(),
    )
    .await
    .unwrap();
    assert!(Arc::ptr_eq(&path.native_owner().unwrap(), &slot));
    cancelled.cancel(); // original caller's completed budget is not the pool owner.
    let target = Address::Ipv4([192, 0, 2, 1]);
    path.send_to(&target, 53, b"").await.unwrap();
    let mut wire = [0; 64];
    let (size, sender) = peer.recv_from(&mut wire).await.unwrap();
    assert_eq!(&wire[..size], &[192, 0, 2, 1, 0, 53]);
    peer.send_to(&Codec.encode(&target, 53, b"reply").unwrap(), sender)
        .await
        .unwrap();
    assert_eq!(
        path.recv_from_with_source(&mut wire).await.unwrap(),
        (5, Some("192.0.2.1:53".parse().unwrap()))
    );
    assert_eq!(&wire[..5], b"reply");
    drop(path);
    // Dropping the view did not close/refund the original socket.
    assert_eq!(slot.state.lock().unwrap().native.occupied(), 1);
    let report = slot.close_native();
    assert!(report.is_complete());
    assert_eq!(report.native.confirmed_closed, 1);
    assert_eq!(report.binding.as_ref(), Some(&expected_binding));
    assert!(slot.begin(&demand(0).binding).is_err());
}
#[tokio::test]
async fn native_close_wakes_actual_receive_and_is_not_a_task_join_receipt() {
    let (_proxy, services) = services();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let slot = PacketCarrierOwnerSlot::new();
    let path = build(
        &services,
        slot.clone(),
        demand(services.egress_generation()),
        peer.local_addr().unwrap().port(),
    )
    .await
    .unwrap();
    let mut output = [0; 64];
    let mut receiving = Box::pin(path.recv_from(&mut output));
    std::future::poll_fn(|cx| {
        assert!(receiving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(slot.close_native().is_complete());
    assert!(receiving.await.is_err());
}
#[tokio::test]
async fn pending_build_is_retained_until_original_future_drops_and_cannot_reacquire() {
    let slot = PacketCarrierOwnerSlot::new();
    let mut pending = Box::pin(async {
        let _attempt = slot.begin(&demand(0).binding).unwrap();
        std::future::pending::<()>().await;
    });
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let report = slot.close_native();
    assert!(report.building);
    assert!(!report.is_complete());
    assert!(report.native.is_complete()); // no native was acquired, distinct from the build.
    drop(pending);
    assert!(slot.close_native().is_complete());
    assert!(slot.begin(&demand(0).binding).is_err());
}
#[tokio::test]
async fn service_some_and_expired_budget_reject_before_consuming_actual_slot() {
    let (_proxy, services) = services();
    let slot = PacketCarrierOwnerSlot::new();
    let mut wrong = demand(services.egress_generation());
    wrong.context.session = Some(SessionId(17));
    assert!(build(&services, slot.clone(), wrong, 53).await.is_err());
    let mut expired = demand(services.egress_generation());
    expired.context.deadline = Some(tokio::time::Instant::now());
    assert!(build(&services, slot.clone(), expired, 53).await.is_err());
    assert!(!slot.state.lock().unwrap().attempted);
    let attempt = slot.begin(&demand(0).binding).unwrap();
    let original = Arc::new(std::io::Error::from_raw_os_error(13));
    #[derive(Debug)]
    struct Original(Arc<std::io::Error>);
    impl std::fmt::Display for Original {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }
    impl std::error::Error for Original {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }
    let error = attempt.retain_failure(EngineError::Io(std::io::Error::other(Original(
        original.clone(),
    ))));
    let EngineError::Io(error) = error else {
        panic!("expected shared original failure")
    };
    let shared = error
        .get_ref()
        .unwrap()
        .downcast_ref::<SharedBuildFailure>()
        .unwrap();
    let first = shared.0.clone();
    drop(attempt);
    let report = slot.close_native();
    assert!(Arc::ptr_eq(&first, report.failure.as_ref().unwrap()));
    let EngineError::Io(source) = first.as_ref() else {
        panic!("expected actual IO failure")
    };
    assert!(Arc::ptr_eq(
        &source
            .get_ref()
            .unwrap()
            .downcast_ref::<Original>()
            .unwrap()
            .0,
        &original
    ));
    assert_eq!(original.raw_os_error(), Some(13));
}

#[test]
fn business_demand_keeps_original_some_session_and_rejects_erasure() {
    use zero_core::{
        component::{InboundId, RoleId},
        request::RequestFacts,
        ProtocolType,
    };
    let mut request = demand(0);
    let session = SessionId(29);
    request.identity = RequestIdentity::Business(Arc::new(RequestFacts {
        session,
        inbound: RoleId::Inbound(InboundId(4)),
        original_source: None,
        effective_source: None,
        local_endpoint: None,
        original_target: None,
        target: None,
        target_source: None,
        subject: None,
        peer: None,
        protocol: ProtocolType::new("owned-packet-fixture"),
        sni: None,
        process: None,
    }));
    assert!(request.validate().is_err());
    request.context.session = Some(session);
    assert!(request.validate().is_ok());
    request.context.session = Some(SessionId(30));
    assert!(request.validate().is_err());
}
