use super::*;
use crate::runtime::raw_ip::RawIpAction;
use crate::runtime::udp_flow::managed::{packet::PacketUdpFlow, ManagedTupleUdpFlowConnection};
use std::{
    future::Future,
    sync::atomic::{AtomicU8, Ordering},
    task::Poll,
};
use zero_platform_tokio::contract::scope::TaskScope;

struct Tunnel {
    sent: Arc<AtomicU8>,
    changed: Arc<tokio::sync::Notify>,
}
impl RawIpTunnel for Tunnel {
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(Vec::new())
    }
    fn send_ip_packet(&mut self, packet: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
        let flag = match zero_stack::packet::ip_protocol(packet) {
            Some(zero_stack::packet::IPPROTO_TCP) => 1,
            Some(zero_stack::packet::IPPROTO_UDP) => 2,
            _ => 0,
        };
        self.sent.fetch_or(flag, Ordering::Release);
        self.changed.notify_one();
        Ok(Vec::new())
    }
    fn receive_datagram(
        &mut self,
        _: Option<SocketAddr>,
        _: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError> {
        unreachable!("the actual driver must request authentication facts")
    }
    fn receive_datagram_with_authentication(
        &mut self,
        _: Option<SocketAddr>,
        bytes: &[u8],
    ) -> Result<(Vec<RawIpAction>, bool), EngineError> {
        Ok((
            vec![RawIpAction::SendNetwork(b"reply".to_vec().into())],
            bytes == b"authenticated",
        ))
    }
    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(Vec::new())
    }
    fn timer_enabled(&self) -> bool {
        false
    }
    fn allows_source(&self, _: IpAddr) -> bool {
        true
    }
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}

#[tokio::test]
async fn same_wire_driver_and_client_tasks_join_in_scope_with_authenticated_source_roaming() {
    let original = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local = original.local_addr().unwrap();
    let carrier = Arc::new(DirectRawIpWireCarrier::new(original));
    let configured = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let roaming = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut scope = TaskScope::suspended_hosted(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(4),
    );
    scope.enable_registration().unwrap();
    let owner = EndpointStackOwner::suspended();
    owner.bind_scope(scope.registrar().downgrade()).unwrap();
    let sent = Arc::new(AtomicU8::new(0));
    let changed = Arc::new(tokio::sync::Notify::new());
    let recipe = WireStackRecipe::prepare(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        configured.local_addr().unwrap(),
        carrier.clone(),
        Box::new(Tunnel {
            sent: sent.clone(),
            changed: changed.clone(),
        }),
        Default::default(),
    )
    .unwrap();
    let device = owner.start_wire(recipe).unwrap();
    device.wait_ready().await.unwrap();
    assert_eq!(owner.test_original_witnesses().len(), 1); // only the actual wire driver
    let local_ip = "10.0.0.1".parse().unwrap();
    let target = "192.0.2.2:443".parse().unwrap();
    let mut connecting = Box::pin(device.open_tcp_observed(local_ip, target, None));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let target_address = zero_core::Address::Ipv4([192, 0, 2, 2]);
    let flow = PacketUdpFlow::open(
        device.clone(),
        target_address.clone(),
        443,
        target,
        local_ip,
        None,
    )
    .unwrap();
    flow.send(&target_address, 443, b"actual-client-udp")
        .await
        .unwrap();
    tokio::time::timeout_at(deadline(), async {
        while sent.load(Ordering::Acquire) != 3 {
            changed.notified().await;
        }
    })
    .await
    .unwrap();
    // The TCP control/retransmission pair and UDP socket worker are created
    // only by these actual pending flows, in this same original host Scope.
    let witnesses = owner.test_original_witnesses();
    assert_eq!(witnesses.len(), 4);
    roaming.send_to(b"authenticated", local).await.unwrap();
    let mut bytes = [0; 32];
    let (size, source) = tokio::time::timeout_at(deadline(), roaming.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"reply");
    assert_eq!(source, local);
    configured.send_to(b"untrusted", local).await.unwrap();
    // Authenticated roaming changed the endpoint; an unauthenticated packet
    // from the former endpoint cannot move it back or trigger a reply.
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(20),
        configured.recv_from(&mut bytes)
    )
    .await
    .is_err());
    owner.revoke();
    scope.begin_shutdown();
    let drain = async {
        let mut original_joins = Vec::new();
        while let Some(receipt) = tokio::time::timeout_at(deadline(), scope.next_receipt())
            .await
            .unwrap()
        {
            assert!(receipt.result().is_ok(), "{receipt:?}");
            original_joins.push(receipt.joined().clone());
            scope.acknowledge(&receipt).unwrap();
        }
        assert!(scope.is_drained());
        original_joins
    };
    let (joined, monitor) = tokio::join!(owner.stop(deadline()), drain);
    assert!(joined.unwrap().is_complete());
    assert_eq!(monitor.len(), 4);
    assert!(witnesses.iter().all(|witness| witness.is_joined()));
    assert!(owner.is_drained());
    assert!(connecting.await.is_err());
    drop(flow);
    // This library carrier's explicit close is separate from host task joins.
    // It does not qualify a production native provider or standard Packet role.
    carrier.close_native().await.unwrap();
    assert!(carrier
        .send(b"late", configured.local_addr().unwrap())
        .await
        .is_err());
}
