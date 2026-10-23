//! Actual settled bind failure versus submitted-send failure, on one real peer.
use super::*;
use crate::runtime::inbound_owners::PeerBoundDatagramRegistration;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use tokio::{net::UdpSocket, time::timeout};
use zero_platform_tokio::{
    controlled_datagram::{AcquisitionOutcome, DatagramAcquisitionError},
    EgressInterface,
};

struct BoundFixture {
    live: LiveDirectSource,
    bridge: DirectDispatchEndpoint,
    binding: DirectEndpointBinding,
    registration: PeerBoundDatagramRegistration,
}
impl BoundFixture {
    async fn new(attempts: usize, invalid_v6_interface: bool) -> Self {
        let live = LiveDirectSource::new().await;
        if invalid_v6_interface {
            live.runtime
                .services()
                .upstream()
                .egress_control()
                .replace_for(
                    true,
                    Some(
                        EgressInterface::new("invalid-device-name-over-the-native-limit", u32::MAX)
                            .unwrap(),
                    ),
                );
        }
        let one = NonZeroUsize::new(1).unwrap();
        let ((bridge, driver, resources), authority) = live
            .context
            .claim_bound_outbound_datagram(deadline(), one, one, |grant| {
                let mut grants = limits();
                grants.native_sockets = one;
                grants.candidates_per_job = NonZeroUsize::new(2).unwrap();
                let (owner, driver) = DirectEndpointOwner::for_bound_peer(
                    DirectBoundPeerOptions {
                        identity: DirectEndpointIdentity {
                            resource: grant.identity.instance,
                            ingress: live.context.ingress(),
                            dispatcher: grant.identity.association,
                        },
                        task: grant.identity.task,
                        queue: UdpReceiveQueueConfig::default(),
                        limits: grants,
                    },
                    live.context.binding(),
                    None,
                )?;
                let bridge = DirectDispatchEndpoint::new(
                    owner.endpoint(),
                    live.proxy.retired_datagrams.clone(),
                );
                Ok((
                    zero_platform_tokio::contract::lifecycle::erase_lifecycle(owner),
                    (bridge, driver, grant.resources),
                ))
            })
            .await
            .unwrap();
        live.driver_slot.submit(driver).unwrap();
        let generation = NetworkGeneration(
            live.runtime
                .services()
                .upstream()
                .egress_control()
                .generation(),
        );
        let authorized = authority
            .authorize(live.source().await, generation)
            .unwrap();
        let mut binding = as_binding(
            &resources,
            authorized.id,
            generation,
            authorized.policy,
            authorized.source,
        );
        binding.pre_io_bind_attempts = NonZeroUsize::new(attempts).unwrap();
        Self {
            live,
            bridge,
            binding,
            registration: authority.registration(),
        }
    }
    async fn admit(&mut self, peer: SocketAddr) -> (SessionId, zero_engine::SessionHandle) {
        let (session, handle) = business(&self.live, peer).await;
        let id = SessionId(session.id);
        assert_ne!(session.id, 0);
        self.bridge
            .admit_bound(id, self.binding.clone(), None, CancellationToken::new())
            .unwrap();
        (id, handle)
    }
}
fn candidates(peer: SocketAddr) -> [SocketAddr; 2] {
    // Nonloopback retains the configured invalid IPv6 interface. Loopback
    // intentionally bypasses interface binding in the real selector.
    [
        SocketAddr::new("2001:db8::197".parse().unwrap(), peer.port()),
        peer,
    ]
}
fn has_closed_acquisition(error: &(dyn Error + 'static), depth: usize) -> bool {
    assert!(depth < 64);
    if let Some(acquired) = error.downcast_ref::<DatagramAcquisitionError>() {
        return matches!(acquired.outcome, AcquisitionOutcome::Closed(_));
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        if io
            .get_ref()
            .is_some_and(|inner| has_closed_acquisition(inner, depth + 1))
        {
            return true;
        }
    }
    error
        .source()
        .is_some_and(|inner| has_closed_acquisition(inner, depth + 1))
}
async fn assert_no_packet(peer: &UdpSocket) {
    let mut bytes = [0; 64];
    let receive = peer.recv_from(&mut bytes);
    tokio::pin!(receive);
    poll_fn(|cx| {
        assert!(
            receive.as_mut().poll(cx).is_pending(),
            "actual receiver has no replayed payload"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn sealed_settled_original_bind_failure_falls_back_before_the_single_real_payload_send() {
    let mut fixture = BoundFixture::new(2, true).await;
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let (session, _handle) = fixture.admit(address).await;
    let sent = fixture
        .bridge
        .send_new_packet(
            session,
            &Address::Domain("two-families.test".into()),
            &candidates(address),
            b"sent once",
        )
        .await
        .unwrap();
    assert_eq!(sent.target, address);
    let mut bytes = [0; 64];
    let (size, original) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"sent once");
    assert!(original.is_ipv4());
    assert_eq!(sent.local.unwrap().port(), original.port());
    assert!(UdpSocket::bind(original).await.is_err());
    // With native cap one, the successful second original cannot exist until
    // the first acquisition's original is really closed/reaped by this owner.
    assert_no_packet(&peer).await;
    peer.send_to(b"reverse", original).await.unwrap();
    let (size, source) = timeout(
        Duration::from_secs(2),
        fixture.bridge.recv_from_addr(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&bytes[..size], b"reverse");
    assert_eq!(source.session_id, Some(session.0));
    assert!(source.is_current());
    assert!(fixture.registration.closed().is_none());
    fixture
        .bridge
        .finish_session(session, deadline())
        .await
        .unwrap();
    drop(fixture.bridge);
    fixture.live.close().await;
    assert!(fixture
        .registration
        .closed()
        .unwrap()
        .matches(&fixture.registration));
    UdpSocket::bind(original).await.unwrap();
}

#[tokio::test]
async fn finite_bind_attempt_exhaustion_keeps_the_original_peer_healthy_for_a_new_real_flow() {
    let mut fixture = BoundFixture::new(1, true).await;
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let (failed, _failed_handle) = fixture.admit(address).await;
    let error = fixture
        .bridge
        .send_new_packet(
            failed,
            &Address::Domain("two-families.test".into()),
            &candidates(address),
            b"must not reach wire",
        )
        .await
        .err()
        .unwrap();
    assert!(
        has_closed_acquisition(&error, 0),
        "retain the real original acquisition outcome/source"
    );
    assert!(!fixture.live.context.binding().cancellation().is_cancelled());
    assert_no_packet(&peer).await;
    fixture
        .bridge
        .finish_session(failed, deadline())
        .await
        .unwrap();
    let (healthy, _healthy_handle) = fixture.admit(address).await;
    assert_ne!(failed, healthy);
    let original = send_bridge(
        &mut fixture.bridge,
        healthy,
        &peer,
        b"same listener remains healthy",
    )
    .await;
    assert!(fixture.registration.closed().is_none());
    fixture
        .bridge
        .finish_session(healthy, deadline())
        .await
        .unwrap();
    drop(fixture.bridge);
    fixture.live.close().await;
    assert!(fixture
        .registration
        .closed()
        .unwrap()
        .matches(&fixture.registration));
    UdpSocket::bind(original).await.unwrap();
}

#[tokio::test]
async fn actual_send_error_never_replays_payload_to_another_family() {
    let mut fixture = BoundFixture::new(2, false).await;
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let (session, _handle) = fixture.admit(address).await;
    let error = fixture
        .bridge
        .send_new_packet(
            session,
            &Address::Domain("two-families.test".into()),
            &["[::1]:0".parse().unwrap(), address],
            b"never replay a send",
        )
        .await
        .err()
        .unwrap();
    assert!(!has_closed_acquisition(&error, 0));
    // This fatal send error ends the original inline driver and consumer. The
    // actual observer may already have joined both and explicitly closed the
    // original provider before this waiter resumes; that is real completion,
    // not a timing promise that closed() must still be None.
    let joined = fixture
        .live
        .context
        .binding()
        .wait_joined(deadline())
        .await
        .unwrap();
    assert_eq!(joined.task(), fixture.registration.identity().task);
    let original_failure = joined
        .failure()
        .expect("actual send failure remains a failed peer exit");
    assert_no_packet(&peer).await;
    drop(fixture.bridge);
    // Join/native close does not erase the business send failure or allow a
    // second-family replay. Keep the same original error in the close receipt.
    assert!(fixture.live.finish().await.is_err());
    let closed = fixture.registration.closed().unwrap();
    assert!(closed.matches(&fixture.registration));
    assert_eq!(closed.joined().task(), joined.task());
    assert_eq!(closed.joined().class(), joined.class());
    assert!(Arc::ptr_eq(
        &closed.joined().failure().unwrap(),
        &original_failure
    ));
}
