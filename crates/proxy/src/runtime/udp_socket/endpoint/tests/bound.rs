//! Actual host Source, same registered peer task and original OS endpoints.
use super::*;
use crate::runtime::{
    udp_dispatch::operation::LiveDirectSource,
    udp_socket::endpoint::{dispatch::DirectDispatchEndpoint, peer::DirectBoundPeerOptions},
};
use std::error::Error;
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{component::ResourceAccess, RetryClass};

async fn business(
    live: &LiveDirectSource,
    peer: SocketAddr,
) -> (Session, zero_engine::SessionHandle) {
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        peer.port(),
        Network::Udp,
        ProtocolType::new("direct"),
    );
    live.runtime
        .prepare_udp_session(&mut session, "source")
        .await
        .unwrap();
    let handle = live.runtime.track_session(session.id);
    (session, handle)
}
async fn send_bridge(
    bridge: &mut DirectDispatchEndpoint,
    session: SessionId,
    peer: &tokio::net::UdpSocket,
    payload: &[u8],
) -> SocketAddr {
    let target = peer.local_addr().unwrap();
    // A prepared domain candidate remains a domain at the logical boundary;
    // this scoped endpoint never performs DNS or replaces the business ID.
    let packet = bridge
        .send_new_packet(
            session,
            &Address::Domain("loopback.test".into()),
            &[target],
            payload,
        )
        .await
        .unwrap();
    assert_eq!(packet.sent, payload.len());
    let mut bytes = [0; 64];
    let (size, original) = tokio::time::timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], payload);
    original
}
fn as_binding(
    resources: &Arc<dyn ResourceAccess>,
    id: zero_platform_tokio::contract::component::DatagramBindingId,
    generation: NetworkGeneration,
    policy: DirectUdpPolicy,
    source: Arc<dyn zero_platform_tokio::contract::component::DatagramBindingAuthorization>,
) -> DirectEndpointBinding {
    DirectEndpointBinding {
        resources: resources.clone(),
        id,
        generation,
        policy,
        source,
        pre_io_bind_attempts: NonZeroUsize::new(1).unwrap(),
    }
}

fn assert_rejection(error: &EngineError, reason: Reason) {
    assert!(super::super::failure::logical_rejection(error));
    let EngineError::Io(io) = error else {
        panic!("actual scoped error must retain its typed boundary");
    };
    let channel = io
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .expect("actual source ChannelError is not replaced by Io/Unknown");
    assert_eq!(channel.reason, reason);
    assert_eq!(channel.delivery, DeliveryState::NotSubmitted);
    assert_eq!(channel.retry, RetryClass::Never);
    assert!(!channel.retry_permitted());
    if reason == Reason::StalePlan {
        assert_eq!(channel.stage, Stage::Prepare);
        assert!(channel.source().is_some());
    }
}

#[tokio::test]
async fn actual_bound_peer_shares_original_ports_preserves_tombstones_and_keeps_healthy_flows_under_pressure(
) {
    let live = LiveDirectSource::new().await;
    let two = NonZeroUsize::new(2).unwrap();
    let retirement = live.proxy.retired_datagrams.clone();
    let ((mut bridge, endpoint, driver, resources), authority) = live
        .context
        .claim_bound_outbound_datagram(deadline(), two, two, |grant| {
            let mut grants = limits();
            grants.native_sockets = two;
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
            let endpoint = owner.endpoint();
            Ok((
                zero_platform_tokio::contract::lifecycle::erase_lifecycle(owner),
                (
                    DirectDispatchEndpoint::new(endpoint.clone(), retirement),
                    endpoint,
                    driver,
                    grant.resources,
                ),
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
    // A second actual source mint for this same policy/network/peer epoch must
    // reuse the provider binding, rather than consume one per logical target.
    let same = authority
        .authorize(live.source().await, generation)
        .unwrap();
    assert_eq!(authorized.id, same.id);
    let binding = as_binding(
        &resources,
        authorized.id,
        generation,
        authorized.policy,
        authorized.source,
    );
    let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let (one, _one_handle) = business(&live, first.local_addr().unwrap()).await;
    let (two, _two_handle) = business(&live, second.local_addr().unwrap()).await;
    bridge
        .admit_bound(
            SessionId(one.id),
            binding.clone(),
            None,
            CancellationToken::new(),
        )
        .unwrap();
    bridge
        .admit_bound(
            SessionId(two.id),
            binding.clone(),
            None,
            CancellationToken::new(),
        )
        .unwrap();
    let original = send_bridge(&mut bridge, SessionId(one.id), &first, b"first").await;
    assert_eq!(
        send_bridge(&mut bridge, SessionId(two.id), &second, b"second").await,
        original
    );
    first.send_to(b"already read", original).await.unwrap();
    let mut bytes = [0; 64];
    let (size, old) = bridge.recv_from_addr(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"already read");
    assert_eq!(old.session_id, Some(one.id));
    assert!(old.is_current());
    assert!(bridge
        .finish_session(SessionId(one.id), deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(!old.is_current());
    assert!(tokio::net::UdpSocket::bind(original).await.is_err());
    let (three, _three_handle) = business(&live, first.local_addr().unwrap()).await;
    bridge
        .admit_bound(
            SessionId(three.id),
            binding.clone(),
            None,
            CancellationToken::new(),
        )
        .unwrap();
    let collision = send_bridge(&mut bridge, SessionId(three.id), &first, b"new first").await;
    assert_ne!(
        collision, original,
        "retired tuple cannot inherit old replies"
    );
    assert_eq!(
        send_bridge(&mut bridge, SessionId(two.id), &second, b"keeper").await,
        original
    );
    let (four, _four_handle) = business(&live, first.local_addr().unwrap()).await;
    bridge
        .admit_bound(SessionId(four.id), binding, None, CancellationToken::new())
        .unwrap();
    let pressure = match bridge
        .send_new_packet(
            SessionId(four.id),
            &Address::Ipv4([127, 0, 0, 1]),
            &[first.local_addr().unwrap()],
            b"requires third native original",
        )
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a third native original must fail at the explicit capacity bound"),
    };
    assert_rejection(&pressure, Reason::Capacity);
    assert!(bridge
        .finish_session(SessionId(four.id), deadline())
        .await
        .unwrap()
        .is_complete());
    assert_eq!(
        send_bridge(&mut bridge, SessionId(two.id), &second, b"still healthy").await,
        original
    );
    assert!(!live.context.binding().cancellation().is_cancelled());
    assert!(bridge
        .finish_session(SessionId(three.id), deadline())
        .await
        .unwrap()
        .is_complete());
    tokio::net::UdpSocket::bind(collision).await.unwrap();
    assert!(bridge
        .finish_session(SessionId(two.id), deadline())
        .await
        .unwrap()
        .is_complete());
    tokio::net::UdpSocket::bind(original).await.unwrap();
    endpoint.refresh_sources(deadline()).await.unwrap();
    assert_eq!(
        live.context
            .binding()
            .wait_joined(Instant::now() + Duration::from_millis(1))
            .await
            .unwrap_err()
            .reason,
        Reason::Timeout
    );
    drop(bridge);
    drop(resources);
    drop(endpoint);
    live.close().await;
}

mod epoch;

#[cfg(target_os = "linux")]
mod fallback;
