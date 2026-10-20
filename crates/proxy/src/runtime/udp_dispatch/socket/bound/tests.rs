//! Real dispatch/admission and the actual peer/provider, with explicit fixture grants.
use super::*;
use crate::runtime::{pipe::UdpPipeInput, udp_dispatch::operation::LiveDirectSource};
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    time::{timeout, Instant},
};
use zero_core::{ProtocolType, SessionAuth};

fn budget() -> BoundDirectBudget {
    let two = NonZeroUsize::new(2).unwrap();
    BoundDirectBudget {
        // These are explicit fixture grants, not inferred production defaults.
        limits: DirectEndpointLimits {
            logical: two,
            native_sockets: two,
            pending_native_opens: NonZeroUsize::new(1).unwrap(),
            pending_bytes: NonZeroUsize::new(1_000_000).unwrap(),
            candidates_per_job: two,
        },
        max_bindings: two,
        start_deadline: Instant::now() + Duration::from_secs(2),
    }
}
fn input<'a>(peer: SocketAddr, payload: &'a [u8], auth: &'a SessionAuth) -> UdpPipeInput<'a> {
    UdpPipeInput {
        target: Address::Ipv4([127, 0, 0, 1]),
        port: peer.port(),
        payload,
        auth: Some(auth),
        source_addr: Some("127.0.0.1:12345".parse().unwrap()),
        protocol: ProtocolType::new("raw-ip-test"),
        peer_identity: None,
        route_target: None,
        sniffed_original_target: None,
        sniffed_host_source: None,
        skip_fake_ip_restore: false,
        client_session_id: None,
        transparent_target: false,
        transparent_original_target: None,
        transparent_host_source: None,
    }
}
#[tokio::test]
async fn actual_dispatch_uses_one_bound_provider_with_real_business_auth_and_peer_completion() {
    let live = LiveDirectSource::new().await;
    let runtime = live.runtime.clone().with_bound_direct_budget(budget());
    let mut dispatch = runtime.new_dispatch("source").await.unwrap();
    let registration = dispatch.bound_direct_registration().unwrap();
    assert!(registration.belongs_to(&live.context.binding()));
    assert_eq!(
        registration.identity().task,
        live.context.binding().wait_bound().await.unwrap()
    );
    assert!(registration.closed().is_none());
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let auth = SessionAuth {
        scheme: "actual-test".into(),
        principal_key: Some("actual-udp-caller".into()),
        up_bps: Some(1_000_000),
        down_bps: Some(2_000_000),
        device_limit: Some(2),
        quota_remaining_bytes: Some(1_000_000),
        policy_revision: Some(7),
    };
    let id = dispatch.dispatch(input(address, b"", &auth)).await.unwrap();
    assert_ne!(id, 0);
    let active = live
        .proxy
        .engine()
        .active_sessions()
        .into_iter()
        .find(|session| session.id == id)
        .unwrap();
    assert_eq!(active.auth.as_ref(), Some(&auth));
    assert_eq!(active.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(active.port, address.port());
    let mut bytes = [0; 64];
    let (size, original) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(size, 0, "zero-length UDP datagram is real wire I/O");
    assert!(UdpSocket::bind(original).await.is_err());
    peer.send_to(b"reply", original).await.unwrap();
    let (size, source) = timeout(
        Duration::from_secs(2),
        dispatch.poll_sockets().0.recv_from_addr(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&bytes[..size], b"reply");
    assert_eq!(dispatch.direct_response_session_id(&source), Some(id));
    assert!(source.is_current());
    assert_eq!(
        dispatch
            .dispatch(input(address, b"same original flow", &auth))
            .await
            .unwrap(),
        id
    );
    let (size, same) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"same original flow");
    assert_eq!(same, original);
    let other_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = dispatch
        .dispatch(input(
            other_peer.local_addr().unwrap(),
            b"other target",
            &auth,
        ))
        .await
        .unwrap();
    assert_ne!(second, id);
    let (_, shared_original) = timeout(Duration::from_secs(2), other_peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        shared_original, original,
        "same actual epoch/peer shares its original socket"
    );
    let pressure_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let pressure = dispatch
        .dispatch(input(
            pressure_peer.local_addr().unwrap(),
            b"third logical",
            &auth,
        ))
        .await
        .unwrap_err();
    let EngineError::Io(pressure) = pressure else {
        panic!("capacity must preserve typed rejection")
    };
    let rejection = pressure
        .get_ref()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    assert_eq!(
        rejection.reason,
        zero_platform_tokio::contract::Reason::Capacity
    );
    assert_eq!(
        rejection.delivery,
        zero_platform_tokio::contract::DeliveryState::NotSubmitted
    );
    assert_eq!(
        dispatch
            .dispatch(input(address, b"healthy under pressure", &auth))
            .await
            .unwrap(),
        id
    );
    let (size, still_original) = timeout(Duration::from_secs(2), peer.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes[..size], b"healthy under pressure");
    assert_eq!(still_original, original);
    {
        let receive = dispatch.poll_sockets().0.recv_from_addr(&mut bytes);
        tokio::pin!(receive);
        poll_fn(|cx| {
            assert!(
                receive.as_mut().poll(cx).is_pending(),
                "actual logical receive must be pending"
            );
            Poll::Ready(())
        })
        .await;
    }
    assert!(
        UdpSocket::bind(original).await.is_err(),
        "cancelled receive never closes its original socket"
    );
    assert!(registration.closed().is_none());
    let completed = dispatch.finish_all();
    assert!(completed
        .iter()
        .any(|flow| flow.record.id == id && flow.record.auth.as_ref() == Some(&auth)));
    assert!(
        registration.closed().is_none(),
        "consumer Drop is not peer join/native completion"
    );
    live.close().await;
    let closed = registration
        .closed()
        .expect("original provider and actual peer completed");
    assert!(closed.matches(&registration));
    assert_eq!(closed.joined().task(), registration.identity().task);
    UdpSocket::bind(original).await.unwrap();
}

#[tokio::test]
async fn explicit_controlled_budget_without_the_original_peer_driver_denies_before_native_open() {
    let live = LiveDirectSource::new().await;
    let runtime = UdpIngressRuntime::new(
        live.proxy
            .tcp_runtime_services()
            .for_inbound("source".into()),
    )
    .with_peer_resources(live.context.clone())
    .with_bound_direct_budget(budget());
    let error = runtime
        .new_dispatch("source")
        .await
        .err()
        .expect("missing actual driver cannot fall back");
    assert!(error.to_string().contains("original peer driver"));
    live.close().await;
}
