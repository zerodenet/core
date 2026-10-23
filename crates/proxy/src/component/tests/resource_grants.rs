use super::HostResources;
use std::{error::Error, net::SocketAddr};
use tokio::time::{Duration, Instant};
use zero_core::{component::ComponentId, request::Destination, Address};
use zero_platform_tokio::contract::{
    component::{ResourceAccess, ResourceDeclaration},
    CancellationToken, Reason,
};
#[path = "resource_grants/controlled.rs"]
mod controlled;

fn endpoint(address: SocketAddr) -> Destination {
    Destination::new(
        match address.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        address.port(),
    )
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn real_listener_grants_enforce_start_phase_protocol_endpoint_and_lifetime_budget() {
    let address = endpoint("127.0.0.1:0".parse().unwrap());
    let cancel = CancellationToken::new();
    let grants = HostResources::new(
        ComponentId(101),
        &[
            ResourceDeclaration::TcpListener {
                accepted: None,
                destination: address.clone(),
                max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
            },
            ResourceDeclaration::DatagramListener {
                destination: address.clone(),
                max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
                max_pending_sends: std::num::NonZeroUsize::new(1).unwrap(),
            },
        ],
        cancel.clone(),
    )
    .unwrap();
    assert_eq!(
        grants
            .listen_tcp(address.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    let access = grants.activate(deadline()).unwrap();
    let wrong = endpoint("127.0.0.2:0".parse().unwrap());
    assert_eq!(
        grants.listen_tcp(wrong).await.err().unwrap().reason,
        Reason::InvalidConfig
    );
    // A future which was never polled performs no bind and consumes no budget.
    drop(grants.listen_tcp(address.clone()));
    let mut tcp = grants.listen_tcp(address.clone()).await.unwrap();
    let actual = tcp.local_addr().unwrap();
    assert!(std::net::TcpListener::bind(actual).is_err());
    let (accepted, connected) = tokio::time::timeout_at(deadline(), async {
        tokio::join!(tcp.accept(), tokio::net::TcpStream::connect(actual))
    })
    .await
    .unwrap();
    drop(accepted.unwrap());
    drop(connected.unwrap());
    let mut udp = grants.listen_datagram(address.clone()).await.unwrap();
    let udp_address = udp.rx.local_addr().unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    peer.send_to(b"", udp.rx.local_addr().unwrap())
        .await
        .unwrap();
    let mut buffer = [0; 1];
    assert_eq!(
        tokio::time::timeout_at(deadline(), udp.rx.recv_from_addr(&mut buffer))
            .await
            .unwrap()
            .unwrap()
            .0,
        0
    );
    assert_eq!(
        grants
            .listen_tcp(address.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Capacity
    );
    assert_eq!(
        grants
            .listen_datagram(address.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Capacity
    );
    drop(tcp);
    drop(udp);
    // View Drop is not a native close. The granting host still owns both originals.
    assert!(std::net::TcpListener::bind(actual).is_err());
    assert!(std::net::UdpSocket::bind(udp_address).is_err());
    assert_eq!(
        grants
            .listen_tcp(address.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Capacity
    );
    drop(access);
    assert_eq!(
        grants
            .listen_datagram(address.clone())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    let access = grants.activate(deadline()).unwrap();
    cancel.cancel();
    assert_eq!(
        grants.listen_tcp(address).await.err().unwrap().reason,
        Reason::Revoked
    );
    drop(access);
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 2);
    std::net::TcpListener::bind(actual).unwrap();
    std::net::UdpSocket::bind(udp_address).unwrap();
    assert!(grants
        .shutdown_io(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
}

#[tokio::test]
async fn native_bind_failure_keeps_the_real_os_source_and_the_unused_grant() {
    let original = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = endpoint(original.local_addr().unwrap());
    let grants = HostResources::new(
        ComponentId(102),
        &[ResourceDeclaration::TcpListener {
            accepted: None,
            destination: address.clone(),
            max_bind_attempts: std::num::NonZeroUsize::new(3).unwrap(),
        }],
        CancellationToken::new(),
    )
    .unwrap();
    let access = grants.activate(deadline()).unwrap();
    for _ in 0..2 {
        let failure = grants.listen_tcp(address.clone()).await.err().unwrap();
        assert_eq!(failure.reason, Reason::Io);
        assert_eq!(failure.component, Some(ComponentId(102)));
        assert_eq!(original_io(&failure).kind(), std::io::ErrorKind::AddrInUse);
        assert!(original_io(&failure).raw_os_error().is_some());
    }
    drop(original);
    let actual = grants.listen_tcp(address).await.unwrap();
    assert_ne!(actual.local_addr().unwrap().port(), 0);
    let real = actual.local_addr().unwrap();
    drop(access);
    let report = grants.shutdown_io(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(report.closed_io[0].contains("3 original native closes"));
    std::net::TcpListener::bind(real).unwrap();
}

#[tokio::test]
async fn listener_declarations_do_not_imply_dns_and_expired_unpolled_grants_cannot_bind() {
    let hostname = Destination::new(Address::Domain("localhost".into()), 0);
    assert!(
        matches!(HostResources::new(ComponentId(103),&[ResourceDeclaration::TcpListener { accepted: None, destination: hostname, max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap() }],CancellationToken::new()),Err(error) if error.reason==Reason::Unsupported)
    );
    let address = endpoint("127.0.0.1:0".parse().unwrap());
    assert!(HostResources::new(
        ComponentId(103),
        &[
            ResourceDeclaration::TcpListener {
                accepted: None,
                destination: address.clone(),
                max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap()
            },
            ResourceDeclaration::TcpListener {
                accepted: None,
                destination: address.clone(),
                max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap()
            }
        ],
        CancellationToken::new()
    )
    .is_err());
    let grants = HostResources::new(
        ComponentId(103),
        &[ResourceDeclaration::DatagramListener {
            destination: address.clone(),
            max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
            max_pending_sends: std::num::NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
    )
    .unwrap();
    let access = grants.activate(deadline()).unwrap();
    let unpolled = grants.listen_datagram(address.clone());
    drop(access);
    assert_eq!(unpolled.await.err().unwrap().reason, Reason::InvalidConfig);
    let end = Instant::now() + Duration::from_secs(1);
    let _access = grants.activate(end).unwrap();
    tokio::time::sleep_until(end).await;
    assert_eq!(
        grants.listen_datagram(address).await.err().unwrap().reason,
        Reason::Timeout
    );
}

fn original_io<'a>(mut error: &'a (dyn Error + 'static)) -> &'a std::io::Error {
    loop {
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            return io;
        }
        error = error.source().expect("actual original IO source");
    }
}
