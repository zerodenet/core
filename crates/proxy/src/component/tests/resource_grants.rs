use super::HostResources;
use std::{error::Error, net::SocketAddr};
use tokio::time::{Duration, Instant};
use zero_core::{component::ComponentId, request::Destination, Address};
use zero_platform_tokio::contract::{
    component::{ResourceAccess, ResourceDeclaration},
    CancellationToken, Reason,
};

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
            ResourceDeclaration::TcpListener(address.clone()),
            ResourceDeclaration::DatagramListener(address.clone()),
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
    let tcp = grants.listen_tcp(address.clone()).await.unwrap();
    let actual = tcp.local_addr().unwrap();
    assert!(std::net::TcpListener::bind(actual).is_err());
    let (accepted, connected) = tokio::time::timeout_at(deadline(), async {
        tokio::join!(tcp.accept(), tokio::net::TcpStream::connect(actual))
    })
    .await
    .unwrap();
    drop(accepted.unwrap());
    drop(connected.unwrap());
    let udp = grants.listen_datagram(address.clone()).await.unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    peer.send_to(b"", udp.local_addr().unwrap()).await.unwrap();
    let mut buffer = [0; 1];
    assert_eq!(
        tokio::time::timeout_at(deadline(), udp.recv_from(&mut buffer))
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
    // Closing an acquired socket does not authorize a second instance in this generation.
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
}

#[tokio::test]
async fn native_bind_failure_keeps_the_real_os_source_and_the_unused_grant() {
    let original = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = endpoint(original.local_addr().unwrap());
    let grants = HostResources::new(
        ComponentId(102),
        &[ResourceDeclaration::TcpListener(address.clone())],
        CancellationToken::new(),
    )
    .unwrap();
    let access = grants.activate(deadline()).unwrap();
    for _ in 0..2 {
        let failure = grants.listen_tcp(address.clone()).await.err().unwrap();
        assert_eq!(failure.reason, Reason::Io);
        assert_eq!(failure.component, Some(ComponentId(102)));
        assert_eq!(
            failure
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::AddrInUse
        );
    }
    drop(original);
    let actual = grants.listen_tcp(address).await.unwrap();
    assert_ne!(actual.local_addr().unwrap().port(), 0);
    drop(access);
}

#[tokio::test]
async fn listener_declarations_do_not_imply_dns_and_expired_unpolled_grants_cannot_bind() {
    let hostname = Destination::new(Address::Domain("localhost".into()), 0);
    assert!(
        matches!(HostResources::new(ComponentId(103),&[ResourceDeclaration::TcpListener(hostname)],CancellationToken::new()),Err(error) if error.reason==Reason::Unsupported)
    );
    let address = endpoint("127.0.0.1:0".parse().unwrap());
    assert!(HostResources::new(
        ComponentId(103),
        &[
            ResourceDeclaration::TcpListener(address.clone()),
            ResourceDeclaration::TcpListener(address.clone())
        ],
        CancellationToken::new()
    )
    .is_err());
    let grants = HostResources::new(
        ComponentId(103),
        &[ResourceDeclaration::DatagramListener(address.clone())],
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
