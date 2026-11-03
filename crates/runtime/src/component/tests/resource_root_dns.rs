//! Real DNS wire consumer of the sole original HostResources native UDP owner.
use crate::component::instance::resources::HostResources;
use std::{collections::BTreeMap, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    net::UdpSocket,
    runtime::Handle,
    sync::oneshot,
    time::{timeout, Instant},
};
use zero_config::{DnsAnswerConfig, DnsConfig, DnsServerConfig};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
use zero_dns::{DnsSystem, OwnedDnsQueryTasks};
use zero_platform_tokio::{
    contract::{component::ResourceDeclaration, scope::TaskScope, CancellationToken},
    EgressInterfaceControl,
};
use zero_traits::IpAddress;

fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn query() -> Vec<u8> {
    let mut query = vec![0x4d, 0x12, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in [b"owned".as_slice(), b"example".as_slice()] {
        query.push(u8::try_from(label.len()).unwrap());
        query.extend_from_slice(label);
    }
    query.extend_from_slice(&[0, 0, 1, 0, 1]);
    query
}
fn config(peer: SocketAddr) -> DnsConfig {
    DnsConfig {
        servers: BTreeMap::from([(
            "owned".into(),
            DnsServerConfig::Udp {
                host: peer.ip().to_string(),
                port: peer.port(),
                bootstrap: Vec::new(),
                detour: None,
            },
        )]),
        default_server: "owned".into(),
        dispatch: Vec::new(),
        cache: None,
        reverse_mapping: None,
        answer: DnsAnswerConfig::Real,
        policy: Default::default(),
    }
}
fn fixture(scope: &TaskScope, peer: SocketAddr) -> (Arc<HostResources>, Arc<DnsSystem>) {
    let egress = EgressInterfaceControl::default();
    let host = HostResources::new_for_instance(
        ComponentInstanceId {
            component: ComponentId(8051),
            generation: ResourceGeneration(1),
        },
        &[ResourceDeclaration::ControlledOutboundDatagram {
            peer,
            network_generation: NetworkGeneration(egress.generation()),
            max_sockets: n(1),
        }],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let registrar = scope.registrar();
    let grants = host
        .issue_datagram_root(&[0], egress.clone(), &registrar, n(1))
        .unwrap();
    let dns = DnsSystem::build_with_owned_udp(
        &config(peer),
        egress,
        OwnedDnsQueryTasks::new(&registrar, n(1)),
        grants,
    )
    .unwrap();
    let access = host.activate(deadline()).unwrap();
    drop(access); // Root permission belongs to the original live root, not StartAccess.
    (host, Arc::new(dns))
}

#[tokio::test]
async fn owned_udp_dns_keeps_original_wire_matching_and_native_close_is_not_query_join() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut scope = TaskScope::new(Handle::current(), n(1));
    let (host, dns) = fixture(&scope, server.local_addr().unwrap());
    let original = query();
    let (observed, peer) = oneshot::channel();
    let response = zero_dns::udp::build_dns_response(&original, &[IpAddress::V4([203, 0, 113, 8])]);
    let expected = response.clone();
    let received = original.clone();
    let serving = tokio::spawn(async move {
        let mut bytes = [0; 4096];
        let (size, address) = server.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[..size], received);
        let impostor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        impostor.send_to(&response, address).await.unwrap();
        let mut wrong_id = response.clone();
        wrong_id[0] ^= 1;
        server.send_to(&wrong_id, address).await.unwrap();
        server.send_to(&response, address).await.unwrap();
        observed.send(address).unwrap();
    });
    let answer = timeout(Duration::from_secs(3), dns.answer_udp_query(&original))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(answer, expected);
    let local = peer.await.unwrap();
    let probe = std::net::UdpSocket::bind(local).unwrap();
    // Original native close really released this FD's address; it did not
    // consume the original query task. Query shutdown without the host pump
    // must remain pending even after all DNS bytes were delivered.
    assert!(dns.shutdown_queries(Instant::now()).await.is_err());
    let actual = scope.next_exit().await.unwrap();
    assert!(actual.result.is_ok());
    assert_ne!(actual.id.0, 0);
    dns.shutdown_queries(deadline()).await.unwrap();
    serving.await.unwrap();
    let report = host.shutdown_io(deadline()).await.unwrap();
    assert_eq!(report.closed_io.len(), 1);
    drop(probe);
}

#[tokio::test]
async fn canceled_dns_query_retains_original_fd_until_explicit_native_close_after_actual_join() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut scope = TaskScope::new(Handle::current(), n(1));
    let (host, dns) = fixture(&scope, server.local_addr().unwrap());
    let caller = {
        let dns = dns.clone();
        tokio::spawn(async move { dns.answer_udp_query(&query()).await })
    };
    let mut buffer = [0; 4096];
    let (_, local) = timeout(Duration::from_secs(3), server.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        std::net::UdpSocket::bind(local).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(dns.shutdown_queries(Instant::now()).await.is_err());
    // A cancelled waiter/future and a close deadline own no native-close proof.
    assert_eq!(
        std::net::UdpSocket::bind(local).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    let actual = scope.next_exit().await.unwrap();
    assert!(actual.result.is_ok());
    dns.shutdown_queries(deadline()).await.unwrap();
    assert_eq!(
        std::net::UdpSocket::bind(local).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    let report = host.shutdown_io(deadline()).await.unwrap();
    assert_eq!(report.closed_io.len(), 1);
    let probe = std::net::UdpSocket::bind(local).unwrap();
    drop(probe);
}

#[tokio::test]
async fn truncated_owned_udp_reply_never_falls_through_to_an_unowned_tcp_socket() {
    use std::{future::poll_fn, task::Poll};
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = server.local_addr().unwrap();
    let tcp = tokio::net::TcpListener::bind(peer).await.unwrap();
    let mut scope = TaskScope::new(Handle::current(), n(1));
    let (host, dns) = fixture(&scope, peer);
    let original = query();
    let serving = tokio::spawn(async move {
        let mut bytes = [0; 4096];
        let (size, local) = server.recv_from(&mut bytes).await.unwrap();
        let mut truncated =
            zero_dns::udp::build_dns_response(&bytes[..size], &[IpAddress::V4([203, 0, 113, 8])]);
        truncated[2] |= 2;
        server.send_to(&truncated, local).await.unwrap();
        local
    });
    let answer = timeout(Duration::from_secs(3), dns.answer_udp_query(&original))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&answer[..2], &original[..2]);
    assert_eq!(
        answer[3] & 15,
        2,
        "unowned TCP is rejected into original wire SERVFAIL"
    );
    let local = serving.await.unwrap();
    let probe = std::net::UdpSocket::bind(local).unwrap();
    let actual = scope.next_exit().await.unwrap();
    assert!(actual.result.is_ok());
    // Inspect the actual listening native TCP endpoint, not an elapsed delay.
    poll_fn(|cx| {
        assert!(
            tcp.poll_accept(cx).is_pending(),
            "no naked TCP fallback was attempted"
        );
        Poll::Ready(())
    })
    .await;
    dns.shutdown_queries(deadline()).await.unwrap();
    let report = host.shutdown_io(deadline()).await.unwrap();
    assert_eq!(report.closed_io.len(), 1);
    drop(probe);
}
