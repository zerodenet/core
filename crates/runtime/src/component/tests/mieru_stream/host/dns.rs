//! Real node DNS wire, original root authority, supervised query and Mieru IO.
use super::*;
use crate::component::instance::HostResources;
use std::{collections::BTreeMap, num::NonZeroUsize};
use zero_config::{DnsConfig, DnsServerConfig, OutboundProtocolConfig};
use zero_core::component::{
    ComponentId, ComponentInstanceId, NetworkGeneration, ResourceGeneration,
};
use zero_dns::{DnsSystem, OwnedDnsQueryTasks};
use zero_platform_tokio::{
    contract::{component::ResourceDeclaration, scope::TaskScope},
    EgressInterfaceControl,
};

fn owned_dns(
    scope: &TaskScope,
    peer: std::net::SocketAddr,
    egress: &EgressInterfaceControl,
) -> (Arc<HostResources>, Arc<DnsSystem>) {
    let limit = NonZeroUsize::new(4).unwrap();
    let resources = HostResources::new_for_instance(
        ComponentInstanceId {
            component: ComponentId(8211),
            generation: ResourceGeneration(1),
        },
        &[ResourceDeclaration::ControlledOutboundDatagram {
            peer,
            network_generation: NetworkGeneration(egress.generation()),
            max_sockets: limit,
        }],
        zero_platform_tokio::contract::CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let grants = resources
        .issue_datagram_root(&[0], egress.clone(), &scope.registrar(), limit)
        .unwrap();
    let config = DnsConfig {
        servers: BTreeMap::from([(
            "node".into(),
            DnsServerConfig::Udp {
                host: peer.ip().to_string(),
                port: peer.port(),
                bootstrap: vec![],
                detour: None,
            },
        )]),
        default_server: "node".into(),
        dispatch: vec![],
        cache: None,
        reverse_mapping: None,
        answer: zero_config::DnsAnswerConfig::Real,
        policy: Default::default(),
    };
    let dns = DnsSystem::build_with_owned_udp(
        &config,
        egress.clone(),
        OwnedDnsQueryTasks::new(&scope.registrar(), limit),
        grants,
    )
    .unwrap();
    drop(resources.activate(deadline()).unwrap());
    (resources, Arc::new(dns))
}

#[tokio::test]
async fn cancelled_pending_node_dns_returns_pool_reservation_without_inventing_dns_close() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server = Server::new().await;
        let egress = Arc::new(EgressInterfaceControl::default());
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(4).unwrap(),
        );
        let (resources, dns) = owned_dns(&scope, peer.local_addr().unwrap(), &egress);
        let fixture = Assembly::with_dns(server.address, egress, Some(dns.clone()));
        let mut config = (*fixture.config()).clone();
        config.outbounds.truncate(1);
        config.outbounds[0].dial.address_family = zero_config::OutboundAddressFamily::OnlyIpv4;
        if let OutboundProtocolConfig::Mieru { server, .. } = &mut config.outbounds[0].protocol {
            *server = "pending.example".into();
        }
        let config = Arc::new(config);
        fixture
            .catalog
            .drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let mut opening = Box::pin(fixture.try_open(&slot));
                let mut bytes = [0; 4096];
                tokio::select! {
                    _ = opening.as_mut() => panic!("actual DNS peer has sent no response"),
                    result = peer.recv_from(&mut bytes) => { result.unwrap(); },
                }
                std::future::poll_fn(|cx| {
                    assert!(opening.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                assert_eq!(fixture.pool.snapshot().connections, 0);
                assert_eq!(server.accepts.load(Ordering::SeqCst), 0);
                drop(opening);
                fixture.catalog.shutdown(deadline()).await.unwrap();
                assert_eq!(fixture.pool.snapshot().connections, 0);
                // This was an unchanged, separately owned DNS dependency. Pool
                // shutdown has no authority to confirm that provider's originals.
                assert!(!resources.io_evidence().physical_confirmed());
            })
            .await;
        let closing = dns.shutdown_queries(deadline());
        tokio::pin!(closing);
        loop {
            tokio::select! {
                result = &mut closing => { result.unwrap(); break; },
                exit = scope.next_exit() => { assert!(exit.unwrap().result.is_ok()); },
            }
        }
        assert!(scope.shutdown(deadline()).await.is_complete());
        let report = resources.shutdown_io(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.closed_io.len(), 1);
        assert!(resources.io_evidence().physical_confirmed());
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_node_dns_retries_only_after_original_candidate_close_and_preserves_the_domain() {
    tokio::time::timeout(Duration::from_secs(15), async {
        // TCP cannot connect to the limited broadcast address. Unlike an
        // unbound/free port or an unlistened socket, this does not assume a
        // particular OS refusal policy or an endpoint remaining unoccupied.
        let server = Server::tcp("[::1]:0").await;
        let dns_peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let egress = Arc::new(EgressInterfaceControl::default());
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(4).unwrap(),
        );
        let (resources, dns) = owned_dns(&scope, dns_peer.local_addr().unwrap(), &egress);
        let fixture = Assembly::with_dns(server.address, egress, Some(dns.clone()));
        let mut config = (*fixture.config()).clone();
        config.outbounds.truncate(1);
        if let OutboundProtocolConfig::Mieru { server, .. } = &mut config.outbounds[0].protocol {
            *server = "owned.example".into();
        }
        let config = Arc::new(config);
        let serving = tokio::spawn(async move {
            for _ in 0..2 {
                let mut bytes = [0; 4096];
                let (size, peer) = dns_peer.recv_from(&mut bytes).await.unwrap();
                let query = &bytes[..size];
                let question = zero_dns::udp::parse_dns_question(query).unwrap();
                assert_eq!(question.domain, "owned.example");
                let qtype = question.query_type;
                let addresses = match qtype {
                    1 => vec![zero_traits::IpAddress::V4([255, 255, 255, 255])],
                    28 => vec![zero_traits::IpAddress::V6(
                        std::net::Ipv6Addr::LOCALHOST.octets(),
                    )],
                    _ => panic!("actual DNS address request"),
                };
                let response = zero_dns::udp::build_dns_response(query, &addresses);
                dns_peer.send_to(&response, peer).await.unwrap();
            }
        });
        {
            let work = fixture.catalog.drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let mut stream = fixture.open(&slot).await;
                stream.write_all(b"resolved").await.unwrap();
                let mut bytes = [0; 8];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"resolved");
                assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
                assert_eq!(fixture.pool.snapshot().connections, 1);
                drop(stream);
                fixture.catalog.shutdown(deadline()).await.unwrap();
                assert_eq!(
                    fixture.closes.load(Ordering::SeqCst),
                    1,
                    "final cleanup reports the successful original once; the failed candidate was already settled before retry"
                );
            });
            tokio::pin!(work);
            loop {
                tokio::select! {
                    ()=&mut work=>break,
                    exit=scope.next_exit()=>assert!(exit.unwrap().result.is_ok()),
                }
            }
        }
        serving.await.unwrap();
        let report = scope.shutdown(deadline()).await;
        assert!(report.is_complete(), "{report:?}");
        dns.shutdown_queries(deadline()).await.unwrap();
        let report = resources.shutdown_io(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(
            report.closed_io.len(),
            1,
            "original close reports are bounded by declaration, not query count"
        );
        assert!(report.closed_io[0].ends_with(": 2 original closes"));
        assert!(resources.io_evidence().physical_confirmed());
        server.close().await;
    })
    .await
    .unwrap();
}
