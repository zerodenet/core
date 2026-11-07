//! One live Business caller retries only a genuinely refused native address.
use super::fixture;
use std::{net::SocketAddr, time::Duration};
use zero_platform_tokio::contract::component::ResourceDeclaration;
use zero_traits::IpAddress;
#[tokio::test]
async fn ordinary_grpc_domain_refusal_closes_same_setup_before_next_original_with_business_n1() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for kind in ["grpc-single", "grpc-multi"] {
            let directory = tempfile::tempdir().unwrap();
            let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert = directory.path().join("server.pem");
            let key = directory.path().join("server.key");
            std::fs::write(&cert, certificate.cert.pem()).unwrap();
            std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
            let remote = fixture::server(kind, &cert, &key).await;
            let refused = tokio::net::TcpSocket::new_v4().unwrap();
            refused.bind(SocketAddr::new("127.0.0.2".parse().unwrap(), remote.address.port())).unwrap();
            let mut node = crate::dns_fixture::Dns::new().await;
            let client = fixture::start(serde_json::json!({
                "runtime":{"vmess_grpc_max_originals":1,"dns":{"servers":{"node":{"type":"udp","host":"127.0.0.1","port":node.address.port()}},"default_server":"node"}},
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                "outbounds":[{"tag":"node","protocol":{"type":"vmess","server":"node.example","port":remote.address.port(),
                    "id":"11111111-2222-3333-4444-555555555555","tls":{"server_name":"localhost","ca_cert_path":cert,"client_fingerprint":null},
                    "grpc":{"service_names":["original/Tun"],"multi_mode":kind=="grpc-multi"}}}],
                "route":{"final":{"type":"route","outbound":"node"}}
            }), "entry").await;
            let (owner, original, failed, first_setup) = {
                let opening = fixture::echo_with(&client, || {
                    let (owner, original) = fixture::capture(&client.proxy);
                    let observed = owner.observe().unwrap();
                    assert_eq!(observed.retained(), 1);
                    assert_eq!(observed.slots()[0].ordinal(), 2);
                    (owner, original)
                });
                tokio::pin!(opening);
                let bootstrap = tokio::select! {
                    query = node.request() => query,
                    result = &mut opening => panic!("ordinary Business ended before Node response: {}", result.0.instance().component.0),
                };
                let services = client.proxy.tcp_runtime_services();
                let plan = services.components.as_ref().unwrap();
                let slot = plan.configured("node").unwrap();
                assert_eq!(slot.recipe.spec().task_limit.unwrap().get(), 1);
                let owner = slot.recipe.spec().resources.iter().enumerate().find_map(|(ordinal, declaration)| {
                    matches!(declaration, ResourceDeclaration::ControlledNodeStream { .. })
                        .then(|| slot.original_carrier_owner(ordinal).unwrap())
                }).unwrap();
                let observed = owner.observe().unwrap();
                assert_eq!(observed.retained(), 1);
                let first = &observed.slots()[0];
                assert_eq!(first.ordinal(), 1);
                assert!(first.native().is_none() && first.driver().is_none());
                let failed = first.original().clone();
                let first_setup = first.auxiliary().unwrap().clone();
                assert!(!first_setup.is_joined());
                bootstrap.answer_ips(false, false, &[IpAddress::V4([127,0,0,2]), IpAddress::V4([127,0,0,1])]);
                let (same, original) = opening.await;
                assert!(owner.same_owner(&same));
                (owner, original, failed, first_setup)
            };
            let closed = failed.closed().expect("same failed native actually closed before retry");
            assert!(failed.matches_closed(&closed) && closed.had_native_acquisition());
            let joined = first_setup.try_joined().unwrap();
            assert!(first_setup.matches_joined(&joined) && joined.registration_released());
            assert!(joined.result().is_err(), "the exact refused setup remains an accepted failure");
            node.no_new_request();
            fixture::wait_returned(&owner, &original).await;
            client.close().await;
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            remote.close().await;
            node.close().await;
            drop(refused);
        }
    }).await.expect("original Domain refusal and complete Business halfclose deadline");
}
