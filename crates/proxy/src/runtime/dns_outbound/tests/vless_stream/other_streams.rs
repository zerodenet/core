//! Existing VMess and Trojan transports share the same configured native owner.
use super::*;
pub(super) const CASES: [(&str, &str); 4] = [
    ("vmess", "raw"),
    ("vmess", "ws"),
    ("trojan", "grpc-single"),
    ("trojan", "grpc-multi"),
];
#[tokio::test]
async fn ordinary_vmess_and_trojan_dns_consumes_real_stream_loans_and_persistent_doh() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for (protocol, kind) in CASES {
            let remote = Remote::for_protocol(protocol, kind).await;
            let mut dns = crate::dns_fixture::Dns::new().await;
            let (proxy, running) = start(&remote, kind, dns.server()).await;
            let mut originals = Vec::new();
            for (index, cancel) in [false, true, false].into_iter().enumerate() {
                let mut query = query(&proxy, &format!("{index}.{protocol}.{kind}.dns.test"));
                let request = tokio::select! {
                    request = dns.request() => request,
                    result = &mut query => panic!("ordinary {protocol}/{kind} DNS failed before framed wire: {result:?}"),
                };
                assert!(request.tcp);
                let caller = pending(&proxy).await;
                let child = caller.original.as_ref().unwrap();
                let original = donor(&proxy, child);
                if cancel {
                    query.abort();
                    assert!(query.await.unwrap_err().is_cancelled());
                    drop(request);
                } else {
                    request.answer(false, false);
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127, 0, 0, 1])]);
                }
                let joined = tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap();
                assert!(caller.task.matches_joined(&joined)); joined.result().unwrap();
                returned(child); native_closed(&original);
                for old in &originals { assert!(!original.same_original(old)); }
                originals.push(original);
            }
            running.shutdown().await.unwrap();
            for original in &originals {
                assert!(original.owner().closed().unwrap().matches(original.owner()));
                assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            }
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            dns.close().await;

            let mut tls = crate::dns_fixture::TlsDns::new(true).await;
            let (proxy, running) = start(&remote, kind, tls.server(true)).await;
            zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
            let mut first: Option<zero_dns::OwnedDnsQueryObservation> = None;
            for (index, cancel) in [false, true, false].into_iter().enumerate() {
                let mut query = query(&proxy, &format!("{index}.{protocol}.{kind}.doh.test"));
                let (bytes, answer) = tokio::select! {
                    request = tokio::time::timeout_at(deadline(), tls.requests.recv()) => request.unwrap().unwrap(),
                    result = &mut query => panic!("ordinary {protocol}/{kind} DoH failed before HTTP wire: {result:?}"),
                };
                let actual = pending(&proxy).await;
                let child = actual.original.as_ref().unwrap(); let original = donor(&proxy, child);
                if let Some(first) = &first {
                    assert!(actual.task.same_registration(&first.task));
                    assert!(child.same_original(first.original.as_ref().unwrap()));
                }
                if cancel {
                    query.abort(); assert!(query.await.unwrap_err().is_cancelled()); drop(answer);
                } else {
                    answer.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127, 0, 0, 1])])).unwrap();
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127, 0, 0, 1])]);
                }
                tokio::time::timeout_at(deadline(), async {
                    loop {
                        let pool = zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap();
                        assert_eq!(pool.registered_driver_tasks, 1);
                        if pool.active_leases == 0 { break; }
                        tokio::task::yield_now().await;
                    }
                }).await.unwrap();
                assert!(child.closed().is_none() && original.closed().is_none() && !actual.task.is_joined());
                assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
                first = Some(actual);
            }
            let first = first.unwrap(); let child = first.original.as_ref().unwrap(); let original = donor(&proxy, child);
            running.shutdown().await.unwrap(); returned(child); native_closed(&original);
            let joined = first.task.try_joined().unwrap();
            assert!(first.task.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
            assert!(original.owner().closed().unwrap().matches(original.owner()));
            assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            tls.close().await; remote.close().await;
        }
    }).await.unwrap();
}
#[tokio::test]
async fn ordinary_vmess_and_trojan_first_tls_pending_cancel_joins_the_actual_preparation_and_native()
 {
    tokio::time::timeout(Duration::from_secs(15), async {
        for (protocol, kind) in CASES {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let material = tempfile::tempdir().unwrap();
            let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let ca = material.path().join("ca.pem");
            std::fs::write(&ca, certified.cert.pem()).unwrap();
            let mut dns = crate::dns_fixture::Dns::new().await;
            let (proxy, running) = start_protocol_at(
                protocol,
                listener.local_addr().unwrap(),
                &ca,
                kind,
                dns.server(),
            )
            .await;
            let mut query = query(&proxy, &format!("pending.{protocol}.{kind}.dns.test"));
            let (mut wire, _) = tokio::select! {
                accepted = listener.accept() => accepted.unwrap(),
                result = &mut query => panic!("{protocol}/{kind} failed before TLS: {result:?}"),
            };
            let mut header = [0; 5];
            wire.read_exact(&mut header).await.unwrap();
            assert_eq!(&header[..2], &[22, 3]);
            let mut hello = vec![0; u16::from_be_bytes([header[3], header[4]]) as usize];
            wire.read_exact(&mut hello).await.unwrap();
            assert_eq!(hello[0], 1);
            assert!(!query.is_finished());
            let caller = proxy
                .resolver
                .native_query_observations()
                .into_iter()
                .find(|call| !call.task.is_joined())
                .unwrap();
            assert!(caller.original.is_none());
            let plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let slot = plan.configured("node").unwrap();
            let owner = (0..slot.recipe.spec().resources.len())
                .filter_map(|ordinal| slot.original_carrier_owner(ordinal))
                .find(|owner| owner.observe().is_some_and(|state| state.retained() == 1))
                .unwrap();
            let held = owner.observe().unwrap();
            let held = &held.slots()[0];
            assert!(held.driver().is_none());
            let auxiliary = held.auxiliary().unwrap().clone();
            assert!(
                auxiliary
                    .scope_identity()
                    .same_scope(&owner.bound().unwrap().scope_identity())
            );
            assert!(!auxiliary.is_joined() && !auxiliary.same_registration(&caller.task));
            let original = held.original().clone();
            assert!(original.closed().is_none());
            query.abort();
            assert!(query.await.unwrap_err().is_cancelled());
            tokio::time::timeout_at(deadline(), caller.task.wait())
                .await
                .unwrap()
                .result()
                .unwrap();
            let joined =
                tokio::time::timeout_at(deadline(), auxiliary.wait_registration_released())
                    .await
                    .unwrap();
            assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
            let closed = original.closed().unwrap();
            assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
            assert_eq!(owner.observe().unwrap().retained(), 0);
            let mut remaining = Vec::new();
            tokio::time::timeout_at(deadline(), wire.read_to_end(&mut remaining))
                .await
                .unwrap()
                .unwrap();
            dns.no_new_request();
            running.shutdown().await.unwrap();
            assert!(
                owner.closed().unwrap().matches(&owner)
                    && owner.users_closed().unwrap().matches(&owner)
            );
            assert!(
                proxy.engine().active_sessions().is_empty()
                    && proxy.engine().completed_sessions().is_empty()
            );
            dns.close().await;
        }
    })
    .await
    .unwrap();
}
