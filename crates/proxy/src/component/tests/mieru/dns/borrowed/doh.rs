use super::super::encrypted::TlsDns;
use super::*;

#[tokio::test]
async fn actual_doh_driver_survives_query_cancellation_and_reuses_one_mieru_loan() {
    doh(false).await;
}

#[tokio::test]
async fn actual_doh_network_replacement_joins_old_dns_and_mieru_originals_before_reopen() {
    doh(true).await;
}

async fn doh(network_change: bool) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let plain = Dns::new().await;
        let mut tls = TlsDns::new(true).await;
        let server = Server::new(true).await;
        let mut settings = detoured(&plain, &server);
        let mut backend = tls.server(true);
        let zero_config::DnsServerConfig::Doh { detour, .. } = &mut backend else {
            unreachable!()
        };
        *detour = Some("a".into());
        settings
            .runtime
            .dns
            .as_mut()
            .unwrap()
            .servers
            .insert("query".into(), backend);
        let (proxy, running, handle) = start(settings.clone()).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone())
            .unwrap();
        let query = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move {
                resolver
                    .resolve_real_type("first.doh.borrowed.test", 1)
                    .await
            }
        });
        let (bytes, answer) = tokio::time::timeout_at(deadline(), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        let old = pending(&proxy).await;
        let child = old.original.clone().unwrap();
        let donor = child.parent().unwrap().clone();
        let native = donor.native_origin().clone();
        answer
            .send(zero_dns::udp::build_dns_response(
                &bytes,
                &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
            ))
            .unwrap();
        query.await.unwrap().unwrap();
        let warm = zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap();
        assert_eq!(warm.registered_driver_tasks, 1);
        assert_eq!(warm.active_leases, 0);
        assert!(!old.task.is_joined());
        assert!(child.closed().is_none());
        assert!(native.closed().is_none());
        handle
            .apply_runtime_config_and_wait(settings, Duration::from_secs(5))
            .await
            .unwrap();
        let query = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move {
                resolver
                    .resolve_real_type("pending.doh.borrowed.test", 1)
                    .await
            }
        });
        let (_, answer) = tokio::time::timeout_at(deadline(), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        let still = pending(&proxy).await;
        assert!(still.task.same_registration(&old.task));
        assert!(still.original.as_ref().unwrap().same_original(&child));
        assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        assert_eq!(tls.connections.load(Ordering::SeqCst), 1);
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        drop(answer);
        if network_change {
            proxy.egress_interface_control().invalidate_network();
        }
        let query = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move {
                resolver
                    .resolve_real_type("next.doh.borrowed.test", 1)
                    .await
            }
        });
        let (bytes, answer) = tokio::time::timeout_at(deadline(), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        let next = pending(&proxy).await;
        if network_change {
            assert!(old.task.is_joined());
            assert!(child.closed().unwrap().matches(&child));
            assert!(!child.closed().unwrap().had_native_acquisition());
            assert!(native.driver().unwrap().is_joined());
            assert!(native.closed().unwrap().matches(&native));
            assert!(native.closed().unwrap().had_native_acquisition());
            assert!(!next.task.same_registration(&old.task));
            assert_eq!(server.accepts.load(Ordering::SeqCst), 2);
            assert_eq!(tls.connections.load(Ordering::SeqCst), 2);
        } else {
            assert!(next.task.same_registration(&old.task));
            assert!(next.original.as_ref().unwrap().same_original(&child));
            assert!(!native.driver().unwrap().is_joined());
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
            assert_eq!(tls.connections.load(Ordering::SeqCst), 1);
        }
        answer
            .send(zero_dns::udp::build_dns_response(
                &bytes,
                &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
            ))
            .unwrap();
        query.await.unwrap().unwrap();
        let current = next.original.as_ref().unwrap();
        let actual = current.native_origin();
        running.shutdown().await.unwrap();
        assert!(next.task.is_joined());
        assert!(current.closed().unwrap().matches(current));
        assert!(!current.closed().unwrap().had_native_acquisition());
        assert!(actual.driver().unwrap().is_joined());
        assert!(actual.closed().unwrap().matches(actual));
        assert!(actual.closed().unwrap().had_native_acquisition());
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        server.close().await;
        tls.close().await;
        plain.close().await;
    })
    .await
    .unwrap();
}
