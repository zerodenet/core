use super::*;

#[tokio::test]
async fn actual_proxy_native_dns_worker_panic_keeps_its_actual_scope_failure_and_closes_original_io(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(config).await;
        let query = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("pending-at-fault.node.test").await })
        };
        let request = dns.request().await;
        let suspended = pending(&proxy).await;
        let prior = original(&suspended);
        let fault = zero_dns::test_support::panic_native_query_worker(&proxy.resolver).unwrap();
        assert!(fault
            .scope_identity()
            .same_scope(&suspended.task.scope_identity()));
        let joined = fault.wait().await;
        let source = joined.result().unwrap_err();
        assert!(
            running.shutdown().await.is_err(),
            "the actual critical worker failure remains an error"
        );
        assert!(query.await.unwrap().is_err());
        assert!(suspended.task.is_joined());
        assert!(Arc::ptr_eq(
            &source,
            &fault.try_joined().unwrap().result().unwrap_err()
        ));
        closed(&prior);
        assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        drop(request);
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_native_dns_network_change_completes_the_other_coordinators_cancelled_flight()
{
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(config).await;
        let mut wire = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
        for label in ["old-wire", "node", "test"] {
            wire.push(label.len() as u8);
            wire.extend_from_slice(label.as_bytes());
        }
        wire.extend_from_slice(&[0, 0, 1, 0, 1]);
        let old = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.answer_udp_query(&wire).await })
        };
        let request = dns.request().await;
        let suspended = pending(&proxy).await;
        let prior = original(&suspended);
        proxy.egress_interface_control().invalidate_network();
        let new = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("new-name.node.test").await })
        };
        let fresh = dns.request().await;
        assert!(suspended.task.is_joined());
        closed(&prior);
        let response = old.await.unwrap().unwrap();
        assert_eq!(
            response[3] & 15,
            2,
            "original wire waiter receives SERVFAIL after real cancellation"
        );
        fresh.answer(false, false);
        new.await.unwrap().unwrap();
        drop(request);
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_native_dns_reclaims_completed_original_custody_instead_of_retaining_a_history(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(config).await;
        let mut previous = None;
        for index in 0..32 {
            let query = {
                let resolver = proxy.resolver.clone();
                tokio::spawn(async move {
                    resolver
                        .resolve_node(&format!("retirement-{index}.node.test"))
                        .await
                })
            };
            let request = dns.request().await;
            if let Some(prior) = previous.take() {
                let prior: zero_platform_tokio::controlled_carrier::CarrierOrigin = prior;
                closed(&prior);
                assert!(
                    prior.endpoints().is_none(),
                    "the actual completed private slot is no longer retained"
                );
            }
            let suspended = pending(&proxy).await;
            let prior = original(&suspended);
            // Alternate cancellation and normal completion, both with real I/O
            // pending before the transition. Repeated cancellations also must
            // retire their actual original controllers before later opens.
            if index % 2 == 0 {
                query.abort();
                assert!(query.await.unwrap_err().is_cancelled());
            } else {
                request.answer(false, false);
                query.await.unwrap().unwrap();
            }
            suspended.task.wait().await;
            previous = Some(prior);
        }
        running.shutdown().await.unwrap();
        let last = previous.unwrap();
        closed(&last);
        assert!(last.endpoints().is_none());
        dns.close().await;
    })
    .await
    .unwrap();
}
