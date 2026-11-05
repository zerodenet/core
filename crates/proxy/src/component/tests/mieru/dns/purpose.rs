//! The two actual purposes share capacity and a network retirement barrier.
use super::*;

#[tokio::test]
async fn node_and_default_query_owners_share_the_original_and_task_capacity_until_actual_join() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = Dns::new().await;
        let mut settings = config(["127.0.0.1:1".parse().unwrap(); 3]);
        settings.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(settings).await;
        let mut jobs = Vec::new();
        let mut requests = Vec::new();
        let mut held = Vec::new();
        for index in 0..crate::component::native_dns::LIMIT {
            let resolver = proxy.resolver.clone();
            jobs.push(tokio::spawn(async move {
                if index % 2 == 0 {
                    resolver
                        .resolve_node(&format!("node-{index}.purpose.test"))
                        .await
                } else {
                    resolver
                        .resolve_real_type(&format!("default-{index}.purpose.test"), 1)
                        .await
                }
            }));
            requests.push(dns.request().await);
            let prior = tokio::time::timeout_at(deadline(), async {
                loop {
                    if let Some(actual) = proxy
                        .resolver
                        .native_query_observations()
                        .into_iter()
                        .find(|query| {
                            query.io_pending
                                && query.original.is_some()
                                && !held.iter().any(|old: &zero_dns::OwnedDnsQueryObservation| {
                                    old.task.same_registration(&query.task)
                                })
                        })
                    {
                        break actual;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            held.push(prior);
        }
        assert!(!held[0]
            .task
            .scope_identity()
            .same_scope(&held[1].task.scope_identity()));
        for (index, actual) in held.iter().enumerate() {
            assert!(actual
                .task
                .scope_identity()
                .same_scope(&held[index % 2].task.scope_identity()));
            assert!(actual.original.as_ref().unwrap().closed().is_none());
        }
        for node in [false, true] {
            let error = if node {
                proxy
                    .resolver
                    .resolve_node("rejected-node.purpose.test")
                    .await
            } else {
                proxy
                    .resolver
                    .resolve_real_type("rejected-default.purpose.test", 1)
                    .await
            }
            .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
            dns.no_new_request();
        }
        let cancelled = jobs.remove(0);
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        held[0].task.wait().await;
        closed(held[0].original.as_ref().unwrap());
        let next = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move {
                resolver
                    .resolve_real_type("released-default.purpose.test", 1)
                    .await
            }
        });
        let request = dns.request().await;
        assert!(held[0].task.is_joined());
        closed(held[0].original.as_ref().unwrap());
        assert!(held[1..]
            .iter()
            .all(|query| !query.task.is_joined()
                && query.original.as_ref().unwrap().closed().is_none()));
        request.answer(false, false);
        next.await.unwrap().unwrap();
        running.shutdown().await.unwrap();
        for job in jobs {
            assert!(job.await.unwrap().is_err());
        }
        for query in held {
            assert!(query.task.is_joined());
            closed(query.original.as_ref().unwrap());
        }
        drop(requests);
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn new_default_network_query_joins_and_closes_both_old_purpose_owners_before_open() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let mut settings = config(["127.0.0.1:1".parse().unwrap(); 3]);
        settings.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(settings).await;
        let old_node = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move { resolver.resolve_node("old-node.purpose.test").await }
        });
        let node_request = dns.request().await;
        let node = pending(&proxy).await;
        let old_default = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move {
                resolver
                    .resolve_real_type("old-default.purpose.test", 1)
                    .await
            }
        });
        let default_request = dns.request().await;
        let old = proxy.resolver.native_query_observations();
        assert_eq!(old.len(), 2);
        assert!(old.iter().any(|query| !query
            .task
            .scope_identity()
            .same_scope(&node.task.scope_identity())));
        proxy.egress_interface_control().invalidate_network();
        let next = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move {
                resolver
                    .resolve_real_type("new-default.purpose.test", 1)
                    .await
            }
        });
        let request = dns.request().await;
        for query in &old {
            assert!(query.task.is_joined());
            closed(query.original.as_ref().unwrap());
        }
        assert!(old_node.await.unwrap().is_err());
        assert!(old_default.await.unwrap().is_err());
        request.answer(false, false);
        next.await.unwrap().unwrap();
        running.shutdown().await.unwrap();
        drop((node_request, default_request));
        dns.close().await;
    })
    .await
    .unwrap();
}
