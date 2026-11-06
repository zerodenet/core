mod pool;
use super::*;
pub(super) use crate::dns_fixture::TlsDns;
#[tokio::test]
async fn actual_proxy_native_dot_query_uses_original_tcp_controller_and_host_task() {
    exchange(false, false).await;
}
#[tokio::test]
async fn actual_proxy_native_doh_queries_reuse_one_physical_original_and_host_driver() {
    exchange(true, false).await;
}
#[tokio::test]
async fn actual_proxy_native_doh_shutdown_joins_actual_pool_driver_and_confirms_original_close() {
    exchange(true, true).await;
}

#[test]
fn actual_proxy_native_encrypted_dns_runs_on_a_two_mib_worker_stack() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async {
            exchange(false, false).await;
            exchange(true, false).await;
            exchange(true, true).await;
        })
        .await
        .unwrap();
    });
}

async fn exchange(doh: bool, shutdown: bool) {
    exchange_in(doh, shutdown, true).await;
}
async fn exchange_in(doh: bool, shutdown: bool, background: bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut tls = TlsDns::new(doh).await;
        let plain = Dns::new().await;
        let mut dns = plain.config();
        dns.servers.insert("native".into(), tls.server(doh));
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns);
        let (proxy, running, _) = if background {
            start(config).await
        } else {
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config, Duration::from_secs(5))
                .await
                .unwrap();
            (proxy, running, handle)
        };
        if doh {
            zero_dns::test_support::install_doh_ca(
                &proxy.resolver,
                "native",
                tls.certificate.clone(),
            )
            .unwrap();
        } else {
            zero_dns::test_support::install_dot_ca(
                &proxy.resolver,
                "native",
                tls.certificate.clone(),
            )
            .unwrap();
        }
        let querying = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("encrypted.node.test").await })
        };
        let (query, reply) = tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        let suspended = pending(&proxy).await;
        let prior = original(&suspended);
        assert!(prior.closed().is_none());
        if shutdown {
            running.shutdown().await.unwrap();
            assert!(querying.await.unwrap().is_err());
        } else {
            reply
                .send(zero_dns::udp::build_dns_response(
                    &query,
                    &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
                ))
                .unwrap();
            querying.await.unwrap().unwrap();
            if doh {
                assert!(
                    !suspended.task.is_joined(),
                    "the healthy physical driver is reused"
                );
                assert!(prior.closed().is_none());
                let witness =
                    zero_dns::test_support::doh_pool_witness(&proxy.resolver, "native").unwrap();
                assert_eq!(witness.registered_driver_tasks, 1);
                assert_eq!(witness.active_leases, 0);
                let next = tokio::spawn({
                    let resolver = proxy.resolver.clone();
                    async move { resolver.resolve_node("reused.encrypted.node.test").await }
                });
                let (query, reply) =
                    tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
                        .await
                        .unwrap()
                        .unwrap();
                let same = pending(&proxy).await;
                assert!(original(&same).same_original(&prior));
                assert!(same.task.same_registration(&suspended.task));
                assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
                reply
                    .send(zero_dns::udp::build_dns_response(
                        &query,
                        &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
                    ))
                    .unwrap();
                next.await.unwrap().unwrap();
            } else {
                suspended.task.wait().await;
                closed(&prior);
            }
            running.shutdown().await.unwrap();
        }
        assert!(suspended.task.is_joined());
        closed(&prior);
        assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        tls.close().await;
        plain.close().await;
    })
    .await
    .unwrap();
}
