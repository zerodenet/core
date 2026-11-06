//! Type65 borrows the actual Node Scope, rather than a detached ECH worker.
use super::*;
use std::time::Duration;
use zero_platform_tokio::contract::dns::DnsService;

#[tokio::test]
async fn ordinary_ech_refresh_at_one_query_retains_original_until_success_and_pending_cancel() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server = format!("udp://{}", peer.local_addr().unwrap());
        let config = config(&server);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(3))
            .await
            .unwrap();
        for cancel in [false, true] {
            let (answer, result) = tokio::sync::oneshot::channel();
            let refresh = proxy
                .resolver
                .spawn_ech_refresh("cover.ech.test".into(), server.clone(), move |value| {
                    Box::pin(async move {
                        let _ = answer.send(value);
                    })
                })
                .unwrap();
            let mut wire = vec![0; 65536];
            let (size, source) = peer.recv_from(&mut wire).await.unwrap();
            wire.truncate(size);
            assert_eq!(&wire[size - 4..], &[0, 65, 0, 1]);
            let observed = loop {
                if let Some(observed) =
                    proxy
                        .resolver
                        .native_query_observations()
                        .into_iter()
                        .find(|observation| {
                            observation.task.same_registration(&refresh.task)
                                && observation.original.is_some()
                        })
                {
                    break observed;
                }
                tokio::task::yield_now().await;
            };
            let original = observed.original.unwrap();
            assert!(original.closed().is_none());
            assert_eq!(refresh.generation, proxy.resolver.generation());
            if cancel {
                refresh.cancel.cancel();
                assert!(
                    result.await.is_err(),
                    "cancel drops the same job's unpublished completion"
                );
            } else {
                let response = zero_dns::udp::build_dns_response(&wire, &[]);
                peer.send_to(&response, source).await.unwrap();
                assert_eq!(result.await.unwrap().unwrap().config_list, None);
            }
            refresh.task.wait().await.result().unwrap();
            let closed = original
                .closed()
                .expect("the exact actual ECH native original closed before its Query join");
            assert!(closed.matches(&original) && closed.had_native_acquisition());
        }
        let (answer, result) = tokio::sync::oneshot::channel();
        let unauthorized = proxy
            .resolver
            .spawn_ech_refresh(
                "cover.ech.test".into(),
                "udp://127.0.0.1:9".into(),
                move |value| {
                    Box::pin(async move {
                        let _ = answer.send(value);
                    })
                },
            )
            .unwrap();
        assert_eq!(
            result.await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        unauthorized.task.wait().await.result().unwrap();
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
fn config(server: &str) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"native_dns_max_queries":1},
        "outbounds":[{"tag":"ech-node", "protocol":{"type":"vless", "server":"127.0.0.1", "port":443,
            "id":"00112233-4455-6677-8899-aabbccddeeff", "tls":{"server_name":"cover.ech.test", "options":{"ech_config_list":server}}}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
