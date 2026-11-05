#![cfg(feature = "dns")]
use super::*;
use tokio::net::TcpListener;
use zero_platform_tokio::contract::Reason;

#[cfg(feature = "dns")]
#[tokio::test]
async fn ordinary_proxy_failed_domain_resolution_closes_its_original_before_returning_without_a_mieru_dial(
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config([listener.local_addr().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        if let zero_config::OutboundProtocolConfig::Mieru { server, .. } =
            &mut config.outbounds[0].protocol
        {
            *server = "failed.node.test".into();
        }
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
        let opening = {
            let proxy = proxy.clone();
            tokio::spawn(async move { try_service(&proxy).await })
        };
        let request = dns.request().await;
        let query = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(query) = proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|query| query.io_pending && query.original.is_some())
                {
                    break query;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let original = query.original.unwrap();
        assert!(original.closed().is_none());
        request.answer(false, true);
        let error = match opening.await.unwrap() {
            Ok(_) => panic!("failed DNS response must reject carrier preparation"),
            Err(error) => error,
        };
        assert_eq!(error.reason, Reason::Io, "{error}");
        query.task.wait().await;
        assert!(original.closed().unwrap().matches(&original));
        assert!(original.closed().unwrap().had_native_acquisition());
        std::future::poll_fn(|cx| {
            assert!(
                listener.poll_accept(cx).is_pending(),
                "failed resolution must not open the Mieru donor"
            );
            std::task::Poll::Ready(())
        })
        .await;
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}
