use super::*;
use zero_dns::native::NativeDnsQuicOpenFailure;

fn failure<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> Option<&'a NativeDnsQuicOpenFailure> {
    if let Some(failure) = source.downcast_ref::<NativeDnsQuicOpenFailure>() {
        return Some(failure);
    }
    if let Some(inner) = source
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        if let Some(found) = failure(inner) {
            return Some(found);
        }
    }
    source.source().and_then(failure)
}
#[tokio::test]
async fn failed_doq_tls_open_retains_its_original_metadata_close_before_fallback() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let peer = Peer::new(false).await;
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(peer.config());
        let (proxy, running, handle) = start(config.clone()).await;
        // The real peer certificate is deliberately untrusted. The failed TLS
        // handshake must clean the actual endpoint; an error kind is no proof.
        let error = proxy
            .resolver
            .resolve_node("untrusted.quic.test")
            .await
            .unwrap_err();
        let closed = failure(&error)
            .unwrap_or_else(|| panic!("lost original failed-open receipt: {error:?}"));
        assert!(closed.original_close().matches(closed.original()));
        assert!(closed.original_close().had_native_acquisition());
        confirmed(closed.original());
        let settings = config.runtime.dns.as_mut().unwrap();
        settings.servers.insert("fallback".into(), dns.server());
        settings.policy.node_fallback_servers = vec!["fallback".into()];
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        let querying = query(&proxy, "fallback.quic.test");
        let response = dns.request().await;
        // Reaching this peer required the failed DoQ original's matching
        // provider receipt and actual Quinn driver joins in that same query.
        response.answer_ips(
            false,
            false,
            &[zero_traits::IpAddress::V4([198, 51, 100, 93])],
        );
        assert_eq!(
            querying.await.unwrap().unwrap(),
            vec![zero_traits::IpAddress::V4([198, 51, 100, 93])]
        );
        running.shutdown().await.unwrap();
        dns.close().await;
        peer.close().await;
    })
    .await
    .unwrap();
}
