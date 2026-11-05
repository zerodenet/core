use super::*;
use zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin;
mod limits;
mod open_failure;
mod peer;
mod transition;
use peer::Peer;
async fn pending_quic(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(query) =
                proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|query| {
                        query.io_pending && query.metadata.is_some() && !query.task.is_joined()
                    })
            {
                return query;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn confirmed(original: &MetadataPoolOrigin) {
    let receipt = original
        .closed()
        .expect("same original native metadata close");
    assert!(receipt.matches(original) && receipt.had_native_acquisition());
}
fn query(
    proxy: &Proxy,
    name: &str,
) -> tokio::task::JoinHandle<std::io::Result<Vec<zero_traits::IpAddress>>> {
    let resolver = proxy.resolver.clone();
    let name = name.to_owned();
    tokio::spawn(async move { resolver.resolve_node(&name).await })
}
async fn configured(peer: &Peer) -> (Proxy, crate::RunningProxy, ProxyHandle) {
    let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
    config.runtime.dns = Some(peer.config());
    let (proxy, running, handle) = start(config).await;
    peer.trust(&proxy);
    (proxy, running, handle)
}
#[tokio::test]
async fn actual_proxy_native_doq_uses_real_tls_scope_original_and_reclaims_closed_query_custody() {
    let mut peer = Peer::new(false).await;
    let (proxy, running, _) = configured(&peer).await;
    let job = query(&proxy, "original.quic.test");
    let request = peer.request().await;
    let held = pending_quic(&proxy).await;
    let original = held.metadata.clone().unwrap();
    let (local, remote) = original.endpoints().unwrap();
    assert_eq!(remote, peer.address);
    assert_eq!(local.port(), request.client.port());
    assert!(local.ip().is_unspecified() || local.ip() == request.client.ip());
    assert!(original.closed().is_none());
    assert!(held.original.is_none());
    request.answer([198, 51, 100, 91]);
    assert_eq!(
        job.await.unwrap().unwrap(),
        vec![zero_traits::IpAddress::V4([198, 51, 100, 91])]
    );
    held.task.wait().await;
    confirmed(&original);
    let next = query(&proxy, "next.quic.test");
    let request = peer.request().await;
    confirmed(&original);
    assert!(original.endpoints().is_none());
    let current = pending_quic(&proxy).await;
    let fresh = current.metadata.clone().unwrap();
    assert!(!original.same_original(&fresh));
    assert!(original.owner().same_owner(fresh.owner()));
    request.answer([198, 51, 100, 92]);
    next.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    confirmed(&fresh);
    assert!(original.owner().closed().unwrap().matches(original.owner()));
    peer.close().await;
}
#[tokio::test]
async fn actual_proxy_native_doq_domain_mieru_uses_the_one_real_shared_pool() {
    let mut peer = Peer::new(false).await;
    let a = Server::new(true).await;
    let b = Server::new(true).await;
    let c = Server::new(false).await;
    let mut config = config([a.address, b.address, c.address]);
    config.runtime.dns = Some(peer.config());
    if let zero_config::OutboundProtocolConfig::Mieru { server, .. } =
        &mut config.outbounds[0].protocol
    {
        *server = "native.quic.node.test".into();
    }
    let (proxy, running, _) = start(config).await;
    peer.trust(&proxy);
    let connecting = tokio::spawn({
        let proxy = proxy.clone();
        async move { service(&proxy).await }
    });
    let request = peer.request().await;
    let held = pending_quic(&proxy).await;
    assert_eq!(a.accepts.load(Ordering::SeqCst), 0);
    request.answer([127, 0, 0, 1]);
    let mut stream = connecting.await.unwrap();
    confirmed(&held.metadata.unwrap());
    stream.write_all(b"native DoQ").await.unwrap();
    stream.read_exact(&mut [0; 10]).await.unwrap();
    drop(stream);
    running.shutdown().await.unwrap();
    a.close().await;
    b.close().await;
    c.close().await;
    peer.close().await;
}
#[tokio::test]
async fn actual_proxy_native_doq_cancelled_pending_read_closes_original_before_next_endpoint_open()
{
    let mut peer = Peer::new(false).await;
    let (proxy, running, _) = configured(&peer).await;
    let job = query(&proxy, "cancelled.quic.test");
    let request = peer.request().await;
    let held = pending_quic(&proxy).await;
    let original = held.metadata.clone().unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    held.task.wait().await;
    let next = query(&proxy, "after-cancel.quic.test");
    let response = peer.request().await;
    confirmed(&original);
    assert!(held.task.is_joined());
    response.answer([198, 51, 100, 91]);
    next.await.unwrap().unwrap();
    drop(request);
    running.shutdown().await.unwrap();
    confirmed(&original);
    peer.close().await;
}
#[test]
fn actual_proxy_native_doq_queries_and_original_close_run_on_a_two_mib_worker() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async {
            let mut peer = Peer::new(false).await;
            let (proxy, running, _) = configured(&peer).await;
            let job = query(&proxy, "small-stack.quic.test");
            let request = peer.request().await;
            let held = pending_quic(&proxy).await;
            request.answer([198, 51, 100, 91]);
            job.await.unwrap().unwrap();
            running.shutdown().await.unwrap();
            confirmed(&held.metadata.unwrap());
            peer.close().await;
        })
        .await
        .unwrap();
    });
}
