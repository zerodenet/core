use super::*;
use std::io;

#[test]
fn pool_ownership_is_constructed_without_a_runtime_or_task() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let resolver = DohDnsResolver::new_cleartext(
        "127.0.0.1".into(),
        listener.local_addr().unwrap().port(),
        "/dns-query".into(),
        vec![],
        EgressInterfaceControl::default(),
    )
    .unwrap();
    assert!(resolver.owner.drained());
}

async fn closing_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await.unwrap();
        while let Some(request) = connection.accept().await {
            let (request, mut respond) = request.unwrap();
            let mut body = request.into_body();
            let mut query = vec![];
            while let Some(chunk) = body.data().await {
                let chunk = chunk.unwrap();
                query.extend_from_slice(&chunk);
                body.flow_control().release_capacity(chunk.len()).unwrap();
            }
            let response = crate::message::build_address_response(
                &query,
                &[zero_traits::IpAddress::V4([203, 0, 113, 53])],
                30,
            );
            let mut send = respond
                .send_response(
                    http::Response::builder().status(200).body(()).unwrap(),
                    false,
                )
                .unwrap();
            send.send_data(Bytes::from(response), true).unwrap();
        }
        // Returning requires the actual H2/TCP connection to observe closure.
    });
    (endpoint, task)
}
fn local(endpoint: std::net::SocketAddr) -> Arc<DohDnsResolver> {
    Arc::new(
        DohDnsResolver::new_cleartext(
            "127.0.0.1".into(),
            endpoint.port(),
            "/dns-query".into(),
            vec![],
            EgressInterfaceControl::default(),
        )
        .unwrap(),
    )
}
fn until() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn explicit_pool_shutdown_joins_the_real_driver_and_remote_socket_closure() {
    let (endpoint, server) = closing_server().await;
    let resolver = local(endpoint);
    exchange(&resolver).await.unwrap();
    assert!(!resolver.owner.drained());
    resolver.owner.shutdown(until()).await.unwrap();
    assert!(resolver.owner.drained());
    assert!(resolver.clients.lock().await.is_empty());
    tokio::time::timeout_at(until(), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        exchange(&resolver).await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn shutdown_deadline_retains_the_actual_lease_and_driver_owner_for_retry() {
    let (endpoint, server) = closing_server().await;
    let resolver = local(endpoint);
    let lease = resolver.ready_client(endpoint, None, None).await.unwrap();
    let error = resolver
        .owner
        .shutdown(tokio::time::Instant::now() + Duration::from_millis(20))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(
        !resolver.owner.drained(),
        "the actual admitted user still owns its lease"
    );
    drop(lease);
    resolver.owner.shutdown(until()).await.unwrap();
    assert!(resolver.owner.drained());
    tokio::time::timeout_at(until(), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn dns_registry_retains_retired_resolver_ownership_until_actual_confirmation() {
    let (endpoint, server) = closing_server().await;
    let resolver = local(endpoint);
    let owners = crate::transport_owners::TransportOwners::default();
    owners.register_doh(resolver.owner.clone()).unwrap();
    exchange(&resolver).await.unwrap();
    let owner = resolver.owner.clone();
    drop(resolver);
    owners.shutdown_doh(until()).await.unwrap();
    assert!(owner.drained());
    tokio::time::timeout_at(until(), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn pool_shutdown_cancels_and_confirms_actual_inline_setup_before_driver_registration() {
    use tokio::io::AsyncReadExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut io, _) = listener.accept().await.unwrap();
        let mut byte = [0; 1];
        assert_eq!(io.read(&mut byte).await.unwrap(), 0);
    });
    let resolver = local(endpoint);
    let detour = Arc::new(PausedDetour {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    });
    let query = {
        let resolver = resolver.clone();
        let detour = detour.clone();
        tokio::spawn(async move {
            let query =
                crate::message::build_query("inline-close.test", crate::message::TYPE_A).unwrap();
            resolver
                .exchange(&query, Some("actual-detour"), Some(detour.as_ref()))
                .await
        })
    };
    tokio::time::timeout_at(until(), detour.entered.notified())
        .await
        .unwrap();
    assert!(
        !query.is_finished(),
        "inline setup owns an actual pending TCP socket"
    );
    resolver.owner.shutdown(until()).await.unwrap();
    assert!(resolver.owner.drained());
    assert!(tokio::time::timeout_at(until(), query)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    tokio::time::timeout_at(until(), server)
        .await
        .unwrap()
        .unwrap();
}
