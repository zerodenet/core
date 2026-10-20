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
    // ready_client may return before the driver reads peer SETTINGS. Establish
    // one real round trip before holding the lease, so this fixture exercises
    // lease retirement rather than a TCP reset with unread handshake bytes.
    exchange(&resolver).await.unwrap();
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

#[tokio::test]
async fn reload_retirement_waits_for_real_lease_and_recovery_rebuilds_retired_backend() {
    use crate::{backends::ResolverBackend, DnsSystem, DnsSystemInner};
    let (endpoint, server) = closing_server().await;
    let resolver = local(endpoint);
    // Establish the real H2 connection before holding a retirement-blocking lease.
    exchange(&resolver).await.unwrap();
    let lease = resolver.ready_client(endpoint, None, None).await.unwrap();
    let resolver = Arc::try_unwrap(resolver)
        .ok()
        .expect("fixture has one resolver owner");
    let actual = Arc::new(ResolverBackend::Doh(resolver));
    let config: zero_config::DnsConfig = serde_json::from_value(serde_json::json!({
        "servers":{"local":{"type":"doh","host":"127.0.0.1","port":endpoint.port()}},
        "default_server":"local"
    }))
    .unwrap();
    let dns = DnsSystem::build(Some(&config)).unwrap();
    // Reuse the real cleartext H2 fixture to isolate resource lifecycle from TLS.
    actual.register_owners(&dns.transport_owners).unwrap();
    if let DnsSystemInner::Configured { servers, .. } = &mut *dns.inner.write().unwrap() {
        servers.get_mut("local").unwrap().backend = actual.clone();
    }
    let candidate = dns
        .prepare_reload_resources_with_dispatch(None, None, &Default::default())
        .unwrap();
    assert!(
        actual.resource_admission_open(),
        "cold preparation cannot close current"
    );
    let error = dns
        .retire_reload_resources(
            &candidate,
            tokio::time::Instant::now() + Duration::from_millis(20),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert_eq!(
        dns.commit_reload_resources(&candidate).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(!actual.resource_admission_open());
    drop(lease);
    dns.retire_reload_resources(&candidate, until())
        .await
        .unwrap();
    tokio::time::timeout_at(until(), server)
        .await
        .unwrap()
        .unwrap();
    // Model a later transaction failure: discard cannot revive the old pool.
    dns.discard_prepared_reload();
    let recovery = dns
        .prepare_reload_resources_with_dispatch(
            Some(&config),
            crate::compile_standalone_dispatch(Some(&config)).unwrap(),
            &Default::default(),
        )
        .unwrap();
    dns.retire_reload_resources(&recovery, until())
        .await
        .unwrap();
    dns.commit_reload_resources(&recovery).unwrap();
    if let DnsSystemInner::Configured { servers, .. } = &*dns.inner.read().unwrap() {
        assert!(
            !Arc::ptr_eq(&servers["local"].backend, &actual),
            "recovery builds the closed backend again"
        );
        assert!(servers["local"].backend.resource_admission_open());
    };
}

#[tokio::test]
async fn rule_only_reload_reuses_the_live_h2_connection_and_actual_backend() {
    use crate::{backends::ResolverBackend, DnsSystem, DnsSystemInner};
    let (endpoint, server) = closing_server().await;
    let resolver = Arc::try_unwrap(local(endpoint)).ok().unwrap();
    let actual = Arc::new(ResolverBackend::Doh(resolver));
    let mut config: zero_config::DnsConfig = serde_json::from_value(serde_json::json!({
        "servers":{"local":{"type":"doh","host":"127.0.0.1","port":endpoint.port()}},
        "default_server":"local"
    }))
    .unwrap();
    let dns = DnsSystem::build(Some(&config)).unwrap();
    actual.register_owners(&dns.transport_owners).unwrap();
    if let DnsSystemInner::Configured { servers, .. } = &mut *dns.inner.write().unwrap() {
        servers.get_mut("local").unwrap().backend = actual.clone();
    }
    let query = crate::message::build_query("reuse.reload.test", crate::message::TYPE_A).unwrap();
    actual.exchange(&query, None, None).await.unwrap();
    config.policy.timeout_ms += 1;
    dns.reload(Some(&config)).unwrap();
    let current = match &*dns.inner.read().unwrap() {
        DnsSystemInner::Configured { servers, .. } => servers["local"].backend.clone(),
        _ => panic!("configured DNS required"),
    };
    assert!(Arc::ptr_eq(&actual, &current));
    // The fixture accepts only one TCP connection; the second real exchange
    // can succeed only by reusing the original live H2 connection.
    current.exchange(&query, None, None).await.unwrap();
    dns.shutdown_doh_pools(until()).await.unwrap();
    tokio::time::timeout_at(until(), server)
        .await
        .unwrap()
        .unwrap();
}
