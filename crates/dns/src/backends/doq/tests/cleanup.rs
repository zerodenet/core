//! The actual first endpoint cannot be bypassed while its IO lease blocks join.
use super::*;
#[tokio::test]
async fn unsettled_actual_endpoint_keeps_query_and_cleanup_errors_and_never_dials_next_candidate() {
    timeout(Duration::from_secs(5), async {
        let (endpoint, mut resolver) = fixture();
        let unused = tokio::net::UdpSocket::bind(std::net::SocketAddr::from(([127,0,0,2],endpoint.local_addr().unwrap().port()))).await.unwrap();
        resolver.addrs.push(unused.local_addr().unwrap());
        let config: crate::DnsConfig = serde_json::from_value(serde_json::json!({
            "servers":{"primary":{"type":"doq","host":"127.0.0.1","port":endpoint.local_addr().unwrap().port(),"server_name":"localhost"},"fallback":{"type":"system"}},
            "default_server":"primary","policy":{"fallback_servers":["fallback"]}
        })).unwrap();
        let dns = crate::DnsSystem::build(Some(&config)).unwrap();
        resolver.attach_registry(dns.transport_owners.clone());
        let registry = dns.transport_owners.clone();
        let mut snapshot = dns.snapshot().unwrap();
        snapshot.servers.get_mut("primary").unwrap().backend = Arc::new(crate::backends::ResolverBackend::Doq(resolver));
        let caller = tokio::spawn(async move {
            let query = crate::message::build_query("unsettled.actual.doq.test", crate::message::TYPE_A).unwrap();
            crate::owned_root::exchange_backend_attempt("primary", &snapshot.servers["primary"], &query, &snapshot,
                Some(snapshot.servers["fallback"].backend.as_ref()), 1000).await
        });
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let size = recv.read_u16().await.unwrap() as usize;
        let mut query = vec![0; size];
        recv.read_exact(&mut query).await.unwrap();
        assert_eq!(
            crate::message::parse_question(&query).unwrap().domain,
            "unsettled.actual.doq.test"
        );
        let owners = registry.doq_snapshot();
        assert_eq!(owners.len(), 1);
        let original = owners[0].clone();
        assert_eq!(
            original.task_count(),
            2,
            "actual endpoint and connection drivers"
        );
        let held = original.query_user().unwrap();
        send.write_u16(1).await.unwrap();
        send.write_all(&[0]).await.unwrap();
        send.finish().unwrap();
        let (result, permits_fallback) = caller.await.unwrap();
        assert!(!permits_fallback, "an actual Unsettled endpoint also forbids the configured next backend");
        let returned = result.unwrap_err();
        assert_eq!(returned.kind(), io::ErrorKind::TimedOut);
        let failed = returned
            .get_ref()
            .unwrap()
            .downcast_ref::<crate::DnsQuicCleanupFailure>()
            .unwrap();
        assert_eq!(
            failed.query_error().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(failed.close_error().kind(), io::ErrorKind::TimedOut);
        assert!(original.closing() && !original.drained());
        assert_eq!(
            registry.doq_snapshot().len(),
            1,
            "no successor owner while this original remains unsettled"
        );
        let mut bytes = [0; 2048];
        assert_eq!(unused.try_recv_from(&mut bytes).unwrap_err().kind(), io::ErrorKind::WouldBlock, "no second candidate QUIC wire");
        drop(held);
        original
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(original.drained());
        assert_eq!(
            original.task_count(),
            0,
            "both original driver exits were consumed"
        );
        assert_eq!(
            failed.query_error().unwrap().kind(),
            io::ErrorKind::InvalidData,
            "the caller retains both original errors after actual cleanup retry"
        );
        connection.closed().await;
        endpoint.close(0u32.into(), b"test finished");
        endpoint.wait_idle().await;
        dns.shutdown_queries(Instant::now() + Duration::from_secs(1)).await.unwrap();
    })
    .await
    .unwrap();
}
