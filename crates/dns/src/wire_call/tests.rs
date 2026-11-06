use super::*;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn unpolled_and_pure_local_call_return_never_fabricates_a_native_query() {
    let dns = DnsSystem::build(None).unwrap();
    let wire = crate::message::build_query("app.localhost", 1).unwrap();
    let (cold, never_polled) = dns.prepare_udp_query(&wire);
    assert!(cold.closed().is_none());
    drop(never_polled);
    let closed = cold.wait_closed(end()).await.unwrap();
    assert!(closed.matches(&cold));
    assert!(closed.query().is_none());
    assert!(closed.joined().is_none());
    assert!(closed.failure().is_none());
    let (local, answer) = dns.prepare_udp_query(&wire);
    assert!(!local.same_call(&cold));
    assert!(!closed.matches(&local));
    let response = answer.await.unwrap();
    let parsed = crate::message::parse_response(&wire, &response).unwrap();
    assert_eq!(
        parsed.addresses,
        vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]
    );
    let local_closed = local.wait_closed(end()).await.unwrap();
    assert!(local_closed.matches(&local));
    assert!(local_closed.query().is_none());
    assert!(local_closed.failure().is_none());
    assert!(Arc::ptr_eq(&local_closed, &local.closed().unwrap()));
}
#[cfg(feature = "udp")]
#[tokio::test]
async fn an_actual_unowned_udp_wire_worker_cannot_issue_a_host_cleanup_receipt() {
    use zero_config::{DnsConfig, DnsServerConfig};
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let config = DnsConfig {
        servers: std::collections::BTreeMap::from([(
            "wire".into(),
            DnsServerConfig::Udp {
                host: "127.0.0.1".into(),
                port: socket.local_addr().unwrap().port(),
                bootstrap: vec![],
                detour: None,
            },
        )]),
        default_server: "wire".into(),
        dispatch: vec![],
        cache: None,
        reverse_mapping: None,
        answer: Default::default(),
        policy: Default::default(),
    };
    let dns = DnsSystem::build(Some(&config)).unwrap();
    let wire = crate::message::build_query("actual.example", 1).unwrap();
    let (control, mut answer) = dns.prepare_udp_query(&wire);
    let received = async {
        let mut bytes = [0; 512];
        let (size, _) = socket.recv_from(&mut bytes).await.unwrap();
        assert_eq!(&bytes[12..size], &wire[12..]);
    };
    tokio::pin!(received);
    tokio::select! {
        _=&mut received=>{},
        result=&mut answer=>panic!("wire query returned before its actual Pending response: {result:?}"),
        _=tokio::time::sleep_until(end())=>panic!("actual DNS UDP request was not received"),
    }
    control.cancel();
    assert_eq!(answer.await.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert!(
        control.wait_closed(end()).await.is_err(),
        "a bare library JoinSet is not a matching installed host/native provider"
    );
    assert!(control.closed().is_none());
    dns.shutdown_queries(end()).await.unwrap();
}
