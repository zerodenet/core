//! Actual native UDP lineage, concurrent logical roles and confirmed replacement.
use super::*;
async fn stage<T>(name: &str, work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), work)
        .await
        .unwrap_or_else(|_| panic!("native UDP stage timed out: {name}"))
}
async fn original(proxy: &Proxy) -> Arc<crate::runtime::inbound_owners::InboundOwner> {
    stage("standard source ready", super::original(proxy)).await
}
async fn request(
    connection: &Arc<ClientConnection>,
    command: u8,
    target: SocketAddr,
) -> MieruLogicalStream {
    stage(
        "logical request/reply",
        super::request(connection, command, target),
    )
    .await
}
async fn udp_exchange(io: &mut MieruLogicalStream, target: &UdpSocket, value: &[u8]) {
    stage("UDP round trip", super::udp_exchange(io, target, value)).await
}
async fn client(endpoint: SocketAddr, user: &str, password: &str) -> Arc<ClientConnection> {
    ClientConnection::udp(
        Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()),
        endpoint,
        user,
        password,
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn native_peer_routes_tcp_udp_and_replaces_only_after_original_scope_and_io_close() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let reserved = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = reserved.local_addr().unwrap();
        let proxy = Proxy::new(config(endpoint.port(), "udp")).unwrap();
        drop(reserved);
        let running = proxy.spawn();
        let owner = original(&proxy).await;
        let connection = client(endpoint, USER, PASSWORD).await;
        let mut stalled = connection.open().await.unwrap();
        stalled.write_all(&[5]).await.unwrap(); stage("stalled logical flush", stalled.flush()).await.unwrap();
        let mut byte = [0];
        let pending = stalled.read(&mut byte);
        tokio::pin!(pending);
        std::future::poll_fn(|cx| { assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending()); Poll::Ready(()) }).await;
        let target_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut tcp = request(&connection, 1, target_tcp.local_addr().unwrap()).await;
        tcp.write_all(b"tcp").await.unwrap(); tcp.flush().await.unwrap();
        let (mut target, _) = target_tcp.accept().await.unwrap();
        let mut bytes = [0; 3]; target.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"tcp");
        target.write_all(b"yes").await.unwrap(); tcp.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"yes");
        let target_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut udp = request(&connection, 3, "0.0.0.0:0".parse().unwrap()).await;
        udp_exchange(&mut udp, &target_udp, b"before").await;
        assert_eq!(proxy.active_sessions().len(), 2, "physical UDP peer and pending handshake are not business sessions");
        let next = endpoint;
        let mut candidate = (*proxy.engine().config()).clone();
        let zero_config::InboundProtocolConfig::Mieru { users, .. } = &mut candidate.inbounds[0].protocol else { unreachable!() };
        users[0].password = "replacement-password".into();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(candidate, Duration::from_secs(5)).await.unwrap();
        let closed = owner.terminal().expect("old original peer closure precedes replacement ACK");
        assert!(closed.failure().is_none()); assert!(closed.task_failures().is_empty());
        assert_eq!(closed.joined_tasks(), 7, "one source, real peer, physical driver, three logical tasks and original UDP response driver");
        assert_eq!(closed.closed_io_receipts(), 3, "original UDP listener, TCP carrier, UDP egress; no invented accepted TCP original");
        assert_eq!(target.read(&mut bytes).await.unwrap(), 0);
        let current = original(&proxy).await; assert_ne!(current.identity(), owner.identity());
        let connection = client(next, USER, "replacement-password").await;
        let mut udp = request(&connection, 3, "0.0.0.0:0".parse().unwrap()).await;
        udp_exchange(&mut udp, &target_udp, b"after").await;
        stage("original host shutdown", running.shutdown()).await.unwrap();
        let closed = current.terminal().unwrap(); assert!(closed.failure().is_none()); assert_eq!(closed.closed_io_receipts(), 2);
        assert_eq!(proxy.completed_sessions().len(),3);
        assert!(proxy.completed_sessions().iter().all(|s| s.auth.as_ref().and_then(|a| a.principal_key.as_deref()) == Some("mieru-principal")));
    }).await.unwrap();
}
#[tokio::test]
async fn hostname_native_udp_source_preserves_other_peers_during_principal_cancellation() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let reserved = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = reserved.local_addr().unwrap();
        let mut cfg = config(endpoint.port(), "udp");
        cfg.inbounds[0].listen.address = "localhost".into();
        let zero_config::InboundProtocolConfig::Mieru { users, .. } = &mut cfg.inbounds[0].protocol
        else {
            unreachable!()
        };
        let mut other = users[0].clone();
        other.username = "other-user".into();
        other.password = "other-password".into();
        other.principal_key = Some("other-principal".into());
        users.push(other);
        let proxy = Proxy::new(cfg).unwrap();
        drop(reserved);
        let running = proxy.spawn();
        let owner = original(&proxy).await;
        // Use the address family selected by the actual system lookup/bind.
        let ip = std::net::ToSocketAddrs::to_socket_addrs(&("localhost", endpoint.port()))
            .unwrap()
            .next()
            .unwrap();
        let first_socket = Arc::new(
            UdpSocket::bind(if ip.is_ipv4() {
                "127.0.0.1:0"
            } else {
                "[::1]:0"
            })
            .await
            .unwrap(),
        );
        let second_socket = Arc::new(
            UdpSocket::bind(if ip.is_ipv4() {
                "127.0.0.1:0"
            } else {
                "[::1]:0"
            })
            .await
            .unwrap(),
        );
        let first = ClientConnection::udp(first_socket, ip, USER, PASSWORD)
            .await
            .unwrap();
        let second = ClientConnection::udp(second_socket, ip, "other-user", "other-password")
            .await
            .unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut a = request(&first, 3, "0.0.0.0:0".parse().unwrap()).await;
        let mut b = request(&second, 3, "0.0.0.0:0".parse().unwrap()).await;
        udp_exchange(&mut a, &target, b"first").await;
        udp_exchange(&mut b, &target, b"second").await;
        assert_eq!(
            proxy
                .close_principal_flows("mieru-principal", "disabled")
                .len(),
            1
        );
        // UDP has no physical EOF. Consume the actual retired peer's native
        // egress receipt instead of assuming the external client's read ends.
        stage("cancelled peer original cleanup", async {
            loop {
                let receipts: usize = owner
                    .peers
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|group| group.closed_io_receipts_count())
                    .sum();
                if receipts != 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        udp_exchange(&mut b, &target, b"survives").await;
        stage("original host shutdown", running.shutdown())
            .await
            .unwrap();
        let closed = owner.terminal().unwrap();
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert_eq!(
            closed.closed_io_receipts(),
            3,
            "one source and two original UDP egress providers"
        );
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn malformed_native_packets_do_not_stop_the_standard_source() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let reserved = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = reserved.local_addr().unwrap();
        let proxy = Proxy::new(config(endpoint.port(), "udp")).unwrap();
        drop(reserved);
        let running = proxy.spawn();
        let owner = original(&proxy).await;
        let invalid = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        invalid.send_to(&[0; 1501], endpoint).await.unwrap();
        invalid.send_to(&[0; 72], endpoint).await.unwrap();
        let connection = client(endpoint, USER, PASSWORD).await;
        let mut udp = request(&connection, 3, "0.0.0.0:0".parse().unwrap()).await;
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_exchange(&mut udp, &target, b"healthy").await;
        stage("original host shutdown", running.shutdown())
            .await
            .unwrap();
        assert!(owner.terminal().unwrap().failure().is_none());
    })
    .await
    .unwrap();
}
