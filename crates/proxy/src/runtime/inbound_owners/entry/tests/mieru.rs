//! Real Mieru wire clients, actual accepted Scope, original provider receipts.
use crate::Proxy;
use ::mieru::{client::ClientConnection, inbound::multiplex::MieruLogicalStream};
use std::{net::SocketAddr, sync::Arc, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
const USER: &str = "standard-user";
const PASSWORD: &str = "standard-password";

async fn original(proxy: &Proxy) -> Arc<crate::runtime::inbound_owners::InboundOwner> {
    loop {
        if let Some(owner) = proxy.inbound_owners.owner("mieru-in") {
            owner.wait_ready().await.unwrap();
            return owner;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
fn config(port: u16, mode: &str) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"mieru-in","listen":{"address":"127.0.0.1","port":port},
            "protocol":{"type":"mieru","transport":mode,"users":[{"username":USER,"password":PASSWORD,"principal_key":"mieru-principal"}]}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
async fn tcp_client(endpoint: SocketAddr) -> Arc<ClientConnection> {
    ClientConnection::tcp(
        zero_platform_tokio::TokioSocket::new(TcpStream::connect(endpoint).await.unwrap()),
        USER,
        PASSWORD,
    )
    .await
    .unwrap()
}
async fn request(
    connection: &Arc<ClientConnection>,
    command: u8,
    target: SocketAddr,
) -> MieruLogicalStream {
    let mut io = connection.open().await.unwrap();
    let std::net::IpAddr::V4(ip) = target.ip() else {
        panic!("IPv4 fixture")
    };
    let mut request = vec![5, command, 0, 1];
    request.extend_from_slice(&ip.octets());
    request.extend_from_slice(&target.port().to_be_bytes());
    io.write_all(&request).await.unwrap();
    io.flush().await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, &[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    io
}
async fn udp_exchange(io: &mut MieruLogicalStream, target: &UdpSocket, value: &[u8]) {
    let codec = ::mieru::udp::MieruInboundUdpSession::new();
    codec
        .write_response_for_target_tokio(
            io,
            &zero_core::Address::Ipv4([127, 0, 0, 1]),
            target.local_addr().unwrap().port(),
            value,
        )
        .await
        .unwrap();
    let mut buffer = [0; 128];
    let (size, peer) = target.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..size], value);
    target.send_to(value, peer).await.unwrap();
    let reply = codec
        .read_inbound_dispatch_tokio(io, &mut [])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply.payload(), value);
}
#[tokio::test]
async fn tcp_source_routes_tcp_udp_beside_stalled_handshake_and_confirms_originals_before_replacement(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = reservation.local_addr().unwrap();
        let proxy = Proxy::new(config(endpoint.port(), "tcp")).unwrap();
        drop(reservation);
        let running = proxy.spawn();
        let owner = original(&proxy).await;
        let id = owner.identity();
        let connection = tcp_client(endpoint).await;
        let mut stalled = connection.open().await.unwrap();
        stalled.write_all(&[5]).await.unwrap();
        stalled.flush().await.unwrap();
        {
            let mut byte = [0];
            let read = stalled.read(&mut byte);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(read.as_mut(), cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        let target_tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut tcp = request(&connection, 1, target_tcp.local_addr().unwrap()).await;
        tcp.write_all(b"tcp").await.unwrap();
        tcp.flush().await.unwrap();
        let (mut target, _) = target_tcp.accept().await.unwrap();
        let mut bytes = [0; 3];
        target.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"tcp");
        target.write_all(b"yes").await.unwrap();
        tcp.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"yes");
        let target_udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut udp = request(&connection, 3, "0.0.0.0:0".parse().unwrap()).await;
        udp_exchange(&mut udp, &target_udp, b"udp").await;
        assert_eq!(
            proxy.active_sessions().len(),
            2,
            "no synthetic flow for physical or stalled logical handshake"
        );
        {
            let mut byte = [0];
            let read = target.read(&mut byte);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                assert!(std::future::Future::poll(read.as_mut(), cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let next = reserved.local_addr().unwrap();
        let mut candidate = (*proxy.engine().config()).clone();
        candidate.inbounds[0].listen.port = next.port();
        drop(reserved);
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
            .await
            .unwrap();
        let closed = owner
            .terminal()
            .expect("same original closed before replacement ACK");
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert_eq!(
            closed.joined_tasks(),
            7,
            "source, parent, physical driver, three logical tasks and original UDP driver"
        );
        assert_eq!(
            closed.closed_io_receipts(),
            4,
            "listener, accepted physical TCP, real TCP egress and UDP provider originals"
        );
        assert_eq!(
            target.read(&mut bytes).await.unwrap(),
            0,
            "actual old TCP egress is closed"
        );
        let current = original(&proxy).await;
        assert_ne!(current.identity(), id);
        let connection = tcp_client(next).await;
        let mut udp = request(&connection, 3, "0.0.0.0:0".parse().unwrap()).await;
        udp_exchange(&mut udp, &target_udp, b"after").await;
        running.shutdown().await.unwrap();
        let closed = current.terminal().unwrap();
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert_eq!(closed.closed_io_receipts(), 3);
        let completed = proxy.completed_sessions();
        assert_eq!(completed.len(), 3);
        assert!(completed
            .iter()
            .all(|s| s.auth.as_ref().and_then(|a| a.principal_key.as_deref())
                == Some("mieru-principal")));
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn tcp_source_principal_cancellation_closes_the_physical_driver_and_other_users_keep_working()
{
    tokio::time::timeout(Duration::from_secs(20), async {
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = reservation.local_addr().unwrap();
        let mut cfg = config(endpoint.port(), "tcp");
        let zero_config::InboundProtocolConfig::Mieru { users, .. } = &mut cfg.inbounds[0].protocol
        else {
            unreachable!()
        };
        let mut second = users[0].clone();
        second.username = "other-user".into();
        second.password = "other-password".into();
        second.principal_key = Some("other-principal".into());
        users.push(second);
        let proxy = Proxy::new(cfg).unwrap();
        drop(reservation);
        let running = proxy.spawn();
        let owner = original(&proxy).await;
        let first = tcp_client(endpoint).await;
        let second = ClientConnection::tcp(
            zero_platform_tokio::TokioSocket::new(TcpStream::connect(endpoint).await.unwrap()),
            "other-user",
            "other-password",
        )
        .await
        .unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut a = request(&first, 3, "0.0.0.0:0".parse().unwrap()).await;
        let mut b = request(&second, 3, "0.0.0.0:0".parse().unwrap()).await;
        udp_exchange(&mut a, &target, b"first").await;
        udp_exchange(&mut b, &target, b"other").await;
        assert_eq!(
            proxy
                .close_principal_flows("mieru-principal", "disabled")
                .len(),
            1
        );
        let mut byte = [0];
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), a.read(&mut byte))
                .await
                .unwrap(),
            Ok(0) | Err(_)
        ));
        udp_exchange(&mut b, &target, b"survives").await;
        running.shutdown().await.unwrap();
        let closed = owner.terminal().unwrap();
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn tcp_source_rejection_uses_protocol_close_and_keeps_the_underlay_available() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = reserved.local_addr().unwrap();
        let mut cfg = config(endpoint.port(), "tcp");
        cfg.route.final_action = zero_config::RouteActionConfig::Reject;
        let proxy = Proxy::new(cfg).unwrap();
        drop(reserved);
        let running = proxy.spawn();
        let owner = original(&proxy).await;
        let connection = tcp_client(endpoint).await;
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut rejected = request(&connection, 1, target.local_addr().unwrap()).await;
        let mut byte = [0];
        assert!(matches!(rejected.read(&mut byte).await, Ok(0) | Err(_)));
        let pending = target.accept();
        tokio::pin!(pending);
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let _another = connection.open().await.unwrap();
        assert!(owner.terminal().is_none());
        running.shutdown().await.unwrap();
        let closed = owner.terminal().unwrap();
        assert!(closed.failure().is_none());
        assert!(closed.task_failures().is_empty());
        assert_eq!(closed.closed_io_receipts(), 2);
    })
    .await
    .unwrap();
}

mod udp;
