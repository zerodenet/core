//! Real ordinary TCP continues across denied metadata shrink, then old native
//! Source and Scope retire only after actual history/header allocations return.
use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_tun::packet_memory::{PacketMetadataArc, PacketMetadataLayout};
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
async fn connect(port: u16, target: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut method = [0; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [5, 0]);
    let std::net::IpAddr::V4(ip) = target.ip() else {
        unreachable!()
    };
    let mut request = vec![5, 1, 0, 1];
    request.extend_from_slice(&ip.octets());
    request.extend_from_slice(&target.port().to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut ack = [0; 10];
    stream.read_exact(&mut ack).await.unwrap();
    assert_eq!(ack[1], 0);
    stream
}
async fn echo(client: &mut TcpStream, peer: &mut TcpStream) {
    let (sent, received) = tokio::join!(
        async {
            client.write_all(b"old-tcp").await.unwrap();
            let mut answer = [0; 7];
            client.read_exact(&mut answer).await.unwrap();
            answer
        },
        async {
            let mut value = [0; 7];
            peer.read_exact(&mut value).await.unwrap();
            peer.write_all(&value).await.unwrap();
            value
        }
    );
    assert_eq!(sent, *b"old-tcp");
    assert_eq!(received, sent);
}
#[tokio::test]
async fn metadata_shrink_retains_real_old_tcp_and_native_owner_until_history_and_weak_header_return(
) {
    let reserved = crate::socket_fixture::tcp_udp();
    let port = reserved.0.local_addr().unwrap().port();
    let initial = RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"metadata-source","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"socks5"}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    drop(reserved);
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(initial.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let owner = proxy.inbound_owners.owner("metadata-source").unwrap();
    owner.wait_ready().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::time::timeout_at(end(), connect(port, listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    tokio::time::timeout_at(end(), echo(&mut client, &mut peer))
        .await
        .unwrap();
    let root = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    let baseline = root.driver_metadata().occupied;
    let history_layout = PacketMetadataArc::<[u8; 4096]>::layout().unwrap();
    let history = PacketMetadataArc::from_prepaid(
        [3; 4096],
        root.reserve_driver_metadata(history_layout).unwrap(),
    )
    .ok()
    .unwrap();
    let header_layout = PacketMetadataArc::<[u8; 2048]>::layout().unwrap();
    let header = PacketMetadataArc::from_prepaid(
        [4; 2048],
        root.reserve_driver_metadata(header_layout).unwrap(),
    )
    .ok()
    .unwrap();
    let weak = PacketMetadataArc::downgrade(&header);
    drop(header);
    let limit = root.driver_metadata().limit;
    let mut next = initial;
    next.runtime.packet_driver_metadata_total_max_bytes = Some(baseline.bytes() + 1);
    assert!(handle
        .apply_runtime_config_and_wait(next.clone(), Duration::from_secs(5))
        .await
        .is_err());
    assert!(Arc::ptr_eq(
        &owner,
        &proxy.inbound_owners.owner("metadata-source").unwrap()
    ));
    assert!(!owner.task_access().cancellation().is_cancelled());
    assert!(owner.terminal_for_test().is_none());
    assert_eq!(root.driver_metadata().limit, limit);
    tokio::time::timeout_at(end(), echo(&mut client, &mut peer))
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    drop(client);
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    drop(peer);
    tokio::time::timeout_at(end(), async {
        while !owner.acquisition_idle_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // There is now no active business to explain denial: actual strong
    // history, then the surviving real weak header, each independently block it.
    assert!(handle
        .apply_runtime_config_and_wait(next.clone(), Duration::from_secs(5))
        .await
        .is_err());
    drop(history);
    assert!(weak.upgrade().is_none());
    assert!(handle
        .apply_runtime_config_and_wait(next.clone(), Duration::from_secs(5))
        .await
        .is_err());
    assert!(Arc::ptr_eq(
        &owner,
        &proxy.inbound_owners.owner("metadata-source").unwrap()
    ));
    assert!(!owner.task_access().cancellation().is_cancelled());
    assert!(owner.terminal_for_test().is_none());
    assert_eq!(root.driver_metadata().limit, limit);
    drop(weak);
    handle
        .apply_runtime_config_and_wait(next.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(
        &owner,
        &proxy.inbound_owners.owner("metadata-source").unwrap()
    ));
    let terminal = owner
        .terminal_for_test()
        .expect("same old native source positively retired");
    assert!(terminal.closed_io_receipts() >= 1);
    assert!(terminal.joined_tasks() >= 1);
    assert_eq!(
        root.driver_metadata().limit,
        PacketMetadataLayout::new(
            next.runtime
                .effective_packet_driver_metadata_total_max_allocations(),
            baseline.bytes() + 1
        )
    );
    // Successful install must end the old metadata fence before startup.
    let mut fresh = tokio::time::timeout_at(end(), connect(port, listener.local_addr().unwrap()))
        .await
        .unwrap();
    let (mut fresh_peer, _) = listener.accept().await.unwrap();
    tokio::time::timeout_at(end(), echo(&mut fresh, &mut fresh_peer))
        .await
        .unwrap();
    fresh.shutdown().await.unwrap();
    drop(fresh);
    assert_eq!(fresh_peer.read(&mut byte).await.unwrap(), 0);
    drop(fresh_peer);
    drop(listener);
    running.shutdown().await.unwrap();
}
