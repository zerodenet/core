//! Public SOCKS Source -> actual standard Datagram Flow -> original MUX provider.
use super::*;
use tokio::net::{TcpStream, UdpSocket};
async fn associate(
    front: std::net::SocketAddr,
    client: &UdpSocket,
) -> (TcpStream, std::net::SocketAddr) {
    let mut control = TcpStream::connect(front).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 3, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&client.local_addr().unwrap().port().to_be_bytes());
    control.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let ip = std::net::Ipv4Addr::new(reply[4], reply[5], reply[6], reply[7]);
    let ip = if ip.is_unspecified() {
        std::net::Ipv4Addr::LOCALHOST
    } else {
        ip
    };
    (
        control,
        (ip, u16::from_be_bytes([reply[8], reply[9]])).into(),
    )
}
async fn send(
    client: &UdpSocket,
    relay: std::net::SocketAddr,
    target: std::net::SocketAddr,
    payload: &[u8],
) {
    let mut frame = vec![0, 0, 0, 1, 127, 0, 0, 1];
    frame.extend_from_slice(&target.port().to_be_bytes());
    frame.extend_from_slice(payload);
    assert_eq!(client.send_to(&frame, relay).await.unwrap(), frame.len());
}
async fn reply(client: &UdpSocket, target: std::net::SocketAddr, payload: &[u8]) {
    let mut frame = vec![0; 65535];
    let (length, _) = client.recv_from(&mut frame).await.unwrap();
    assert_eq!(&frame[..8], &[0, 0, 0, 1, 127, 0, 0, 1]);
    assert_eq!(u16::from_be_bytes([frame[8], frame[9]]), target.port());
    assert_eq!(&frame[10..length], payload);
}
async fn exercise(protocol: &str, stop_pending: bool, ingress: &str) {
    timeout(Duration::from_secs(30),async{
        let server=Server::start(protocol).await;
        // Retain the actual bound peer originals through every I/O suspension.
        let first=UdpSocket::bind("127.0.0.1:0").await.unwrap();let second=UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut value=serde_json::to_value(config(protocol,server.fixture.front.port())).unwrap();
        value["runtime"]["native_mux_max_carriers"]=json!(1);
        value["inbounds"]=json!([{"tag":"front","listen":{"address":"127.0.0.1","port":0},"udp":{"enabled":true},"protocol":{"type":ingress}}]);
        let proxy=Proxy::new(zero_config::RuntimeConfig::parse(&value.to_string()).unwrap()).unwrap();let running=start_components(&proxy).await;
        let source=proxy.inbound_owners.owner("front").unwrap();source.wait_ready().await.unwrap();let front=*source.original_listener_address.get().unwrap();
        let client=UdpSocket::bind("127.0.0.1:0").await.unwrap();let (mut control,relay)=associate(front,&client).await;
        let mut peers=Vec::new();
        for (socket,payload) in [(&first,b"first".as_slice()),(&second,b"second".as_slice())]{
            let target=socket.local_addr().unwrap();send(&client,relay,target,payload).await;
            let mut bytes=[0;65535];let (size,peer)=socket.recv_from(&mut bytes).await.unwrap();assert_eq!(&bytes[..size],payload);peers.push(peer);
            socket.send_to(&bytes[..size],peer).await.unwrap();reply(&client,target,payload).await;
            // Establish the first real RX as Pending before another inbound
            // target wins select and drops that outer response wait.
            timeout(Duration::from_secs(3),async{loop{let actual=source.native_response_readers_for_test();if actual.len()==peers.len()&&actual.iter().all(|reader|reader.pending_for_test()){break;}tokio::task::yield_now().await;}}).await.unwrap();
        }
        let readers=timeout(Duration::from_secs(3),async{loop{let readers=source.native_response_readers_for_test();if readers.len()==2&&readers.iter().all(|reader|reader.pending_for_test()){break readers;}tokio::task::yield_now().await;}}).await.unwrap();
        let books=source.response_books_for_test();assert!(!books.is_empty());assert!(books.iter().all(|book|!book.compatibility_created_for_test()),"ordinary standard RX never creates a private response JoinSet");
        assert!(readers[0].original.same_registration(&readers[1].original),"both real standard RX operations run in the same original association driver");
        assert!(readers.iter().all(|reader|reader.session_for_test()!=0));assert_ne!(readers[0].session_for_test(),readers[1].session_for_test());
        let actual_caller=readers[0].original.clone();assert!(!actual_caller.is_joined());
        let registration=registration(&proxy);let observed=registration.observe().unwrap();assert_eq!(observed.slots().len(),1);
        let original=observed.slots()[0].original().clone();let driver=observed.slots()[0].driver().unwrap().1.clone();
        // No new client send is needed to rearm either actual RX. The other
        // target must not consume, drop, or relabel an unsolicited reply.
        second.send_to(b"unsolicited",peers[1]).await.unwrap();reply(&client,second.local_addr().unwrap(),b"unsolicited").await;
        send(&client,relay,first.local_addr().unwrap(),b"").await;let mut bytes=[0;8];let (size,peer)=first.recv_from(&mut bytes).await.unwrap();assert_eq!(size,0);assert_eq!(peer,peers[0]);first.send_to(&[],peer).await.unwrap();reply(&client,first.local_addr().unwrap(),b"").await;
        timeout(Duration::from_secs(3),async{while !readers.iter().all(|reader|reader.pending_for_test()){tokio::task::yield_now().await;}}).await.unwrap();
        assert!(!actual_caller.is_joined()&&!driver.is_joined()&&original.closed().is_none());
        if !stop_pending{control.shutdown().await.unwrap();drop(control);}
        // The runtime itself requests Source stop and consumes its original
        // full observer. Real Pending RX is established above, never by delay.
        running.shutdown().await.unwrap();
        assert!(books.iter().all(|book|!book.compatibility_created_for_test()));
        assert!(readers.iter().all(|reader|reader.complete()),"the original exclusive dispatcher lease returned these exact RX views");
        let joined=actual_caller.try_joined().expect("same actual caller Scope consumed its join");assert!(actual_caller.matches_joined(&joined));
        assert!(driver.is_joined());let closed=original.closed().expect("same physical provider close receipt");assert!(original.matches_closed(&closed));
        let terminal=source.terminal_for_test().unwrap();assert!(terminal.closed_io_receipts()>0&&terminal.joined_tasks()>0);assert!(terminal.failure().is_none()&&terminal.task_failures().is_empty()&&terminal.native_bound_failures().is_empty());
        assert_eq!(registration.observe().unwrap().retained(),0);drop(readers);
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
        server.close().await;
    }).await.unwrap();
}
#[tokio::test]
async fn standard_datagram_flow_uses_original_rx_for_multiple_targets_empty_packets_and_unsolicited_replies(
) {
    for protocol in ["vless", "vmess"] {
        exercise(protocol, false, "socks5").await;
    }
}
#[tokio::test]
async fn source_stop_during_actual_pending_rx_retains_original_views_native_provider_and_scope_receipts(
) {
    exercise("vmess", true, "socks5").await;
}

#[cfg(feature = "mixed")]
#[tokio::test]
async fn standard_mixed_tcp_control_keeps_original_udp_association_and_pending_rx_ownership() {
    exercise("vmess", false, "mixed").await;
    exercise("vmess", true, "mixed").await;
}
