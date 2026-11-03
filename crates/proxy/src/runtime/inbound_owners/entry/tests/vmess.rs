//! Actual VMess sources: original receipts, acknowledged replacement and MUX Scope joins.
use std::{net::SocketAddr, sync::Arc, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use zero_config::RuntimeConfig;
const USER: &str = "eeb6c16e-9257-47cf-a7b9-d01e05259c14";

fn frame(sid: u16, target: SocketAddr, network: u8, bytes: &[u8]) -> Vec<u8> {
    let std::net::IpAddr::V4(ip) = target.ip() else {
        panic!("IPv4 fixture")
    };
    let mut meta = sid.to_be_bytes().to_vec();
    meta.extend([1, 1, network]);
    meta.extend(target.port().to_be_bytes());
    meta.push(1);
    meta.extend(ip.octets());
    let mut wire = (meta.len() as u16).to_be_bytes().to_vec();
    wire.extend(meta);
    wire.extend((bytes.len() as u16).to_be_bytes());
    wire.extend(bytes);
    wire
}
async fn connect(
    connector: &tokio_rustls::TlsConnector,
    endpoint: SocketAddr,
    grpc: bool,
) -> zero_platform_tokio::TcpRelayStream {
    let socket = loop {
        match TcpStream::connect(endpoint).await {
            Ok(socket) => break socket,
            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    };
    let name = rustls::pki_types::ServerName::try_from("localhost")
        .unwrap()
        .to_owned();
    let carrier =
        zero_platform_tokio::TcpRelayStream::new(connector.connect(name, socket).await.unwrap());
    if grpc {
        zero_platform_tokio::TcpRelayStream::new(
            zero_transport::grpc::connect_grpc(carrier, &["zero.vmess/Tun".into()])
                .await
                .unwrap(),
        )
    } else {
        carrier
    }
}
#[tokio::test]
async fn standard_sources_confirm_udp_originals_before_replacement_and_join_actual_mux_workers() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tokio::time::timeout(Duration::from_secs(25),async {
        for grpc in [false,true] {
        let material=tempfile::tempdir().unwrap();let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert=material.path().join("cert.pem");let key=material.path().join("key.pem");std::fs::write(&cert,certificate.cert.pem()).unwrap();std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
        let reservation=TcpListener::bind("127.0.0.1:0").await.unwrap();let endpoint=reservation.local_addr().unwrap();
        let target=UdpSocket::bind("127.0.0.1:0").await.unwrap();let address=target.local_addr().unwrap();
        let config=RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"vmess-in","listen":{"address":"127.0.0.1","port":endpoint.port()},"protocol":{"type":"vmess","users":[{"id":USER,"cipher":"chacha20-poly1305"}],"tls":{"cert_path":cert,"key_path":key},"grpc": if grpc { serde_json::json!({"service_names":["zero.vmess/Tun"]}) } else { serde_json::Value::Null }}}],"route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=crate::Proxy::new(config).unwrap();drop(reservation);let running=proxy.spawn();
        let mut roots=rustls::RootCertStore::empty();roots.add(certificate.cert.der().clone()).unwrap();
        let connector=tokio_rustls::TlsConnector::from(Arc::new(rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()));
        let mut carrier=connect(&connector,endpoint,grpc).await;
        // Client-only wire construction. The production server assigns real identities.
        let request=zero_core::Session::new(901,zero_core::Address::Ipv4([127,0,0,1]),address.port(),zero_core::Network::Udp,zero_core::ProtocolType::new("client"));
        let state=::vmess::outbound::VmessOutbound.establish_udp_packet_session(&mut carrier,&request,&::vmess::parse_uuid(USER).unwrap(),::vmess::VmessCipher::Chacha20Poly1305).await.unwrap();
        let mut client=::vmess::stream::VmessAeadStream::outbound(carrier,state).unwrap();
        client.write_all(b"one").await.unwrap();client.flush().await.unwrap();let mut bytes=[0;32];let (size,peer)=target.recv_from(&mut bytes).await.unwrap();assert_eq!(&bytes[..size],b"one");target.send_to(b"one",peer).await.unwrap();let mut reply=[0;3];client.read_exact(&mut reply).await.unwrap();assert_eq!(&reply,b"one");
        let original=proxy.inbound_owners.owner("vmess-in").unwrap();let original_id=original.identity();assert!(original.terminal().is_none());
        { let mut byte=[0];let read=client.read(&mut byte);tokio::pin!(read);std::future::poll_fn(|cx| { assert!(std::future::Future::poll(read.as_mut(),cx).is_pending());Poll::Ready(()) }).await; }
        let next=TcpListener::bind("127.0.0.1:0").await.unwrap();let replacement=next.local_addr().unwrap();
        let mut candidate=(*proxy.engine().config()).clone();candidate.inbounds[0].listen.port=replacement.port();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());drop(next);
        handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(5)).await.unwrap();
        let closed=original.terminal().expect("same old owner must be confirmed before replacement ACK");assert!(closed.failure().is_none());assert!(closed.task_failures().is_empty());assert_eq!(closed.joined_tasks(),3 + usize::from(grpc));assert_eq!(closed.closed_io_receipts(),3,"original listener, accepted TLS carrier and Direct UDP native provider");
        let current=proxy.inbound_owners.owner("vmess-in").unwrap();assert_ne!(current.identity(),original_id);assert!(current.is_running());
        let target_tcp=TcpListener::bind("127.0.0.1:0").await.unwrap();let tcp_address=target_tcp.local_addr().unwrap();
        let carrier=connect(&connector,replacement,grpc).await;
        let request=zero_core::Session::new(902,zero_core::Address::Domain("v1.mux.cool".into()),666,zero_core::Network::Tcp,zero_core::ProtocolType::new("client"));
        let (mut mux,_)=::vmess::outbound::PreparedVmessOutboundRequestBundle::from_config(USER,"chacha20-poly1305",None).unwrap().establish_tcp_outbound_stream(carrier,&request).await.unwrap();
        mux.write_all(&frame(1,tcp_address,1,b"tcp")).await.unwrap();mux.flush().await.unwrap();let (mut tcp,_) = target_tcp.accept().await.unwrap();tcp.read_exact(&mut reply).await.unwrap();assert_eq!(&reply,b"tcp");
        mux.write_all(&frame(2,address,2,b"udp")).await.unwrap();mux.flush().await.unwrap();let (size,peer)=target.recv_from(&mut bytes).await.unwrap();assert_eq!(&bytes[..size],b"udp");target.send_to(b"udp",peer).await.unwrap();
        let length=mux.read_u16().await.unwrap();let mut meta=vec![0;length as usize];mux.read_exact(&mut meta).await.unwrap();assert_eq!(&meta[..4],&[0,2,2,1]);let length=mux.read_u16().await.unwrap();let mut payload=vec![0;length as usize];mux.read_exact(&mut payload).await.unwrap();assert_eq!(payload,b"udp");
        { let mut byte=[0];let read=mux.read(&mut byte);tokio::pin!(read);std::future::poll_fn(|cx| { assert!(std::future::Future::poll(read.as_mut(),cx).is_pending());Poll::Ready(()) }).await; }
        assert!(current.terminal().is_none());running.shutdown().await.unwrap();
        let closed=current.terminal().expect("same new source, no task marker or port probe");assert!(closed.failure().is_none());assert!(closed.task_failures().is_empty());
        assert_eq!(closed.joined_tasks(),7 + usize::from(grpc),"source, original accepted task, actual MUX reader/writer, TCP logical task, UDP logical task and UDP driver");
        assert_eq!(closed.closed_io_receipts(),4,"original listener, accepted TLS carrier, real TCP carrier and Direct UDP original provider");
        }
    }).await.unwrap();
}
