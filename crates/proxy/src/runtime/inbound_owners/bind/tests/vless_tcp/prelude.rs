use super::*;
#[tokio::test]
async fn vless_tcp_trusted_preludes_before_tls_preserve_tcp_udp_source_and_original_receipts() {
    timeout(Duration::from_secs(30),async {
        let files=tempfile::tempdir().unwrap();let material=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert=files.path().join("ca.pem");let key=files.path().join("key.pem");std::fs::write(&cert,material.cert.pem()).unwrap();std::fs::write(&key,material.signing_key.serialize_pem()).unwrap();
        for (carrier,version) in [("ws",1),("http_upgrade",2)] {
            let mut protocol=serde_json::json!({"type":"vless","users":[{"id":USER}],"tls":{"cert_path":cert,"key_path":key}});
            protocol[carrier]=serde_json::json!({"path":"/trusted","accept_proxy_protocol":true});
            let proxy=fixture::proxy(protocol).await;
            let tcp=TcpListener::bind("127.0.0.1:0").await.unwrap();let udp=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            for generation in 0..2 {
                let(owner,stop,address)=fixture::install(&proxy);
                let business=async {
                    owner.wait_ready().await.unwrap();
                    let mut stream=open_trusted(address,carrier,version,&cert).await;
                    let mut header=request(1,tcp.local_addr().unwrap().port());header.extend(b"tcp");stream.write_all(&header).await.unwrap();stream.flush().await.unwrap();
                    let mut target=tcp.accept().await.unwrap().0;let mut b=[0;3];target.read_exact(&mut b).await.unwrap();assert_eq!(&b,b"tcp");
                    target.write_all(b"back").await.unwrap();let mut reply=[0;6];stream.read_exact(&mut reply).await.unwrap();assert_eq!(&reply,b"\0\0back");
                    let records=proxy.engine().active_sessions();assert_eq!(records.len(),1);assert_eq!(records[0].source_ip,Some(Address::Ipv4([198,51,100,2])));assert_eq!(records[0].source_port,Some(43210));
                    let mut datagram=open_trusted(address,carrier,version,&cert).await;
                    let mut header=request(2,udp.local_addr().unwrap().port());header.extend(b"\0\x03udp");datagram.write_all(&header).await.unwrap();datagram.flush().await.unwrap();
                    let mut b=[0;64];let(n,sender)=udp.recv_from(&mut b).await.unwrap();assert_eq!(&b[..n],b"udp");udp.send_to(b"reply",sender).await.unwrap();
                    let mut reply=[0;9];datagram.read_exact(&mut reply).await.unwrap();assert_eq!(&reply,b"\0\0\0\x05reply");
                    assert!(proxy.engine().active_sessions().iter().all(|s|s.source_ip==Some(Address::Ipv4([198,51,100,2]))));
                    let mut byte=[0];let mut waiting=Box::pin(target.read(&mut byte));std::future::poll_fn(|cx|{assert!(waiting.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
                    stop.send_replace(true);assert_eq!(waiting.await.unwrap(),0);drop(stream);drop(datagram);
                };
                let(observed,())=tokio::join!(owner.observe_driver(),business);observed.unwrap_or_else(|e|panic!("{carrier}/{generation}: {e:?}"));
                fixture::retire(&proxy,3,3).await;
                assert!(proxy.engine().active_sessions().is_empty());assert_eq!(proxy.engine().completed_sessions().len(),(generation+1)*2);
            }
        }
    }).await.unwrap();
}
async fn open_trusted(
    address: std::net::SocketAddr,
    carrier: &str,
    version: u8,
    cert: &std::path::Path,
) -> TcpRelayStream {
    let mut socket = TcpStream::connect(address).await.unwrap();
    let prelude = zero_transport::proxy_protocol::encode(
        version,
        Some("198.51.100.2:43210".parse().unwrap()),
        Some("203.0.113.8:443".parse().unwrap()),
    )
    .unwrap();
    socket.write_all(&prelude).await.unwrap();
    let tls = zero_transport::profile::OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: Some(cert.to_string_lossy().into_owned()),
        insecure: false,
        alpn: vec![],
        client_fingerprint: None,
    };
    let io = zero_transport::tls::connect_tls_stream_with_profile(socket, &tls, None, "localhost")
        .await
        .unwrap();
    if carrier == "ws" {
        let profile: zero_config::WebSocketConfig =
            serde_json::from_value(serde_json::json!({"path":"/trusted"})).unwrap();
        TcpRelayStream::new(
            zero_transport::ws::connect_ws(io, &profile, "localhost", address.port())
                .await
                .unwrap(),
        )
    } else {
        let profile: zero_config::HttpUpgradeConfig =
            serde_json::from_value(serde_json::json!({"path":"/trusted"})).unwrap();
        TcpRelayStream::new(
            zero_transport::http_upgrade::connect_http_upgrade(io, &profile)
                .await
                .unwrap(),
        )
    }
}
