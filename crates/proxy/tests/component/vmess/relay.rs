use super::fixtures::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_config::RuntimeConfig;
use zero_proxy::{Proxy, ProxyHandle};

#[tokio::test]
async fn actual_registered_socks_to_vmess_chain_keeps_tls_and_service_identity_inside_the_selected_prefix(
) {
    timeout(Duration::from_secs(10), async {
        let tls_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tls_address = tls_listener.local_addr().unwrap();
        let acceptor = acceptor_with_alpn(false);
        let server = tokio::spawn(async move {
            let (stream, _) = tls_listener.accept().await.unwrap();
            let tls = acceptor.accept(stream).await.unwrap();
            let (facts,mut stream)=profile("aes-128-gcm").accept_tcp_stream(vmess::inbound::VmessInbound,zero_platform_tokio::TcpRelayStream::new(tls)).await.unwrap();
            assert_eq!(facts.target,destination().address);assert_eq!(facts.port,destination().port);
            let mut http = Vec::new();
            loop {
                let byte = stream.read_u8().await.unwrap();
                http.push(byte);
                if http.ends_with(b"\r\n\r\n") {
                    break;
                }
                assert!(http.len() < 4096);
            }
            assert!(http.starts_with(b"HEAD /native "));
            stream.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n").await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let socks_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socks_address = socks_listener.local_addr().unwrap();
        let prefix = tokio::spawn(async move {
            let (mut stream, _) = socks_listener.accept().await.unwrap();
            let mut greeting = [0; 3];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            stream.write_all(&[5, 0]).await.unwrap();
            let mut head = [0; 5];
            stream.read_exact(&mut head).await.unwrap();
            assert_eq!(&head[..4], &[5, 1, 0, 3]);
            let mut host = vec![0; head[4] as usize];
            stream.read_exact(&mut host).await.unwrap();
            assert_eq!(host, b"127.0.0.1");
            assert_eq!(stream.read_u16().await.unwrap(), tls_address.port());
            let mut upstream = TcpStream::connect(tls_address).await.unwrap();
            stream.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0]).await.unwrap();
            tokio::io::copy_bidirectional(&mut stream, &mut upstream).await.unwrap();
        });
        let config = RuntimeConfig::parse(&format!(r#"{{
            "outbounds":[
                {{"tag":"prefix","protocol":{{"type":"socks5","server":"127.0.0.1","port":{}}}}},
                {{"tag":"native","protocol":{{"type":"vmess","server":"127.0.0.1","port":{},"id":"{}","cipher":"aes-128-gcm",
                    "tls":{{"server_name":"localhost","insecure":true,"client_fingerprint":"none"}}}}}}
            ],
            "outbound_groups":[{{"tag":"chain","type":"relay","proxies":["prefix","native"]}}],
            "route":{{"final":{{"type":"direct"}}}}
        }}"#, socks_address.port(), tls_address.port(), ID)).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(5)).await.unwrap();
        proxy.probe_outbound_single("chain", "http://native.example:443/native").await.unwrap();
        assert!(proxy.engine().active_sessions().is_empty(), "service requests must not create fake business sessions");
        running.shutdown().await.unwrap();
        server.await.unwrap();
        prefix.await.unwrap();
    }).await.unwrap();
}
