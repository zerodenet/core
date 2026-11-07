//! Ordinary fallback/masquerade borrow the same finite Host native graph.
use super::inbound_work_limits::{closed, owner};
use crate::{Proxy, ProxyHandle};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_config::RuntimeConfig;
async fn exchange(client: &mut TcpStream, target: &mut TcpStream) {
    client.write_all(b"same original").await.unwrap();
    let mut bytes = [0; 13];
    target.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same original");
    target.write_all(&bytes).await.unwrap();
    client.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"same original");
}
#[tokio::test]
async fn ordinary_vless_fallback_holds_global_target_graph_and_busy_keeps_actual_old_wire() {
    timeout(Duration::from_secs(25),async{
        let target=TcpListener::bind("127.0.0.1:0").await.unwrap();let port=target.local_addr().unwrap().port();
        let mut config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"carrier_max_total_originals":3,"inbound_max_target_sockets":1},"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[],"fallback":{"server":"127.0.0.1","port":port}}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();let old=owner(&proxy,"source").await;let address=*old.original_listener_address.get().unwrap();
        let mut client=TcpStream::connect(address).await.unwrap();let request=b"GET /actual HTTP/1.1\r\nHost: localhost\r\n\r\n";client.write_all(request).await.unwrap();let mut backend=target.accept().await.unwrap().0;let mut bytes=vec![0;request.len()];backend.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,request);
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,3,"same original Source listener, accepted socket and finite Target graph are all charged");
        exchange(&mut client,&mut backend).await;config.runtime.inbound_max_target_sockets=Some(2);let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        assert!(handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.is_err());assert_eq!(owner(&proxy,"source").await.identity(),old.identity());exchange(&mut client,&mut backend).await;
        drop(client);drop(backend);timeout(Duration::from_secs(5),async{while !old.acquisition_idle_for_test(){tokio::task::yield_now().await;}}).await.unwrap();
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,1,"same target native, unique view, actor and ACK have returned before Source replacement");
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();closed(&old);let fresh=owner(&proxy,"source").await;assert_ne!(fresh.identity(),old.identity());running.shutdown().await.unwrap();closed(&fresh);assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
#[tokio::test]
async fn ordinary_hy2_proxy_masquerade_uses_the_actual_target_graph_and_returns_it_before_source_stop(
) {
    timeout(Duration::from_secs(25),async{
        let _=rustls::crypto::ring::default_provider().install_default();let files=tempfile::tempdir().unwrap();let cert=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();let certificate=files.path().join("cert.pem");let key=files.path().join("key.pem");std::fs::write(&certificate,cert.cert.pem()).unwrap();std::fs::write(&key,cert.signing_key.serialize_pem()).unwrap();
        let target=TcpListener::bind("127.0.0.1:0").await.unwrap();let port=target.local_addr().unwrap().port();
        let config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"inbound_max_target_sockets":1,"quic_source_max_connections":2,"quic_shared_endpoint_max_users":16},"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"hysteria2","password":"actual","cert_path":certificate,"key_path":key,"masquerade":{"type":"proxy","url":format!("http://127.0.0.1:{port}")}}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy=Proxy::new(config).unwrap();let running=proxy.spawn();let source=owner(&proxy,"source").await;let address=*source.original_listener_address.get().unwrap();
        let before=proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged;
        let mut endpoint=quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();endpoint.set_default_client_config(zero_transport::quic::client_config(true,None,&[b"h3".to_vec()],None).unwrap());let connection=endpoint.connect(address,"localhost").unwrap().await.unwrap();
        let (mut driver,mut requests):(_,h3::client::SendRequest<h3_quinn::OpenStreams,bytes::Bytes>)=h3::client::new(h3_quinn::Connection::new(connection.clone())).await.unwrap();let driver=tokio::spawn(async move{std::future::poll_fn(|cx|driver.poll_close(cx)).await});
        let request=http_uri::Request::builder().uri("https://hysteria/actual-target").body(()).unwrap();let mut response=requests.send_request(request).await.unwrap();response.finish().await.unwrap();
        let mut backend=target.accept().await.unwrap().0;let mut headers=Vec::new();while !headers.ends_with(b"\r\n\r\n"){headers.push(backend.read_u8().await.unwrap());assert!(headers.len()<8192);}assert!(headers.starts_with(b"GET /actual-target "));
        assert!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged>before,"the actual masquerade Target graph is not an uncharged raw socket");
        backend.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nsame original").await.unwrap();drop(backend);assert_eq!(response.recv_response().await.unwrap().status(),200);let mut body=Vec::new();use bytes::Buf;while let Some(mut bytes)=response.recv_data().await.unwrap(){let n=bytes.remaining();body.extend_from_slice(&bytes.copy_to_bytes(n));}assert_eq!(body,b"same original");drop(response);drop(requests);connection.close(0u32.into(),b"actual completed caller");endpoint.close(0u32.into(),b"actual completed caller");endpoint.wait_idle().await;let _=driver.await.unwrap();
        assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());running.shutdown().await.unwrap();closed(&source);assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
