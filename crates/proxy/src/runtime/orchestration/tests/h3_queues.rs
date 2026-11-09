//! Ordinary H3 queues share the existing Source, profile, native and Scope.
use crate::{Proxy, ProxyHandle, RunningProxy};
use bytes::{Buf, Bytes};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_config::RuntimeConfig;
use zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin;
type Owner = crate::runtime::inbound_owners::InboundOwner;
type Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
async fn ready(proxy: &Proxy, tag: &str) -> Arc<Owner> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(owner) = proxy.inbound_owners.owner(tag) {
                owner.wait_ready().await.unwrap();
                return owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn closed(owner: &Owner) {
    let closed = owner
        .terminal_for_test()
        .expect("same actual Source native/users/Scope terminal");
    assert_eq!(closed.identity(), owner.identity());
    assert!(closed.joined_tasks() > 0 && closed.closed_io_receipts() > 0);
    assert!(
        closed.failure().is_none()
            && closed.task_failures().is_empty()
            && closed.native_bound_failures().is_empty()
    );
}
fn queues(config: &RuntimeConfig, expected: usize) {
    let options = crate::adapters::hysteria2::Hysteria2Adapter::default()
        .inbound_options_in_runtime(&config.inbounds[0], None, &config.runtime)
        .unwrap();
    let actual = options.h3_queue_limits();
    assert_eq!(
        [
            actual.classifications().get(),
            actual.tcp_streams().get(),
            actual.web_streams().get(),
            actual.web_requests().get()
        ],
        [expected; 4]
    );
}
async fn web(address: std::net::SocketAddr) {
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(
        zero_transport::quic::client_config(true, None, &[b"h3".to_vec()], None).unwrap(),
    );
    eprintln!(
        "actual H3 web client original local={:?}",
        endpoint.local_addr().unwrap()
    );
    let connection = endpoint
        .connect(address, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (mut driver, mut requests): (_, Sender) =
        h3::client::new(h3_quinn::Connection::new(connection.clone()))
            .await
            .unwrap();
    let driver =
        tokio::spawn(async move { std::future::poll_fn(|cx| driver.poll_close(cx)).await });
    let mut streams = Vec::new();
    for index in 0..3 {
        let request = http_uri::Request::builder()
            .method("GET")
            .uri(format!("https://hysteria/queue-{index}"))
            .body(())
            .unwrap();
        let mut stream = requests.send_request(request).await.unwrap();
        stream.finish().await.unwrap();
        streams.push(stream);
    }
    futures_util::future::join_all(streams.into_iter().map(|mut stream| async move {
        assert_eq!(stream.recv_response().await.unwrap().status(), 200);
        let mut body = Vec::new();
        while let Some(mut data) = stream.recv_data().await.unwrap() {
            let size = data.remaining();
            body.extend_from_slice(&data.copy_to_bytes(size));
        }
        assert_eq!(body, b"same actual queue");
    }))
    .await;
    let request = http_uri::Request::builder()
        .method("POST")
        .uri("https://hysteria/auth")
        .header("hysteria-auth", "actual")
        .header("hysteria-cc-rx", "0")
        .body(())
        .unwrap();
    let mut auth = requests.send_request(request).await.unwrap();
    auth.finish().await.unwrap();
    assert_eq!(auth.recv_response().await.unwrap().status(), 233);
    while auth.recv_data().await.unwrap().is_some() {}
    drop(auth);
    drop(requests);
    connection.close(0u32.into(), b"normal original H3 peer close");
    endpoint.close(0u32.into(), b"normal original H3 peer close");
    endpoint.wait_idle().await;
    let _actual_driver_result = driver.await.unwrap();
}
async fn socks(address: std::net::SocketAddr, port: u16) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(&[5, 1, 0]).await?;
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await?;
    assert_eq!(auth, [5, 0]);
    let mut bytes = vec![5, 1, 0, 1, 127, 0, 0, 1];
    bytes.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&bytes).await?;
    let mut response = [0; 10];
    stream.read_exact(&mut response).await?;
    if response[1] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "actual HY2 business failed",
        ));
    }
    Ok(stream)
}
async fn live(front: &mut TcpStream, back: &mut TcpStream) {
    front.write_all(b"actual H3").await.unwrap();
    let mut bytes = [0; 9];
    back.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"actual H3");
    back.write_all(&bytes).await.unwrap();
    front.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"actual H3");
}
fn metadata(proxy: &Proxy) -> MetadataPoolOrigin {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let role = plan.configured("pool").unwrap();
    role.dependency_instances()
        .iter()
        .filter_map(|id| plan.component(id.component))
        .find_map(|root| {
            (0..root.recipe.spec().resources.len()).find_map(|ordinal| {
                root.original_metadata_owner(ordinal)
                    .and_then(|owner| owner.current_original())
            })
        })
        .expect("actual same authenticated endpoint original")
}
#[tokio::test]
async fn configured_h3_queue_owners_use_real_web_auth_tcp_and_retire_source_before_change() {
    timeout(Duration::from_secs(25),async {
        let _=rustls::crypto::ring::default_provider().install_default();let files=tempfile::tempdir().unwrap();let certificate=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();let cert=files.path().join("cert.pem");let key=files.path().join("key.pem");std::fs::write(&cert,certificate.cert.pem()).unwrap();std::fs::write(&key,certificate.signing_key.serialize_pem()).unwrap();
        let mut config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"hysteria2_h3_max_pending_classifications":1,"hysteria2_h3_max_pending_tcp_streams":1,"hysteria2_h3_max_pending_web_streams":1,"hysteria2_h3_max_pending_web_requests":1},"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"hysteria2","password":"actual","cert_path":cert,"key_path":key,"masquerade":{"type":"string","content":"same actual queue","status":200,"content_type":"text/plain"}}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        queues(&config,1);let remote=Proxy::new(config.clone()).unwrap();let remote_running:RunningProxy=remote.spawn();let original=ready(&remote,"source").await;let address=*original.original_listener_address.get().unwrap();web(address).await;
        assert!(remote.engine().active_sessions().is_empty() && remote.engine().completed_sessions().is_empty(),"masquerade/authentication is not a business Session");
        let client_config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"hysteria2_max_entries":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"pool","protocol":{"type":"hysteria2","server":"127.0.0.1","server_name":"localhost","port":address.port(),"password":"actual","insecure":true}}],"route":{"final":{"type":"route","outbound":"pool"}}}).to_string()).unwrap();
        let client=Proxy::new(client_config).unwrap();let running=client.spawn();let entry=ready(&client,"entry").await;let front=*entry.original_listener_address.get().unwrap();let echo=TcpListener::bind("127.0.0.1:0").await.unwrap();let port=echo.local_addr().unwrap().port();
        let (first,peer)=tokio::join!(socks(front,port),echo.accept());let mut first=first.unwrap();let mut first_peer=peer.unwrap().0;live(&mut first,&mut first_peer).await;let native=metadata(&client);eprintln!("actual HY2 client native original endpoints={:?}",native.endpoints());
        let (second,peer)=tokio::join!(socks(front,port),echo.accept());let mut second=second.unwrap();let mut second_peer=peer.unwrap().0;live(&mut second,&mut second_peer).await;live(&mut first,&mut first_peer).await;
        assert!(native.same_original(&metadata(&client)),"queue1 does not create a second authenticated endpoint/pool");assert!(native.closed().is_none());
        drop(first);drop(second);drop(first_peer);drop(second_peer);running.shutdown().await.unwrap();closed(&entry);let native_closed=native.closed().unwrap();assert!(native_closed.matches(&native) && native_closed.had_native_acquisition());assert!(native.owner().closed().unwrap().matches(native.owner()));assert_eq!(client.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0,"same originals users/drivers/registration ACK returned");
        let idle=timeout(Duration::from_secs(5),async {while !original.acquisition_idle_for_test() {tokio::task::yield_now().await;}}).await;
        if idle.is_err() { eprintln!("actual H3 source running={} terminal={:?}",original.is_running(),original.terminal_for_test().map(|closed|closed.identity()));original.peer_tasks_for_test().diagnose_actual_retirement(); }
        idle.unwrap();
        config.runtime.hysteria2_h3_max_pending_classifications=Some(2);config.runtime.hysteria2_h3_max_pending_tcp_streams=Some(2);config.runtime.hysteria2_h3_max_pending_web_streams=Some(2);config.runtime.hysteria2_h3_max_pending_web_requests=Some(2);
        let handle=ProxyHandle::new(zero_engine::EngineHandle::new(remote.engine().clone()),remote.clone());handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.unwrap();closed(&original);let fresh=ready(&remote,"source").await;assert_ne!(fresh.identity(),original.identity());queues(&config,2);web(*fresh.original_listener_address.get().unwrap()).await;
        remote_running.shutdown().await.unwrap();closed(&fresh);assert_eq!(remote.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
