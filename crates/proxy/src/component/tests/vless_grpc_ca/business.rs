//! Real accepted Business caller retains the cached physical origin facts.
use super::*;

async fn socks_connect(client: &mut tokio::net::TcpStream, request: &[u8]) -> [u8; 10] {
    client.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    tokio::time::timeout_at(deadline(), client.read_exact(&mut auth))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(auth, [5, 0]);
    client.write_all(request).await.unwrap();
    let mut reply = [0; 10];
    tokio::time::timeout_at(deadline(), client.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    reply
}

#[tokio::test]
async fn ordinary_socks_business_uses_the_same_grpc_origin_and_keeps_both_traffic_directions() {
    tokio::time::timeout(Duration::from_secs(25),async {
        for multi in [false,true] {
            let directory=Directory::new();let path=directory.0.join("ca.pem");let peer=Peer::new().await;std::fs::write(&path,&peer.pem).unwrap();
            let mut value=serde_json::to_value(config(&peer,&path,"rustls",multi)).unwrap();
            // Exercise Darwin's bounded process lookup with the original
            // 4 KiB limit. Other platforms use their ordinary lookup capacity.
            #[cfg(not(target_os = "macos"))]
            value["runtime"].as_object_mut().unwrap().remove("file_read_max_bytes");
            value["inbounds"]=serde_json::json!([{"tag":"caller","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}]);
            value["route"]["final"]=serde_json::json!({"type":"route","outbound":"grpc"});
            let config=RuntimeConfig::parse(&value.to_string()).unwrap();let proxy=Proxy::new(config.clone()).unwrap();let registry=proxy.system_tun_source_registry();let running=proxy.spawn();
            let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
            let source=tokio::time::timeout_at(deadline(),async {loop {if let Some(owner)=registry.owner("caller"){owner.wait_ready().await.unwrap();break owner;}tokio::task::yield_now().await;}}).await.unwrap();
            let bound=*source.original_listener_address.get().unwrap();
            let mut seeded=loan(&proxy).await;echo(&mut seeded).await;let original=seeded.owner.original().clone();let driver=original.driver().unwrap().clone();returned(seeded).await;
            let mut client=tokio::net::TcpStream::connect(bound).await.unwrap();
            let mut request=vec![5,1,0,3,12];request.extend_from_slice(b"real.example");request.extend_from_slice(&443u16.to_be_bytes());
            let reply = socks_connect(&mut client, &request).await;
            assert_eq!(&reply[..2], &[5, 0]);
            client.write_all(b"tls").await.unwrap();let mut payload=[0;3];tokio::time::timeout_at(deadline(),client.read_exact(&mut payload)).await.unwrap().unwrap();assert_eq!(payload,*b"tls");
            client.shutdown().await.unwrap();let mut final_bytes=Vec::new();tokio::time::timeout_at(deadline(),client.read_to_end(&mut final_bytes)).await.unwrap().unwrap();assert!(final_bytes.is_empty());drop(client);
            let completed=tokio::time::timeout_at(deadline(),async {loop {let records=proxy.engine().completed_sessions();if !records.is_empty(){break records;}tokio::task::yield_now().await;}}).await.unwrap();
            assert_eq!(completed.len(),1);let record=&completed[0];assert_eq!(record.inbound_tag.as_deref(),Some("caller"));assert_eq!(record.outbound_tag.as_deref(),Some("grpc"));assert!(record.failure.is_none(),"{:?}",record.failure);
            assert_eq!(record.target,Address::Domain("real.example".into()));assert_eq!(record.port,443);
            assert_eq!(record.outbound_tx_bytes,38,"35-byte actual VLESS request and three payload bytes");assert_eq!(record.outbound_rx_bytes,3);
            let remote=record.path.remote.as_ref().unwrap();assert_eq!(remote.host,"127.0.0.1");assert_eq!(remote.port,peer.address.port());
            assert_eq!(peer.accepts.load(Ordering::SeqCst),1);assert!(original.closed().is_none());assert!(!driver.is_joined());
            running.shutdown().await.unwrap();let joined=driver.wait_registration_released().await;assert!(driver.matches_joined(&joined)&&joined.registration_released());assert!(original.closed().unwrap().matches(&original));assert!(original.owner().closed().unwrap().matches(original.owner()));assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            let terminal=source.terminal_for_test().unwrap();assert!(terminal.failure().is_none());assert!(terminal.joined_tasks()>0);assert!(terminal.closed_io_receipts()>=2);
            peer.close().await;
        }
    }).await.unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn socks_connect_replies_when_the_actual_process_lookup_exceeds_its_read_budget() {
    use std::task::Poll;
    use zero_platform_tokio::contract::{Reason, Stage};

    let proxy = Proxy::new(
        RuntimeConfig::parse(
            r#"{
                "runtime":{"file_read_max_bytes":1},
                "inbounds":[{"tag":"caller","listen":{"address":"127.0.0.1","port":0},
                             "protocol":{"type":"socks5"}}],
                "route":{"final":{"type":"direct"}}
            }"#,
        )
        .unwrap(),
    )
    .unwrap();
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let running = proxy.spawn();
    running.wait_ready(deadline()).await.unwrap();
    let source = proxy.system_tun_source_registry().owner("caller").unwrap();
    let bound = *source.original_listener_address.get().unwrap();
    let mut client = tokio::net::TcpStream::connect(bound).await.unwrap();
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&upstream.local_addr().unwrap().port().to_be_bytes());
    let reply = socks_connect(&mut client, &request).await;
    assert_eq!(reply, [5, 4, 0, 1, 0, 0, 0, 0, 0, 0]);
    let mut tail = Vec::new();
    tokio::time::timeout_at(deadline(), client.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert!(tail.is_empty());
    running.shutdown().await.unwrap();

    assert!(proxy.engine().active_sessions().is_empty());
    assert!(proxy.engine().completed_sessions().is_empty());
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(upstream.poll_accept(cx)))
            .await
            .is_pending()
    );
    let terminal = source.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none());
    assert!(terminal.joined_tasks() > 0);
    assert!(terminal.closed_io_receipts() >= 2);
    assert!(terminal.client_failures().any(|failure| {
        matches!(failure.result(), Err(error)
            if error.stage == Stage::Prepare && error.reason == Reason::Capacity)
    }));
}
