//! Actual standard-source production wiring, identities and confirmed replacement.
use super::*;
use std::time::Duration;
use zero_engine::EngineHandle;
use zero_proxy::ProxyHandle;

fn config(address: &str, port: u16, target: Option<std::net::SocketAddr>) -> RuntimeConfig {
    let mut protocol = serde_json::json!({"type":"direct"});
    if let Some(target) = target {
        protocol["target"] = serde_json::json!(target.ip().to_string());
        protocol["port"] = serde_json::json!(target.port());
    }
    RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds":[{"tag":"entry", "listen":{"address":address,"port":port},
                "udp":{"enabled":false},"protocol":protocol}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap()
}
async fn connect(address: impl tokio::net::ToSocketAddrs + Clone) -> TcpStream {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match TcpStream::connect(address.clone()).await {
                Ok(stream) => return stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .expect("actual source ready")
}
#[tokio::test]
async fn standard_fixed_forward_keeps_one_identity_accounting_and_reverse_half_close() {
    for host in ["127.0.0.1", "localhost"] {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = echo.local_addr().unwrap();
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = reservation.local_addr().unwrap();
        let proxy = Engine::new(config(host, listen.port(), Some(address))).unwrap();
        let before = proxy.engine().mint_session_identity().unwrap();
        let observe = proxy.clone();
        drop(reservation);
        let running = spawn_engine(proxy);
        // Return the real business connection; no discarded readiness probe.
        let mut client = connect((host, listen.port())).await;
        let source = client.local_addr().unwrap();
        client.write_all(b"request").await.unwrap();
        client.shutdown().await.unwrap();
        let (mut peer, _) = echo.accept().await.unwrap();
        let mut request = Vec::new();
        peer.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request");
        peer.write_all(b"response").await.unwrap();
        peer.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"response");
        support::wait_for("standard Direct settlement", || {
            !observe.completed_sessions().is_empty()
        })
        .await;
        let sessions = observe.completed_sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, before.id().0 + 1);
        assert_eq!(sessions[0].source_port, Some(source.port()));
        assert_eq!(
            sessions[0].protocol.as_str(),
            zero_core::ProtocolType::UNKNOWN.as_str()
        );
        assert_eq!((sessions[0].bytes_up, sessions[0].bytes_down), (7, 8));
        running.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn targetless_standard_forward_closes_without_minting_business_flow() {
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = reservation.local_addr().unwrap();
    let proxy = Engine::new(config("127.0.0.1", listen.port(), None)).unwrap();
    let before = proxy.engine().mint_session_identity().unwrap();
    let observe = proxy.clone();
    drop(reservation);
    let running = spawn_engine(proxy);
    let mut client = connect(listen).await;
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    running.shutdown().await.unwrap();
    assert!(observe.completed_sessions().is_empty());
    assert_eq!(
        observe.engine().mint_session_identity().unwrap().id().0,
        before.id().0 + 1
    );
}
#[tokio::test]
async fn standard_forward_replacement_closes_real_old_stream_before_using_new_target() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let old_target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let new_target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = reservation.local_addr().unwrap();
        let proxy = Engine::new(config(
            "127.0.0.1",
            listen.port(),
            Some(old_target.local_addr().unwrap()),
        ))
        .unwrap();
        let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
        drop(reservation);
        let running = spawn_engine(proxy);
        let mut old = connect(listen).await;
        // Submit real business data while retaining both original directions.
        old.write_all(b"old").await.unwrap();
        let (mut old_peer, _) = tokio::time::timeout(Duration::from_secs(2), old_target.accept())
            .await
            .expect("old target accept")
            .unwrap();
        let mut payload = [0; 3];
        tokio::time::timeout(Duration::from_secs(2), old_peer.read_exact(&mut payload))
            .await
            .expect("old business transfer")
            .unwrap();
        assert_eq!(&payload, b"old");
        // Establish actual pending byte I/O before requesting replacement.
        {
            use std::{future::Future, task::Poll};
            let mut byte = [0];
            let pending = old.read(&mut byte);
            tokio::pin!(pending);
            std::future::poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        handle
            .apply_config_and_wait(
                config(
                    "127.0.0.1",
                    listen.port(),
                    Some(new_target.local_addr().unwrap()),
                ),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), old.read(&mut [0]))
                .await
                .expect("old client retirement")
                .unwrap(),
            0
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), old_peer.read(&mut [0]))
                .await
                .expect("old upstream retirement")
                .unwrap(),
            0
        );
        let mut new = connect(listen).await;
        new.write_all(b"new").await.unwrap();
        let (mut peer, _) = tokio::time::timeout(Duration::from_secs(2), new_target.accept())
            .await
            .expect("new target accept")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut payload))
            .await
            .expect("new business transfer")
            .unwrap();
        assert_eq!(&payload, b"new");
        running.shutdown().await.unwrap();
    })
    .await
    .expect("bounded actual replacement");
}
