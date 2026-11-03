//! Real production standard-source reload and failed-candidate recovery.
use super::*;
use std::time::Duration;
use zero_engine::EngineHandle;
use zero_proxy::ProxyHandle;

fn config_at(port: u16, host: &str) -> RuntimeConfig {
    RuntimeConfig::parse(
        &serde_json::json!({
        "inbounds": [{"tag":"standard-http", "listen":{"address":host,"port":port},
                "protocol":{"type":"http"}}], "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap()
}
async fn connect_at(host: &str, port: u16, target: std::net::SocketAddr) -> TcpStream {
    let mut io = TcpStream::connect((host, port)).await.unwrap();
    io.write_all(format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut reply = [0; 39];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"HTTP/1.1 200 Connection Established\r\n\r\n");
    io.write_all(b"live").await.unwrap();
    let mut bytes = [0; 4];
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"live");
    io
}

#[tokio::test]
async fn standard_http_source_replaces_live_originals_before_acknowledged_listener_reload() {
    replacement(false).await;
}
#[tokio::test]
async fn standard_http_source_failed_candidate_recovers_last_good_factory_and_wire_path() {
    replacement(true).await;
}
#[tokio::test]
async fn domain_listener_uses_system_resolution_and_replaces_the_actual_standard_source() {
    replacement_at(false, "localhost").await;
}
#[tokio::test]
async fn domain_listener_exhausts_candidates_then_recovers_last_good_standard_source() {
    replacement_at(true, "localhost").await;
}
async fn replacement(conflict: bool) {
    replacement_at(conflict, "127.0.0.1").await;
}
async fn replacement_at(conflict: bool, host: &str) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = origin.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut peer, _) = origin.accept().await.unwrap();
                let mut bytes = [0; 4];
                peer.read_exact(&mut bytes).await.unwrap();
                peer.write_all(&bytes).await.unwrap();
                assert_eq!(
                    peer.read(&mut bytes).await.unwrap(),
                    0,
                    "original relay actually ended"
                );
            }
        });
        let old_reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_port = old_reservation.local_addr().unwrap().port();
        let next_reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let next_port = next_reservation.local_addr().unwrap().port();
        let mut candidate_reservations = vec![next_reservation];
        // Retain every real candidate endpoint until the candidate is allowed
        // to take ownership. One occupied IPv4 endpoint is not a DNS conflict.
        let mut candidates = Vec::new();
        for endpoint in tokio::net::lookup_host((host, next_port)).await.unwrap() {
            if !candidates.contains(&endpoint) {
                candidates.push(endpoint);
            }
        }
        for endpoint in candidates {
            if !candidate_reservations
                .iter()
                .any(|listener| listener.local_addr().unwrap() == endpoint)
            {
                candidate_reservations.push(TcpListener::bind(endpoint).await.unwrap());
            }
        }
        let proxy = Engine::new(config_at(old_port, host)).unwrap();
        let engine = proxy.engine().clone();
        let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
        drop(old_reservation);
        let running = spawn_engine(proxy);
        tokio::time::timeout(Duration::from_secs(3), async {
            while TcpStream::connect((host, old_port)).await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut old_client = connect_at(host, old_port, target).await;
        if !conflict {
            candidate_reservations.clear();
        }
        let applied = handle
            .apply_runtime_config_and_wait(config_at(next_port, host), Duration::from_secs(5))
            .await;
        let selected = if conflict {
            assert!(applied.is_err());
            assert_eq!(engine.config().inbounds[0].listen.port, old_port);
            old_port
        } else {
            applied.unwrap();
            assert_eq!(engine.config().inbounds[0].listen.port, next_port);
            next_port
        };
        let mut byte = [0; 1];
        assert_eq!(old_client.read(&mut byte).await.unwrap(), 0);
        let mut new_client = connect_at(host, selected, target).await;
        new_client.shutdown().await.unwrap();
        let mut tail = Vec::new();
        new_client.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty());
        running.shutdown().await.unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
