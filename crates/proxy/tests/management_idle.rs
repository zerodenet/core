use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

fn empty_config() -> RuntimeConfig {
    RuntimeConfig::parse(r#"{"inbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}"#)
        .unwrap()
}

fn with_listener(port: u16) -> RuntimeConfig {
    RuntimeConfig::parse(&format!(
        r#"{{"inbounds":[{{"tag":"first","listen":{{"address":"127.0.0.1","port":{port}}},"protocol":{{"type":"direct"}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#
    )).unwrap()
}

#[tokio::test]
async fn failed_first_bind_preserves_idle_runtime_for_retry() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    let proxy = Proxy::new(empty_config()).unwrap();
    let engine = proxy.engine().clone();
    let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy.clone());
    let running = proxy.spawn();

    assert!(handle
        .apply_config_and_wait(with_listener(port), Duration::from_secs(5))
        .await
        .is_err());
    assert!(engine.config().inbounds.is_empty());
    drop(occupied);
    handle
        .apply_config_and_wait(with_listener(port), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(TcpStream::connect(("127.0.0.1", port)).await.is_ok());
    handle
        .apply_config_and_wait(empty_config(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(TcpListener::bind(("127.0.0.1", port)).await.is_ok());
    tokio::time::timeout(Duration::from_secs(3), running.shutdown())
        .await
        .unwrap()
        .unwrap();
}
