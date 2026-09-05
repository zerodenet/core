use zero_config::RuntimeConfig;
use zero_engine::EngineError;
use zero_proxy::Proxy;

use super::{drain_parent_lifetime, proxy_stop_reason, run_supervised_proxy};

#[tokio::test]
async fn proxy_runtime_error_reaches_application_supervisor() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port();
    let config = RuntimeConfig::parse(&format!(
        r#"{{ "inbounds": [{{ "tag":"conflict", "listen":{{"address":"127.0.0.1","port":{port}}}, "protocol":{{"type":"direct"}} }}], "route": {{ "rules": [], "final": {{ "type": "direct" }} }} }}"#
    )).expect("runtime config with externally occupied inbound");
    let proxy = Proxy::new(config).expect("minimal proxy");

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        run_supervised_proxy(&proxy, std::future::pending()),
    )
    .await
    .expect("supervisor must surface bind failure promptly");

    assert!(result.is_err());
}

#[test]
fn proxy_stop_reason_distinguishes_signal_from_runtime_failure() {
    assert_eq!(proxy_stop_reason(&Ok(())), "signal");
    assert_eq!(
        proxy_stop_reason(&Err(EngineError::NoInbounds)),
        "runtime_error"
    );
}

#[test]
fn managed_parent_lifetime_waits_until_pipe_eof() {
    let input = std::io::Cursor::new(b"parent-is-alive".to_vec());

    drain_parent_lifetime(input).expect("parent lifetime pipe should close cleanly at EOF");
}
