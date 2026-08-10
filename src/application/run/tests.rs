use zero_config::RuntimeConfig;
use zero_engine::EngineError;
use zero_proxy::Proxy;

use super::{proxy_stop_reason, run_supervised_proxy};

#[tokio::test]
async fn proxy_runtime_error_reaches_application_supervisor() {
    let config =
        RuntimeConfig::parse(r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#)
            .expect("minimal runtime config");
    let proxy = Proxy::new(config).expect("minimal proxy");

    let result = run_supervised_proxy(&proxy, std::future::pending()).await;

    assert!(matches!(result, Err(EngineError::NoInbounds)));
}

#[test]
fn proxy_stop_reason_distinguishes_signal_from_runtime_failure() {
    assert_eq!(proxy_stop_reason(&Ok(())), "signal");
    assert_eq!(
        proxy_stop_reason(&Err(EngineError::NoInbounds)),
        "runtime_error"
    );
}
