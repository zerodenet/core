//! A real rejected HTTP input does not revoke its still-live original listener.
use super::*;
#[tokio::test]
async fn actual_http_frame_limit_keeps_same_listener_available_and_returns_all_credits() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        let mut limits = zero_runtime::configuration::ControlBudgetLimits::from_runtime(
            &zero_config::RuntimeOptionsConfig::default(),
        );
        limits.messages.http_header_bytes = 64;
        limits.messages.http_body_bytes = 1;
        let budget = zero_runtime::configuration::ControlBudget::new(limits);
        let server = prepare_http_server_with_budget("127.0.0.1:0", None, budget.clone());
        server.start(handle).await.unwrap();
        let address = server.bound.lock().unwrap().unwrap();
        let mut rejected = tokio::net::TcpStream::connect(address).await.unwrap();
        rejected
            .write_all(b"POST /api/v1/query HTTP/1.1\r\nContent-Length: 999999\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        rejected.read_to_end(&mut response).await.unwrap();
        assert!(response.is_empty());
        drop(rejected);
        assert!(!server.retirement_complete());
        let mut accepted = tokio::net::TcpStream::connect(address).await.unwrap();
        accepted
            .write_all(b"GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        accepted.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        drop(accepted);
        server.shutdown().await.unwrap();
        assert!(server.retirement_complete());
        let mut original = server.original.lock().await;
        let original = original.as_mut().unwrap();
        assert!(original
            .wait_closed(crate::ipc::owner::close_deadline())
            .await
            .unwrap()
            .is_complete());
        assert_eq!(original.confirmed_close_count(), 1);
        assert!(!original.native_unconfirmed());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
