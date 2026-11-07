//! Actual same-listener HTTP wire, local capacity rejection and physical close.
use super::*;
async fn request(address: std::net::SocketAddr) -> Vec<u8> {
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET /api/v1/endpoints HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    response
}
#[tokio::test]
async fn endpoint_response_capacity_recovers_on_same_original_http_listener_without_old_dto_fallback(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let proxy = zero_proxy::Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let budget = proxy.application_control_budget();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy,
        );
        let mut limits = budget.limits();
        limits.messages.response_bytes_total = 1;
        budget.prepare(limits).unwrap().commit().unwrap();
        let server = prepare_http_server_with_budget("127.0.0.1:0", None, budget.clone());
        server.start(handle).await.unwrap();
        let address = server.bound.lock().unwrap().unwrap();
        assert!(request(address).await.is_empty());
        assert!(!server.retirement_complete());
        limits.messages.response_bytes_total = 128 * 1024;
        budget.prepare(limits).unwrap().commit().unwrap();
        let wire = request(address).await;
        assert!(wire.starts_with(b"HTTP/1.1 200"));
        let separator = wire
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&wire[separator + 4..]).unwrap();
        assert_eq!(body["ok"], true);
        assert_eq!(body["result"]["total"], 0);
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
