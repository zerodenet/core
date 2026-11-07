//! Actual same original Unix listener, byte refusal, nested wire and full close.
use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
async fn request(path: &Path) -> Vec<u8> {
    let mut client = BufReader::new(tokio::net::UnixStream::connect(path).await.unwrap());
    client
        .get_mut()
        .write_all(b"{\"type\":\"query\",\"id\":17,\"request\":{\"endpoints\":{}}}\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    client.read_until(b'\n', &mut response).await.unwrap();
    response
}
#[tokio::test]
async fn endpoint_ipc_response_admission_recovers_on_same_listener_and_returns_original_native_scope_credits(
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("endpoints.sock");
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
        let server = prepare_ipc_server_with_budget(&path, budget.clone());
        server.start(handle).await.unwrap();
        assert!(request(&path).await.is_empty());
        assert!(!server.retirement_complete());
        limits.messages.response_bytes_total = 128 * 1024;
        budget.prepare(limits).unwrap().commit().unwrap();
        let response = request(&path).await;
        let body: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(body["id"], 17);
        assert_eq!(body["ok"], true);
        assert_eq!(body["result"]["endpoints"]["total"], 0);
        server.shutdown().await.unwrap();
        assert!(server.retirement_complete());
        assert!(server.originals.active.lock().unwrap().is_empty());
        assert!(server.originals.activating.lock().unwrap().is_none());
        let closed = server
            .listener
            .close(crate::ipc::owner::close_deadline())
            .await
            .unwrap();
        assert!(server.listener.matches_closed(&closed));
        assert!(closed.had_native_acquisition());
        assert!(!path.exists());
        budget.close_admission();
        budget.confirm_released().unwrap();
    })
    .await
    .unwrap();
}
