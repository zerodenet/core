use super::*;
use crate::application::services::{
    network::ApplicationEventSinkTcpDialer, startup::ApplicationStartupSlot,
};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
async fn application(engine: Engine) -> Arc<ApplicationServices> {
    let slot = ApplicationStartupSlot::new();
    ApplicationServices::start_with_network(
        engine,
        None,
        zero_connector::EventDispatcherNetwork::new(Arc::new(
            ApplicationEventSinkTcpDialer::for_test(
                zero_platform_tokio::EgressInterfaceControl::default(),
            ),
        )),
        &slot,
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn physically_retired_control_fault_clears_its_actual_slot_before_the_next_server_starts() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("recover.sock");
        let engine =
            Engine::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap())
                .unwrap();
        let proxy = Proxy::from_engine(engine.clone()).unwrap();
        let handle = ProxyHandle::new(EngineHandle::new(engine.clone()), proxy);
        let services = application(engine.clone()).await;
        let old = crate::ipc::prepare_ipc_server_with_budget(&path, services.control_budget());
        services.install_ipc(old.clone()).unwrap();
        old.start(handle.clone()).await.unwrap();
        services.control_budget.close_admission();
        let pending = tokio::net::UnixStream::connect(&path).await.unwrap();
        assert!(old.wait_terminated().await.is_err());
        assert!(!old.retirement_complete());
        let error = services.shutdown_control().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
        assert!(old.retirement_complete());
        assert!(services.control_retirement_complete());
        assert!(services.control.lock().unwrap().ipc.is_none());
        services.shutdown_status_monitor().await.unwrap();
        services.shutdown_dispatcher().await.unwrap();
        drop(pending);
        let fresh = application(engine).await;
        let server = crate::ipc::prepare_ipc_server_with_budget(&path, fresh.control_budget());
        fresh.install_ipc(server.clone()).unwrap();
        server.start(handle).await.unwrap();
        let mut client = BufReader::new(tokio::net::UnixStream::connect(&path).await.unwrap());
        client
            .get_mut()
            .write_all(b"{\"type\":\"ping\",\"id\":2}\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_line(&mut response).await.unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response).unwrap()["id"],
            2
        );
        fresh.shutdown_control().await.unwrap();
        assert!(fresh.control_retirement_complete());
        fresh.shutdown_status_monitor().await.unwrap();
        fresh.shutdown_dispatcher().await.unwrap();
    })
    .await
    .unwrap();
}
