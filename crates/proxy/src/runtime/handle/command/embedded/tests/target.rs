//! Ordinary acknowledged fixed-target diagnostics remain TCP connect-only.
use super::*;
use tokio::io::AsyncReadExt;
#[tokio::test]
async fn acknowledged_target_uses_same_async_command_scope_and_original_connect_only_wire() {
    timeout(Duration::from_secs(10),async{
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint=listener.local_addr().unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"actual-server","protocol":{"type":"socks5","server":"127.0.0.1","port":endpoint.port()}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
        let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        let observation=zero_runtime::ApplyObservation::new();let observed=observation.clone();
        let command=tokio::spawn(async move{observed.scope(handle.execute_acknowledged(zero_api::CommandRequest::DiagnosticsProbeTarget(zero_api::DiagnosticsProbeTargetCommand{
            target_tag:"actual-server".into(),operation_id:Some("same-target-command".into())
        }))).await});
        let(mut peer,remote)=listener.accept().await.unwrap();assert!(remote.ip().is_loopback());
        let result=command.await.unwrap().unwrap();assert!(result.accepted);
        let result=result.result.unwrap();assert_eq!(result["operation_id"],"same-target-command");
        assert_eq!(result["reachable"],true);assert_eq!(result["server"],"127.0.0.1");assert_eq!(result["port"],endpoint.port());
        let mut byte=[0;1];assert_eq!(peer.read(&mut byte).await.unwrap(),0,"target diagnostic must not send HTTP/proxy protocol bytes");
        assert!(observation.blocking_outcome().is_none(),"actual async work has no fabricated blocking outcome");
        proxy.protocols.drive_component_supervision(proxy.embedded_command_owner().shutdown(tokio::time::Instant::now()+Duration::from_secs(5))).await.unwrap();
        assert!(proxy.embedded_command_owner().retirement_complete());
        assert_eq!(proxy.application_control_budget().usage().pending_commands,0);
        assert!(proxy.engine().active_sessions().is_empty());
        running.shutdown().await.unwrap();assert_eq!(proxy.host.original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
