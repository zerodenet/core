//! Real IPC EOF cancels its actual diagnostic waiter, retaining cleanup.
use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_runtime::configuration::{ControlBudget, ControlBudgetLimits};

#[tokio::test]
async fn ipc_disconnect_stops_the_real_probe_and_returns_credits_after_original_cleanup() {
    timeout(Duration::from_secs(5),async {
    let config=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"target","protocol":{"type":"direct"}}],"runtime":{"diagnostic_max_operations":1},"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy=zero_proxy::Proxy::new(config).unwrap();
    // The existing cold diagnostic maintenance path owns the original Host,
    // Apply and probe Scope. No second runtime/standalone probe is constructed.
    let (_,originals)=proxy.application_network_originals().unwrap();
    let handle=zero_proxy::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy);
    let budget=ControlBudget::new(ControlBudgetLimits::from_runtime(&zero_config::RuntimeOptionsConfig::default()));
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut client,server)=tokio::io::duplex(4096);
    let task=tokio::spawn(super::super::handle_ipc_connection_with_budget_until(server,handle,CancellationToken::new(),budget.clone()));
    let request=serde_json::json!({"type":"command","id":"same-probe","method":"diagnostics.probe_outbound","params":{"target_tag":"target","url":format!("http://{}/held",listener.local_addr().unwrap())}});
    client.write_all(format!("{request}\n").as_bytes()).await.unwrap();
    let (mut peer,_)=listener.accept().await.unwrap();
    let mut request=Vec::new();
    loop {
        let byte=peer.read_u8().await.unwrap();request.push(byte);
        if request.ends_with(b"\r\n\r\n"){break;}
    }
    assert!(request.starts_with(b"HEAD /held HTTP/1.1\r\n"));
    let mut byte=[0];
    {
    let reading = peer.read(&mut byte);
    tokio::pin!(reading);
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(reading.as_mut(), cx).is_pending(),
            "the actual original probe is waiting for peer response");
        std::task::Poll::Ready(())
    }).await;
    }
    assert!(!task.is_finished());
    assert_eq!(originals.snapshot().charged,1);
    assert_eq!(budget.usage().pending_commands,1);
    assert!(budget.usage().request_bytes>0);
    // Actual IPC EOF, rather than a test-only task abort or maintenance stop.
    drop(client);
    task.await.unwrap().unwrap();
    assert_eq!(peer.read(&mut byte).await.unwrap(),0,"real TCP peer EOF is independent of accounting closure");
    // This existing Root only removes the same original after its actual
    // Native/Users and caller Scope registration ACK gates have all settled.
    assert_eq!(originals.snapshot().charged,0);
    assert!(originals.failures().is_empty());
    assert_eq!(budget.usage().pending_commands,0);
    assert_eq!(budget.usage().request_bytes,0);
    assert_eq!(budget.usage().response_bytes,0);
    assert_eq!(budget.usage().connections,0);
    budget.close_admission();budget.confirm_released().unwrap();
 }).await.unwrap();
}
