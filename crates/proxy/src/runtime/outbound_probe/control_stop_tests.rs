//! Actual public control stop shares one host probe and waits its real cleanup.
use super::*;
use crate::{Proxy, ProxyHandle};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::CancellationToken;

#[tokio::test]
async fn explicit_control_stop_detaches_only_its_waiter_and_retains_same_native_until_real_scope_ack(
) {
    timeout(Duration::from_secs(5), async {
        let config = zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"target","protocol":{"type":"direct"}}],"runtime":{"diagnostic_max_operations":1},"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let running = proxy.spawn();
        proxy.orchestration_ready.subscribe().wait_for(|ready| *ready).await.unwrap();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let command = zero_api::DiagnosticsProbeOutboundCommand {
            target_tag: "target".into(), url: Some(format!("http://{}/held", listener.local_addr().unwrap())), operation_id: None,
        };
        let first_stop = CancellationToken::new();
        let first_handle = handle.clone();
        let first_command = command.clone();
        let first_signal = first_stop.clone();
        let first = tokio::spawn(async move { first_handle.execute_diagnostics_probe_outbound_command_until(first_command, first_signal).await });
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        assert!(peer.read(&mut request).await.unwrap() > 0);
        let plan = proxy.protocols.current_components(proxy.engine().runtime_snapshot().config()).unwrap();
        let slot = plan.configured("target").unwrap();
        let owner = plan.original_carrier_owner(slot.instance_id(), 0).unwrap();
        let native = owner.observe().unwrap().slots()[0].original().clone();
        let runtime = OutboundProbeRuntime::new(proxy.tcp_runtime_services());
        let second_stop = CancellationToken::new();
        let second_signal = second_stop.clone();
        let second = tokio::spawn(async move { handle.execute_diagnostics_probe_outbound_command_until(command, second_signal).await });
        let publication = loop {
            let original = runtime.shared_probes.lock().unwrap().iter().find_map(|(key, entry)|
                (key.host_identity == Arc::as_ptr(&proxy.host) as usize && entry.waiters == 2).then(|| entry.original.clone()));
            if let Some(original) = original { break original; }
            tokio::task::yield_now().await;
        };
        let call = publication.call.lock().unwrap().as_ref().unwrap().clone();
        let witness = call.witness().unwrap();
        first_stop.cancel();
        loop {
            let one = runtime.shared_probes.lock().unwrap().iter().any(|(key, entry)|
                key.host_identity == Arc::as_ptr(&proxy.host) as usize && entry.waiters == 1);
            if one { break; }
            tokio::task::yield_now().await;
        }
        assert!(!first.is_finished() && !second.is_finished());
        assert!(native.closed().is_none() && !witness.is_joined());
        let mut byte = [0];
        {
            let read = peer.read(&mut byte);
            tokio::pin!(read);
            assert!(futures_util::poll!(&mut read).is_pending(), "other actual caller keeps the same native IO");
        }
        assert_eq!(owner.observe().unwrap().retained(), 1);
        second_stop.cancel();
        for task in [first, second] {
            let response = task.await.unwrap().unwrap();
            assert!(response.accepted);
            assert_eq!(response.result.unwrap()["reachable"], false);
        }
        let joined = witness.try_joined().expect("same original probe joined");
        assert!(witness.matches_joined(&joined) && joined.registration_released());
        let closed = native.closed().expect("actual original Native close");
        assert!(native.matches_closed(&closed));
        assert_eq!(owner.observe().unwrap().retained(), 0);
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0, "wire EOF is checked separately from original close");
        running.shutdown().await.unwrap();
        assert_eq!(proxy.host.original_budgets().carrier.snapshot().charged, 0);
    }).await.unwrap();
}
