//! Actual ordinary probe calls: coalescing, cancellation and original cleanup.
use super::*;
use crate::runtime::Proxy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};

#[tokio::test]
async fn shared_probe_keeps_one_original_until_last_waiter_then_refunds_after_real_cleanup() {
    timeout(Duration::from_secs(5), async {
        let config = zero_config::RuntimeConfig::parse(
            &serde_json::json!({
                "outbounds":[{"tag":"target","protocol":{"type":"direct"}}],
                "runtime":{"diagnostic_max_operations":1},"route":{"final":{"type":"direct"}}
            })
            .to_string(),
        )
        .unwrap();
        let proxy = Proxy::new(config).unwrap();
        let running = proxy.spawn();
        let mut ready = proxy.orchestration_ready.subscribe();
        ready.wait_for(|ready| *ready).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let request =
            OutboundProbeRequest::parse(&format!("http://{}/held", listener.local_addr().unwrap()))
                .unwrap();
        let runtime = OutboundProbeRuntime::new(proxy.tcp_runtime_services());
        let first_runtime = runtime.clone();
        let first_request = request.clone();
        let first = tokio::spawn(async move {
            first_runtime
                .probe_target_tag("target", &first_request)
                .await
        });
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut bytes = vec![0; request.request.len()];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, request.request.as_bytes());
        let actual_plan = proxy
            .protocols
            .current_components(proxy.engine().runtime_snapshot().config())
            .unwrap();
        let slot = actual_plan.configured("target").unwrap();
        let owner = actual_plan
            .original_carrier_owner(slot.instance_id(), 0)
            .unwrap();
        let observation = owner.observe().unwrap();
        assert_eq!(observation.retained(), 1);
        let original = observation.slots()[0].original().clone();
        assert!(original.closed().is_none());
        let second_runtime = runtime.clone();
        let second_request = request.clone();
        let second = tokio::spawn(async move {
            second_runtime
                .probe_target_tag("target", &second_request)
                .await
        });
        loop {
            let coalesced = runtime
                .shared_probes
                .lock()
                .unwrap()
                .iter()
                .any(|(key, entry)| {
                    key.host_identity == Arc::as_ptr(&proxy.host) as usize
                        && key.url == request.url
                        && entry.waiters == 2
                });
            if coalesced {
                break;
            }
            tokio::task::yield_now().await;
        }
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let mut byte = [0];
        assert!(timeout(Duration::from_millis(20), peer.read(&mut byte))
            .await
            .is_err());
        assert!(original.closed().is_none());
        assert_eq!(owner.observe().unwrap().retained(), 1);
        let refused = OutboundProbeRequest::parse(&format!(
            "http://{}/second-slot",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        assert!(
            runtime.probe_target_tag("target", &refused).await.is_err(),
            "one real active probe exhausts the configured host total"
        );
        second.abort();
        assert!(second.await.unwrap_err().is_cancelled());
        loop {
            if original
                .closed()
                .is_some_and(|closed| original.matches_closed(&closed))
                && owner.observe().unwrap().retained() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        // There is no second dial to trigger reaping. The original host
        // supervisor has already closed the actual native and returned users.
        let next_runtime = runtime.clone();
        let next =
            tokio::spawn(async move { next_runtime.probe_target_tag("target", &refused).await });
        let (mut next_peer, _) = listener.accept().await.unwrap();
        let mut request_bytes = [0; 1024];
        assert!(next_peer.read(&mut request_bytes).await.unwrap() > 0);
        next_peer
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        assert!(next.await.unwrap().is_ok());
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cold_probe_uses_original_task_and_completes_native_cleanup_before_return() {
    timeout(Duration::from_secs(5),async{
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy=Proxy::new(zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"target","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
        let called=proxy.clone();let url=format!("http://{}/cold",listener.local_addr().unwrap());
        let probe=tokio::spawn(async move{called.probe_outbound_single("target",&url).await});
        let (mut peer,_)=listener.accept().await.unwrap();let mut bytes=[0;1024];assert!(peer.read(&mut bytes).await.unwrap()>0);
        peer.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n").await.unwrap();
        assert!(probe.await.unwrap().is_ok());
        assert!(proxy.protocols.current_components(proxy.engine().runtime_snapshot().config()).is_none(),"cold Service finishes its actual component lease");
    }).await.unwrap();
}
