//! Ordinary URLTest -> original Host ProbeCall -> standard Direct native I/O.
use crate::Proxy;
use std::{future::Future, task::Poll, time::Duration};
use tokio::io::AsyncReadExt;
#[tokio::test]
async fn removing_a_pending_urltest_group_retires_its_original_host_probe_and_native_peer() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address=listener.local_addr().unwrap();
        let (opened,pending)=tokio::sync::oneshot::channel();
        let native_peer=tokio::spawn(async move {
            let (mut io,_)=listener.accept().await.unwrap();
            let mut headers=Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(io.read_u8().await.unwrap());
                assert!(headers.len()<16384);
            }
            assert!(headers.starts_with(b"GET /probe "));
            let mut byte=[0];let mut read=std::pin::pin!(io.read(&mut byte));
            std::future::poll_fn(|cx| {assert!(read.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
            opened.send(()).unwrap();
            assert_eq!(read.await.unwrap(),0,"that original probe connection closes after group retirement");
        });
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"node","protocol":{"type":"direct"}}],
            "outbound_groups":[{"tag":"auto","type":"url_test","outbounds":["node"],"url":format!("http://{address}/probe"),"interval_seconds":60}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=Proxy::new(config).unwrap();let running=proxy.spawn();
        pending.await.unwrap();
        let replacement=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"node","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone())
            .apply_runtime_config_and_wait(replacement,Duration::from_secs(5)).await.unwrap();
        native_peer.await.unwrap();
        let host=proxy.protocols.runtime_host();
        host.drain_probes(tokio::time::Instant::now()+Duration::from_secs(3)).await.unwrap();
        assert_eq!(host.original_budgets().carrier.snapshot().charged,0,
            "actual Host probe drain consumed original native/caller receipts; no port-rebind proxy");
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
