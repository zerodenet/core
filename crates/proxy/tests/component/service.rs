use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use zero_config::RuntimeConfig;
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::contract::context::RequestIdentity;
use zero_proxy::Proxy;

#[test]
fn service_requests_never_allocate_or_expose_business_identity() {
    let identity = RequestIdentity::Service(Arc::new(ServiceRequestFacts {
        origin: ServiceOrigin::Runtime,
        purpose: ServicePurpose::DiagnosticProbe,
    }));
    assert_eq!(identity.session(), None);
}

#[tokio::test]
async fn actual_manual_probe_uses_registered_role_and_keeps_business_registry_empty() {
    timeout(Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 512];
                let read = socket.read(&mut request).await.unwrap();
                assert!(request[..read].starts_with(b"HEAD /probe "));
                socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n").await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        let config = RuntimeConfig::parse(r#"{"outbounds":[{"tag":"selected","protocol":{"type":"direct"}},{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let handle = zero_proxy::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone(),
        );
        let running = proxy.spawn();
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(1)).await
            .expect("actual host startup and configuration acknowledgement");
        let url = format!("http://127.0.0.1:{port}/probe");
        proxy.probe_outbound_single("selected", &url).await.unwrap();
        // A named `direct` outbound must not alias the implicit kernel role.
        proxy.probe_outbound_single("direct", &url).await.unwrap();
        assert_eq!(proxy.engine().active_sessions().len(), 0);
        server.await.unwrap();
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
