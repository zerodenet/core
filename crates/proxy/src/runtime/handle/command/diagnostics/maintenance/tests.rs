//! A cancelled public cold diagnostic leaves real cleanup with the host task.
use super::*;
use crate::runtime::{tcp_ingress::TcpIngressRuntime, Proxy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration, Instant},
};
use zero_platform_tokio::contract::{ChannelError, Reason};

fn stale(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error
            .downcast_ref::<ChannelError>()
            .is_some_and(|error| error.reason == Reason::StalePlan)
        {
            return true;
        }
        current = error.source();
    }
    false
}
#[tokio::test]
async fn cancelled_cold_diagnostic_keeps_actual_service_owner_and_never_publishes_business() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let proxy = Proxy::new(zero_config::RuntimeConfig::parse(
        r#"{"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#,
    ).unwrap()).unwrap();
    let config = proxy.engine().config();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let waiting = tokio::spawn(async move {
        handle
            .execute_diagnostics_probe_outbound_command(zero_api::DiagnosticsProbeOutboundCommand {
                target_tag: "direct".into(),
                url: Some(format!("http://{address}/actual-cold-probe")),
                operation_id: None,
            })
            .await
    });
    let (mut peer, _) = timeout(Duration::from_secs(2), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = [0; 1024];
    assert!(peer.read(&mut bytes).await.unwrap() > 0);
    assert!(proxy.protocols.current_components(&config).is_none());
    let plan = proxy.protocols.prepare_components(config.clone()).unwrap();
    let direct = plan.direct(Some("direct")).unwrap();
    // This real kernel-admitted Business request must not borrow temporary
    // Service permission just because the role has genuinely started.
    let ingress =
        TcpIngressRuntime::new(proxy.tcp_runtime_services(), "actual-business".into(), None);
    let mut session = zero_core::Session::new(
        0,
        zero_core::Address::Ipv4([127, 0, 0, 1]),
        address.port(),
        zero_core::Network::Tcp,
        zero_core::ProtocolType::UNKNOWN,
    );
    let failed = match ingress.open_tcp_upstream(&mut session).await {
        Ok(_) => panic!("cold Service maintenance published Business admission"),
        Err(error) => error,
    };
    assert!(stale(&failed));
    proxy
        .engine()
        .track_session(session.id)
        .finish(zero_engine::SessionOutcome::Cancelled);
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    let mut joining = Box::pin(proxy.apply_owner.wait_idle());
    assert!(futures_util::poll!(joining.as_mut()).is_pending());
    let mut eof = [0; 1];
    {
        let read = peer.read(&mut eof);
        tokio::pin!(read);
        assert!(
            futures_util::poll!(read.as_mut()).is_pending(),
            "caller cancellation cannot discard the actual Service stream"
        );
    }
    peer.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    timeout(Duration::from_secs(2), joining)
        .await
        .unwrap()
        .unwrap();
    assert!(proxy.protocols.current_components(&config).is_none());
    assert!(
        direct
            .stream(Instant::now() + Duration::from_secs(1))
            .await
            .is_err(),
        "actual temporary component was explicitly shut down"
    );
    let next = proxy.protocols.prepare_components(config).unwrap();
    assert!(
        !std::sync::Arc::ptr_eq(&plan, &next),
        "next probe cannot resurrect the closed temporary generation"
    );
}
