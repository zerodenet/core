//! A cancelled public cold diagnostic leaves real cleanup with the host task.
use super::*;
use crate::runtime::{tcp_ingress::TcpIngressRuntime, Proxy};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    time::{timeout, Duration, Instant},
};
use zero_platform_tokio::contract::{ChannelError, Reason};

fn stale(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(channel) = error.downcast_ref::<ChannelError>() {
        if channel.reason == Reason::StalePlan {
            return true;
        }
    }
    if let Some(zero_engine::EngineError::Io(native)) =
        error.downcast_ref::<zero_engine::EngineError>()
    {
        return stale(native);
    }
    if let Some(native) = error.downcast_ref::<std::io::Error>() {
        // std::io::Error::source may delegate through the payload's source,
        // skipping the actual ChannelError itself. Inspect its real payload.
        if let Some(payload) = native.get_ref() {
            return stale(payload);
        }
    }
    if let Some(shared) = error.downcast_ref::<std::sync::Arc<ChannelError>>() {
        return stale(shared.as_ref());
    }
    error.source().is_some_and(stale)
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
    let owner = plan
        .original_carrier_owner(direct.instance_id(), 0)
        .unwrap();
    let active = owner.observe().unwrap();
    assert_eq!(active.retained(), 1);
    let original = active.slots()[0].original().clone();
    assert!(original.closed().is_none());
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
    assert!(
        stale(&failed),
        "actual Business admission error: {failed:?}"
    );
    proxy
        .engine()
        .track_session(session.id)
        .finish(zero_engine::SessionOutcome::Cancelled);
    let mut joining = Box::pin(proxy.host.apply().wait_idle());
    assert!(futures_util::poll!(joining.as_mut()).is_pending());
    let mut eof = [0; 1];
    {
        let read = peer.read(&mut eof);
        tokio::pin!(read);
        assert!(
            futures_util::poll!(read.as_mut()).is_pending(),
            "the actual Service stream remains in use with its HTTP response withheld"
        );
    }
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    let (joined, read) = timeout(Duration::from_secs(2), async {
        tokio::join!(joining, peer.read(&mut eof))
    })
    .await
    .unwrap();
    joined.unwrap();
    assert_eq!(read.unwrap(), 0);
    let closed = original
        .closed()
        .expect("actual Service probe socket closed");
    assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
    assert!(owner.closed().unwrap().matches(&owner));
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
