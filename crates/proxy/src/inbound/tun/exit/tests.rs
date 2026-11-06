//! State-only projection. No snapshot here claims Source Ready or native TUN IO.
use super::*;
use crate::{
    component::ComponentRecipe,
    inbound::tun::{
        completion::TunCompletion,
        source::{SystemTunSourceFactory, SystemTunSourceOptions},
    },
};
use std::{num::NonZeroUsize, time::Duration};
use zero_platform_tokio::contract::component::PacketDeviceBinding;
fn snapshot(id: u64) -> TunInfo {
    TunInfo {
        id,
        name: "not-acquired".into(),
        addr: "10.66.0.1/24".into(),
        addresses: vec![],
        mtu: 1500,
        tag: "projection-only".into(),
        auto_route: false,
        include_cidrs: vec![],
        exclude_cidrs: vec![],
        dual_stack: false,
        strict_route: false,
        dns_hijack: false,
        dns_hijacked_queries: Arc::default(),
        healthy: false,
        last_error: Some("not ready".into()),
        egress_interface: None,
        egress_interface_v4: None,
        egress_interface_v6: None,
        route_exclusions: vec![],
        managed_config: None,
    }
}
async fn projection(failed: bool) {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let addresses = vec![(
        "10.66.0.1".parse().unwrap(),
        "255.255.255.0".parse().unwrap(),
    )];
    let options = SystemTunSourceOptions::new(
        "projection.original".into(),
        PacketDeviceBinding::Create {
            name: None,
            addresses: addresses.clone(),
            mtu: 1500,
        },
        addresses,
        1500,
        "projection-only".into(),
        NonZeroUsize::new(2).unwrap(),
        crate::inbound::tun::statistics::TunTraffic::prepare(proxy.engine(), "projection-only"),
    )
    .unwrap();
    let metadata = options.state();
    let registry = proxy.system_tun_source_registry();
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let lease = registry
        .reserve_system_tun(
            &proxy.protocols,
            ComponentRecipe::from_validated(Arc::new(SystemTunSourceFactory), options),
            shutdown.clone(),
        )
        .unwrap();
    let original = lease.owner();
    let completion = TunCompletion::for_source(registry, original.clone(), None, metadata.clone());
    *proxy.tun_control.lock().unwrap() = Some(TunControl {
        id: 17,
        shutdown: shutdown.clone(),
        completion: completion.clone(),
        #[cfg(feature = "raw-ip-runtime")]
        budget_recovery: None,
        route_recovery: None,
    });
    *proxy.tun_info.lock().unwrap() = Some(snapshot(17));
    let projection = proxy.system_tun_exit_projection(true);
    let mut failure_events = proxy.configured_tun_failures.subscribe();
    let failure = EngineError::InboundTaskExited;
    let unrelated = proxy.protocols.allocate_resource_identity().unwrap();
    projection.record(unrelated, Some(&failure));
    assert!(!*shutdown.borrow());
    assert_eq!(proxy.tun_info.lock().unwrap().as_ref().unwrap().id, 17);
    assert!(proxy.tun_last_error.lock().unwrap().is_none());
    assert!(matches!(
        failure_events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    projection.record(original.identity(), failed.then_some(&failure));
    assert!(*shutdown.borrow());
    assert!(proxy.tun_info.lock().unwrap().is_none());
    assert_eq!(proxy.tun_last_error.lock().unwrap().is_some(), failed);
    if failed {
        assert_eq!(failure_events.try_recv().unwrap(), failure.to_string());
    } else {
        assert!(matches!(
            failure_events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }
    assert!(
        Arc::ptr_eq(
            &proxy
                .tun_control
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .completion,
            &completion
        ),
        "state event cannot discard the original native/Scope cleanup controller"
    );
    assert!(
        original.terminal().is_none(),
        "projection cannot issue a terminal receipt"
    );
    assert!(metadata.original().is_none());
    assert!(metadata.driver().is_none());
    drop(lease);
    completion
        .wait_main(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(completion.resources_complete().await);
    let terminal = original.terminal().unwrap();
    assert_eq!(terminal.identity(), original.identity());
    assert_eq!(
        terminal.joined_tasks(),
        0,
        "cold projection did not invent a main task"
    );
    assert_eq!(
        terminal.closed_io_receipts(),
        0,
        "no native acquisition was attempted"
    );
}
#[tokio::test]
async fn original_source_failure_projection_retains_cleanup_and_rejects_unrelated_identity() {
    projection(true).await;
}
#[tokio::test]
async fn original_source_normal_stop_projection_does_not_invent_failure() {
    projection(false).await;
}
