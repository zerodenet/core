//! An actual Source main consumes its budget before native driver acquisition.
use super::*;
use crate::{
    component::ComponentRecipe,
    runtime::{
        inbound_owners::system_tun::SYSTEM_TUN_OWNER,
        route_runtime::{InboundListenerRuntime, SharedIngressRuntimeServices},
    },
    Proxy,
};
use std::{error::Error, time::Duration};
fn contains_capacity(mut source: &(dyn Error + 'static)) -> bool {
    loop {
        if source
            .downcast_ref::<ChannelError>()
            .is_some_and(|source| source.reason == Reason::Capacity)
        {
            return true;
        }
        if let Some(zero_engine::EngineError::Io(original)) =
            source.downcast_ref::<zero_engine::EngineError>()
        {
            source = original;
        } else if let Some(original) = source.downcast_ref::<std::io::Error>() {
            match original.get_ref() {
                Some(original) => source = original.as_ref(),
                None => return false,
            }
        } else if let Some(original) = source.downcast_ref::<Arc<ChannelError>>() {
            source = original.as_ref();
        } else {
            match source.source() {
                Some(parent) => source = parent,
                None => return false,
            }
        }
    }
}
#[tokio::test]
async fn one_actual_main_slot_rejects_native_tun_driver_before_any_native_acquisition() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{"runtime":{"system_tun_task_limit":1},"route":{"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    let proxy = Proxy::new(config).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut ready = proxy.orchestration_readiness();
    let running = proxy.spawn();
    tokio::time::timeout_at(deadline, ready.wait_for(|ready| *ready))
        .await
        .unwrap()
        .unwrap();
    let registry = proxy.system_tun_source_registry();
    let io = super::super::statistics::TunTraffic::prepare(proxy.engine(), "budget-tun");
    let addresses = vec![(
        "10.66.0.1".parse().unwrap(),
        "255.255.255.0".parse().unwrap(),
    )];
    let options = SystemTunSourceOptions::new(
        "budget.tun.original".into(),
        PacketDeviceBinding::Create {
            name: None,
            addresses: addresses.clone(),
            mtu: 1500,
        },
        addresses,
        1500,
        "budget-tun".into(),
        NonZeroUsize::new(
            proxy
                .engine()
                .config()
                .runtime
                .effective_system_tun_task_limit(
                crate::runtime::inbound_operation::raw_ip::PacketSourceContext::device_task_limit()
                    + 1,
            ),
        )
        .unwrap(),
        io,
    )
    .unwrap();
    let state = options.state();
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let lease = registry
        .reserve_system_tun(
            &proxy.protocols,
            ComponentRecipe::from_validated(Arc::new(SystemTunSourceFactory), options.clone()),
            shutdown,
        )
        .unwrap();
    let original = lease.owner();
    let runtime = InboundListenerRuntime::new(
        SharedIngressRuntimeServices::new(
            proxy
                .tcp_runtime_services()
                .for_inbound("budget-tun".into()),
        ),
        "budget-tun".into(),
        None,
    );
    let owner = lease
        .start(
            runtime,
            receiver,
            options.ingress_options(false, Arc::default()),
            None,
        )
        .unwrap();
    assert!(Arc::ptr_eq(&owner, &original));
    let failure = tokio::time::timeout_at(deadline, owner.wait_ready())
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        contains_capacity(&failure),
        "original registration failure: {failure:?}"
    );
    assert!(
        state.driver().is_none(),
        "native driver registration was rejected"
    );
    assert!(
        state.original().is_none(),
        "no original native device was acquired or loaned"
    );
    if let Some(receipt) = registry
        .confirm_shutdown(SYSTEM_TUN_OWNER, deadline)
        .await
        .unwrap()
    {
        assert_eq!(receipt.terminal().identity(), owner.identity());
        registry.release_closed(receipt, |_| Ok(())).unwrap();
    }
    let terminal = owner
        .terminal()
        .expect("same actual main joined and unused native declarations retired");
    assert_eq!(terminal.identity(), original.identity());
    assert!(terminal.joined_tasks() >= 1);
    assert!(state.driver().is_none());
    let stopped = tokio::time::timeout_at(deadline, running.shutdown())
        .await
        .unwrap();
    if let Err(source) = stopped {
        assert!(
            contains_capacity(&source),
            "actual retained Source fault: {source:?}"
        );
    }
}

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[tokio::test]
#[ignore = "requires actual native TUN privileges/backend; never substitute a regular file or duplex IO"]
async fn ordinary_native_system_tun_closes_its_original_device_after_source_scope_joins() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let mut ready = proxy.orchestration_readiness();
    let running = proxy.spawn();
    tokio::time::timeout(Duration::from_secs(5), ready.wait_for(|ready| *ready))
        .await
        .unwrap()
        .unwrap();
    let name = format!("zr2{}", std::process::id());
    proxy
        .start_tun(
            crate::inbound::tun::TunInterfaceOptions {
                name: Some(&name),
                addr: "10.66.0.1/24",
                mask: "",
                secondary_addr: None,
            },
            1500,
            "native-tun",
            crate::inbound::tun::TunRuntimeOptions {
                auto_route: false,
                include_cidrs: vec![],
                exclude_cidrs: vec![],
                dual_stack: false,
                strict_route: false,
                dns_hijack: false,
            },
        )
        .await
        .unwrap();
    let original_owner = proxy
        .system_tun_source_registry()
        .owner(SYSTEM_TUN_OWNER)
        .unwrap();
    let completion = proxy
        .tun_control
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .completion
        .clone();
    let original = completion
        .source_original()
        .expect("actual ordinary Source device original");
    assert_eq!(original.instance(), original_owner.identity());
    assert!(original.closed().is_none());
    assert!(!original.driver().is_joined());
    proxy.stop_tun().await.unwrap();
    assert!(completion.resources_complete().await);
    assert!(original
        .closed()
        .expect("matching original native SDK receipt")
        .matches(&original));
    assert!(original.driver().is_joined());
    let terminal = original_owner
        .terminal()
        .expect("same actual Source terminal");
    assert_eq!(terminal.identity(), original_owner.identity());
    assert!(terminal.joined_tasks() >= 4);
    assert!(terminal.closed_io_receipts() > 0);
    assert!(proxy.tun_control.lock().unwrap().is_none());
    running.shutdown().await.unwrap();
}
