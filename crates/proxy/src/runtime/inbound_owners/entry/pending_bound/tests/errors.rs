use super::*;

#[tokio::test]
async fn actual_tcp_close_preserves_genuine_preparation_failure_until_supervisor_consumes_it() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let listener = zero_platform_tokio::TokioListener::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, _) = watch::channel(false);
    let owner = proxy
        .inbound_owners
        .install_listener("failed-cold", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    // Even Interrupted is a genuine failure unless this host issued its exact
    // private phase/readiness marker. No string, ErrorKind or Reason guess.
    let error = owner.retain_unexecuted_bound(
        crate::protocol_registry::BoundInbound::Tcp(listener),
        std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "external preparation failure",
        )
        .into(),
    );
    let failure = original(&error);
    assert!(!crate::runtime::inbound_owners::preparation_stopped(&error));
    assert!(Arc::ptr_eq(
        owner.failure.lock().unwrap().as_ref().unwrap(),
        &failure
    ));
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("failed-cold", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().joined_tasks(), 0);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert!(Arc::ptr_eq(
        &receipt.terminal().failure().unwrap(),
        &failure
    ));
    assert!(TcpListener::bind(address).await.is_ok());
    let identity = owner.identity();
    // A publication failure keeps exactly the same native proof/source/owner.
    proxy
        .inbound_owners
        .release_closed_reported(receipt, |_| {
            Err(std::io::Error::other("supervisor publication interrupted").into())
        })
        .unwrap_err();
    assert_eq!(
        proxy
            .inbound_owners
            .owner("failed-cold")
            .unwrap()
            .identity(),
        identity
    );
    let retry = proxy
        .inbound_owners
        .confirm_shutdown("failed-cold", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.terminal().closed_io_receipts(), 1);
    let released = proxy
        .inbound_owners
        .release_closed_reported(retry, |terminal| {
            assert!(Arc::ptr_eq(&terminal.failure().unwrap(), &failure));
            Ok(())
        })
        .unwrap_err();
    assert_eq!(
        crate::runtime::inbound_owners::released_fault_identity(&released),
        Some(identity)
    );
    assert!(proxy.inbound_owners.owner("failed-cold").is_none());
}

#[tokio::test]
async fn normal_phase_stop_marker_cannot_hide_an_unsealed_registration_or_nested_task_failure() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let (stop, _) = watch::channel(false);
    let owner = proxy
        .inbound_owners
        .install_listener("unsealed", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    let error = owner.retain_failed_driver_installation(owner.ready_stopped());
    assert!(crate::runtime::inbound_owners::preparation_stopped(&error));
    assert!(owner.failure.lock().unwrap().is_none());
    let cause = original(&error);
    let unknown = proxy
        .inbound_owners
        .confirm_shutdown("unsealed", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(Arc::ptr_eq(&original(&unknown), &cause));
    assert!(!crate::runtime::inbound_owners::preparation_stopped(
        &unknown
    ));
    assert!(owner.terminal().is_none());
    assert!(owner.report.lock().unwrap().closed_io.is_empty());
    assert_eq!(*owner.joined_count.lock().unwrap(), 0);
    let (stop, _) = watch::channel(false);
    let task_owner = proxy
        .inbound_owners
        .install("actual-task", &proxy.protocols, 0, stop)
        .unwrap()
        .owner();
    task_owner.spawn_driver(async move { Err(error) }).unwrap();
    let task_error = task_owner.observe_driver().await.unwrap_err();
    assert!(!crate::runtime::inbound_owners::preparation_stopped(
        &task_error
    ));
    let failures = task_owner.task_failures.lock().unwrap();
    assert_eq!(failures.len(), 1);
    let task = failures.values().next().unwrap();
    assert_eq!(Some(&task.id), task_owner.driver.get());
    assert_eq!(
        task.class,
        zero_platform_tokio::contract::scope::TaskClass::Critical
    );
}
