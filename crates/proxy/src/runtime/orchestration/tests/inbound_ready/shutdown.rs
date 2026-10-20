use super::*;

#[tokio::test]
async fn public_shutdown_during_real_plugin_activation_retains_owner_until_child_reap_without_ready(
) {
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = reserved.local_addr().unwrap().port();
    let proxy = Proxy::new(candidate(port, gate.local_addr().unwrap().port())).unwrap();
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    drop(reserved);
    let run = proxy.run_until(async {
        let _ = shutdown.await;
    });
    tokio::pin!(run);
    let (control, _) = tokio::select! {
        result = gate.accept() => result.unwrap(),
        result = &mut run => panic!("startup ended before the real activation gate: {result:?}"),
    };
    let mut control = BufReader::new(control);
    let mut line = String::new();
    tokio::select! {
        result = control.read_line(&mut line) => { result.unwrap(); },
        result = &mut run => panic!("startup ended with child initialization pending: {result:?}"),
    }
    let pid: u32 = line.trim().parse().unwrap();
    std::future::poll_fn(|cx| {
        assert!(run.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let owner = proxy.inbound_owners.owner("ready-ss").unwrap();
    let identity = owner.identity();
    assert!(!owner.is_ready());
    assert!(!*proxy.orchestration_ready.borrow());
    assert!(proxy
        .protocols
        .current_components(&proxy.engine.config())
        .is_none());
    stop.send(()).unwrap();
    timeout(Duration::from_secs(3), &mut run)
        .await
        .unwrap()
        .unwrap();
    assert!(proxy.inbound_owners.owner("ready-ss").is_none());
    assert!(!owner.is_ready());
    assert_eq!(owner.identity(), identity);
    assert!(!*proxy.orchestration_ready.borrow());
    let status = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "stop must retain and reap the actual pending SIP003 Child"
    );
    let mut tail = Vec::new();
    timeout(Duration::from_secs(2), control.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert!(
        tail.is_empty(),
        "the stopped child must never pass the held activation gate"
    );
}

#[tokio::test]
async fn public_shutdown_during_accepted_candidate_ready_drives_original_compensation_and_child_reap(
) {
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = reserved.local_addr().unwrap().port();
    let initial = zero_config::RuntimeConfig::parse(
        r#"{
        "inbounds":[{"tag":"last-good","listen":{"address":"127.0.0.1","port":0},
            "protocol":{"type":"direct","target":"127.0.0.1","port":9}}],
        "route":{"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let (stop, shutdown) = tokio::sync::oneshot::channel();
    let run = proxy.run_until(async {
        let _ = shutdown.await;
    });
    tokio::pin!(run);
    let mut ready = proxy.orchestration_ready.subscribe();
    tokio::select! {
        _ = async { let _ = ready.wait_for(|ready| *ready).await.unwrap(); } => {},
        result = &mut run => panic!("actual initial listener must become Ready: {result:?}"),
    }
    let last_good = proxy.inbound_owners.owner("last-good").unwrap();
    last_good.wait_ready().await.unwrap();
    let last_good_id = last_good.identity();
    let mut candidate = candidate(port, gate.local_addr().unwrap().port());
    candidate.inbounds.insert(0, initial.inbounds[0].clone());
    drop(reserved);
    let applying = handle.apply_runtime_config_and_wait(candidate, Duration::from_secs(10));
    tokio::pin!(applying);
    let (control, _) = tokio::select! {
        result = gate.accept() => result.unwrap(),
        result = &mut applying => panic!("candidate acknowledged before real activation: {result:?}"),
        result = &mut run => panic!("runtime ended before real activation: {result:?}"),
    };
    let mut control = BufReader::new(control);
    let mut line = String::new();
    tokio::select! {
        result = control.read_line(&mut line) => { result.unwrap(); },
        result = &mut applying => panic!("candidate acknowledged with pending real child: {result:?}"),
        result = &mut run => panic!("runtime ended with pending real child: {result:?}"),
    }
    let pid: u32 = line.trim().parse().unwrap();
    let candidate = proxy.inbound_owners.owner("ready-ss").unwrap();
    assert!(!candidate.is_ready());
    assert_eq!(
        proxy.inbound_owners.owner("last-good").unwrap().identity(),
        last_good_id
    );
    assert!(!last_good.task_access().cancellation().is_cancelled());
    stop.send(()).unwrap();
    timeout(Duration::from_secs(4), &mut run)
        .await
        .unwrap()
        .unwrap();
    let error = timeout(Duration::from_secs(1), &mut applying)
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        error.contains("readiness") || error.contains("stopped"),
        "{error}"
    );
    assert_eq!(proxy.engine.config().as_ref(), &initial);
    assert!(proxy.inbound_owners.owner("ready-ss").is_none());
    assert!(proxy.inbound_owners.owner("last-good").is_none());
    assert!(!candidate.is_ready());
    let status = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "the original candidate Child must be reaped before stop completion"
    );
    let mut tail = Vec::new();
    timeout(Duration::from_secs(2), control.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert!(tail.is_empty());
}

#[tokio::test]
async fn already_requested_shutdown_precedes_zero_listener_startup_and_never_publishes_ready() {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let snapshot = proxy.engine.runtime_snapshot();
    proxy
        .run_until(async {
            assert!(
                !*proxy.orchestration_ready.borrow(),
                "an already-ready stop must be polled before publishing startup Ready"
            );
            assert!(proxy
                .protocols
                .current_components(snapshot.config())
                .is_none());
        })
        .await
        .unwrap();
    assert!(!*proxy.orchestration_ready.borrow());
    assert!(proxy
        .protocols
        .current_components(snapshot.config())
        .is_none());
    assert!(proxy.inbound_owners.identities().is_empty());
}
