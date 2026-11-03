use super::*;
use zero_config::RuntimeConfig;
use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};

#[tokio::test]
async fn actual_plugin_start_deadline_rejects_ready_ack_only_after_original_cleanup() {
    let initial = RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(initial).unwrap();
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let previous = state.applied_snapshot.clone();
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let candidate = candidate(
        reserved.local_addr().unwrap().port(),
        gate.local_addr().unwrap().port(),
    );
    proxy.engine.stage_runtime_config(candidate).unwrap();
    let expected = proxy.engine.runtime_snapshot();
    let (sender, mut acknowledgement) = tokio::sync::oneshot::channel();
    *proxy.host.reload_ack().lock().unwrap() = Some(PendingReloadAck {
        expected: expected.clone(),
        previous: previous.clone(),
        compensating: false,
        sender,
    });
    drop(reserved);
    let mut applying = Box::pin(state.reconcile_reload(&proxy));
    let (control, _) = tokio::select! {
        result = gate.accept() => result.unwrap(),
        _ = &mut applying => panic!("actual source must reach the child activation gate"),
    };
    let mut control = BufReader::new(control);
    let mut line = String::new();
    let pid = tokio::select! {
        result = control.read_line(&mut line) => { result.unwrap(); line.trim().parse::<u32>().unwrap() },
        _ = &mut applying => panic!("actual child initialization is still pending"),
    };
    std::future::poll_fn(|cx| {
        assert!(applying.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        acknowledgement.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let owner = proxy.inbound_owners.owner("ready-ss").unwrap();
    assert!(!owner.is_ready());
    // The actual host passes a five-second lifecycle deadline. The protocol
    // applies min(host deadline, its ten-second ceiling), including here.
    timeout(Duration::from_millis(5200), &mut applying)
        .await
        .expect("the original finite lifecycle start deadline must settle");
    drop(applying);
    let error = acknowledgement.await.unwrap().unwrap_err();
    let terminal = owner
        .terminal_for_test()
        .expect("actual original retired before rejection ACK");
    assert_eq!(
        error,
        ChannelError::new(Stage::Prepare, Reason::Timeout, Impact::Component).to_string(),
        "actual original terminal and complete error source: {terminal:#?}"
    );
    assert_eq!(terminal.identity(), owner.identity());
    assert!(terminal.joined_tasks() > 0);
    assert!(terminal.closed_io_receipts() > 0);
    let failure = terminal
        .failure()
        .expect("original preparation timeout retained");
    assert_eq!(failure.stage, Stage::Prepare);
    assert_eq!(failure.reason, Reason::Timeout);
    assert_eq!(failure.component, Some(owner.identity().component));
    assert_eq!(failure.delivery, DeliveryState::Unknown);
    assert!(!owner.is_ready());
    assert!(proxy.inbound_owners.owner("ready-ss").is_none());
    assert!(Arc::ptr_eq(&proxy.engine.runtime_snapshot(), &previous));
    assert!(Arc::ptr_eq(&state.applied_snapshot, &previous));
    assert_eq!(proxy.engine.config_revision(), previous.config_revision());
    let status = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(
        !status.success(),
        "actual timed-out SIP003 Child must be reaped"
    );
    let mut tail = Vec::new();
    timeout(Duration::from_secs(2), control.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert!(
        tail.is_empty(),
        "the timed-out child must never bind after the held gate"
    );
    state.finish(&proxy, Ok(())).await.unwrap();
}
