//! Main exit classification consumes the same real IO task and original Scope.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
async fn eof(stopping: bool) {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(
            r#"{"runtime":{"system_tun_task_limit":2},"route":{"final":{"type":"direct"}}}"#,
        )
        .unwrap(),
    )
    .unwrap();
    let (lease, metadata) = super::system_tun_budget::reserve(&proxy);
    let owner = lease.owner();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut remote = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut io, _) = listener.accept().await.unwrap();
    let (entered, pending) = tokio::sync::oneshot::channel();
    owner
        .spawn_driver(async move {
            let mut entered = Some(entered);
            let mut byte = [0];
            let read = io.read(&mut byte);
            tokio::pin!(read);
            let bytes = std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        let _ = entered.send(());
                    }
                }
                result
            })
            .await?;
            assert_eq!(bytes, 0, "same native TCP read observed actual EOF");
            Ok(())
        })
        .unwrap();
    timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    let witness = owner.task_access().source_main_witness().await.unwrap();
    if stopping {
        owner.task_access().request_stop();
    }
    remote.shutdown().await.unwrap();
    let result = timeout(Duration::from_secs(2), owner.observe_system_tun(None))
        .await
        .unwrap();
    assert_eq!(owner.driver_was_stopping.get(), Some(&stopping));
    let joined = witness
        .try_joined()
        .expect("same original observer consumed main native join");
    assert_eq!(Some(joined.id()), witness.task_id());
    assert!(
        joined.result().is_ok(),
        "unexpected successful exit is a supervision fault, not a fabricated failed TaskExit"
    );
    if stopping {
        result.unwrap();
        assert!(owner.failure.lock().unwrap().is_none());
    } else {
        assert!(result.is_err());
        assert!(
            format!("{:?}", owner.failure.lock().unwrap().as_ref().unwrap())
                .contains("InboundTaskExited")
        );
    }
    assert!(metadata.original().is_none());
    assert!(metadata.driver().is_none());
    drop(lease);
    let registry = &proxy.inbound_owners;
    let receipt = registry
        .confirm_shutdown(
            crate::runtime::inbound_owners::system_tun::SYSTEM_TUN_OWNER,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().identity(), owner.identity());
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert_eq!(
        receipt.terminal().closed_io_receipts(),
        0,
        "this fixture never claims native TUN readiness or cleanup"
    );
    assert_eq!(receipt.terminal().failure().is_some(), !stopping);
    registry.release_closed(receipt, |_| Ok(())).unwrap();
}
#[tokio::test]
async fn system_tun_actual_main_eof_retains_unexpected_success_fault_without_failing_native_task() {
    eof(false).await;
}
#[tokio::test]
async fn system_tun_actual_main_stop_before_eof_keeps_original_successful_join() {
    eof(true).await;
}
