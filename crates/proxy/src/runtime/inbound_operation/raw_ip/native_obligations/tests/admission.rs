//! Real caller Scope capacity before metadata/Business submission.
use super::*;
use crate::runtime::{inbound_owners::InboundOwnerRegistry, Proxy};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[tokio::test]
async fn rejected_pre_admission_metadata_waits_actual_caller_join_without_poisoning_same_source() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("metadata-source", &proxy.protocols, 1, stop)
        .unwrap();
    let group = installed.child_tasks(NonZeroUsize::MIN).unwrap();
    let host = installed.tasks();
    let owner = installed.owner();
    let native = NativeObligations::new(1, 0, 0);
    installed.register(native.resource()).unwrap();
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (entered, observed) = oneshot::channel();
    owner
        .spawn_driver(async move {
            let read = reader.read_u8();
            tokio::pin!(read);
            let mut entered = Some(entered);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await?;
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    let monitor = {
        let owner = owner.clone();
        tokio::spawn(async move { owner.observe_listener().await })
    };
    let mut permit = native.reserve(ChildKind::Tcp).unwrap();
    let registration = permit.registration();
    let (sent, received) = oneshot::channel();
    group
        .spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            permit
                .bind_caller(binding.clone())
                .map_err(std::io::Error::other)?;
            permit.enter().map_err(std::io::Error::other)?;
            let phase = permit
                .registration()
                .begin_session_admission()
                .map_err(std::io::Error::other)?;
            let source = binding
                .lookup_local_process(
                    "127.0.0.1:1".parse().unwrap(),
                    zero_platform_tokio::LocalProcessTransport::Tcp,
                    NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
                )
                .await
                .unwrap_err();
            assert_eq!(source.stage, Stage::Prepare);
            assert_eq!(source.reason, Reason::Capacity);
            assert_eq!(
                source.delivery,
                zero_platform_tokio::contract::DeliveryState::NotSubmitted
            );
            drop(phase); // Ends only this actual phase; not a close/join proof.
            sent.send(binding).unwrap();
            permit.consume_business_result(Err(std::io::Error::other(source).into()))
        })
        .unwrap();
    let binding = received.await.unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    binding.wait_joined(end).await.unwrap();
    let report = registration.caller_witness().unwrap().try_joined().unwrap();
    assert!(report.result().is_ok());
    assert!(
        binding.process_retired(),
        "the actual registrar rejected before any blocking/native submission"
    );
    native.settle_joined(end).await.unwrap();
    assert!(native.0.lock().unwrap().records.is_empty());
    let next = native.reserve(ChildKind::Tcp).unwrap();
    drop(next);
    assert!(!host.cancellation().is_cancelled());
    assert!(
        !monitor.is_finished(),
        "new-flow capacity did not stop the original pending Source main"
    );
    owner.request_stop();
    writer.write_u8(1).await.unwrap();
    tokio::time::timeout_at(end, monitor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let receipt = registry
        .confirm_shutdown("metadata-source", end)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().identity(), owner.identity());
    assert!(receipt.terminal().failure().is_none());
    assert_eq!(receipt.terminal().joined_tasks(), 2);
    registry.release_closed(receipt, |_| Ok(())).unwrap();
}

#[tokio::test]
async fn in_flight_pre_gateway_cancellation_waits_for_caller_and_settles_on_shutdown() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, _) = watch::channel(false);
    let installed = registry
        .install("pre-gateway-cancel", &proxy.protocols, 1, stop)
        .unwrap();
    let group = installed.child_tasks(NonZeroUsize::MIN).unwrap();
    let host = installed.tasks();
    let owner = installed.owner();
    let native = NativeObligations::new(1, 0, 0);
    installed.register_after_scope(native.resource()).unwrap();
    let (mut reader, _writer) = tokio::io::duplex(1);
    let cancel_driver = host.cancellation();
    owner
        .spawn_driver(async move {
            tokio::select! {
                result = reader.read_u8() => { result?; Ok(()) }
                _ = cancel_driver.cancelled() => Ok(()),
            }
        })
        .unwrap();
    owner.seal_listener_installation();
    let mut permit = native.reserve(ChildKind::Tcp).unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    group
        .spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            let stop = binding.cancellation();
            permit.bind_caller(binding).map_err(std::io::Error::other)?;
            permit.enter().map_err(std::io::Error::other)?;
            let phase = permit
                .registration()
                .begin_session_admission()
                .map_err(std::io::Error::other)?;
            phase.admitted();
            entered_tx.send(()).unwrap();
            stop.cancelled().await;
            permit.registration().note_no_gateway_opened();
            permit.consume_business_result(Ok(()))
        })
        .unwrap();
    entered_rx.await.unwrap();
    let end = Instant::now() + Duration::from_secs(5);
    let receipt = registry
        .confirm_shutdown("pre-gateway-cancel", end)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().identity(), owner.identity());
    assert!(receipt.terminal().failure().is_none());
    assert!(native.0.lock().unwrap().records.is_empty());
    registry.release_closed(receipt, |_| Ok(())).unwrap();
}
