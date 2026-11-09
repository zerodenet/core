use super::*;
use std::{error::Error, num::NonZeroUsize};
use zero_platform_tokio::contract::{
    lifecycle::Unfinished,
    scope::{TaskClass, TaskScope},
    CleanupFailureReceipt, Impact, Reason, Stage,
};
#[tokio::test]
async fn partial_report_delivery_keeps_actual_source_and_cannot_ack_hosted_scope() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let original = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
        let weak = Arc::downgrade(&original);
        let receipt = CleanupFailureReceipt::new();
        assert!(receipt.bind_payload(original.clone()));
        let source = Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(original.clone())
                .with_cleanup_failure_receipt(receipt.clone()),
        );
        let error_weak = Arc::downgrade(&source);
        let mut owner =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        owner.enable_registration().unwrap();
        let error = source.clone();
        let (id, witness) = owner
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                Err(ChannelError::retained(error))
            })
            .unwrap();
        let exit = owner.next_receipt().await.unwrap();
        let actual = exit.result().unwrap_err();
        assert_eq!(exit.id(), id);
        let receiver = Arc::new(ReportReceiver::default());
        let mut partial = CloseReport::default();
        partial.failed_tasks.push(TaskFailure {
            id: exit.id(),
            class: exit.class(),
            error: ChannelError::retained(actual.clone()),
        });
        partial.unfinished.push(Unfinished::Task(id));
        let observed = receiver.receive("actual hosted report", partial);
        assert!(!observed.complete);
        let first = observed.into_result().unwrap_err();
        assert!(
            receipt.is_acknowledged(),
            "same source is strong in the actual receiver before local delivery"
        );
        owner.begin_shutdown();
        assert!(
            !owner.is_drained(),
            "local source delivery is not Host registration ACK"
        );
        assert!(!witness.try_joined().unwrap().registration_released());
        let mut complete = CloseReport::default();
        complete.failed_tasks.push(TaskFailure {
            id,
            class: exit.class(),
            error: ChannelError::retained(actual.clone()),
        });
        let observed = receiver.receive("actual hosted report", complete);
        assert!(
            observed.complete,
            "historical fault is independent of physical completion"
        );
        let returned = observed.into_result().unwrap_err();
        assert_eq!(
            receiver.0.lock().unwrap().tasks.len(),
            1,
            "redelivery retains one actual original row"
        );
        let retained = returned
            .get_ref()
            .unwrap()
            .downcast_ref::<ReportFailure>()
            .unwrap();
        assert!(retained
            .source()
            .unwrap()
            .downcast_ref::<ChannelError>()
            .is_some());
        owner.acknowledge(&exit).unwrap();
        assert!(owner.is_drained());
        assert!(witness.try_joined().unwrap().registration_released());
        drop(first);
        drop(exit);
        drop(witness);
        drop(actual);
        drop(source);
        drop(original);
        drop(owner);
        drop(receiver);
        assert!(
            weak.upgrade().is_some(),
            "the returned actual failure retains the native source after owner retirement"
        );
        assert!(error_weak.upgrade().is_some());
        drop(returned);
        assert!(error_weak.upgrade().is_none());
        // The separately retained receipt itself owns its sealed payload.
        drop(receipt);
        assert!(weak.upgrade().is_none(), "no receiver/source cycle");
    })
    .await
    .unwrap();
}
#[cfg(unix)]
#[tokio::test]
async fn fault_delivery_cannot_close_the_actual_unix_original_or_release_its_scope() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use zero_platform_tokio::{
        contract::{lifecycle::ChannelControl, CancellationToken},
        PendingUnixConnect,
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let (mut pending, mut native) =
            PendingUnixConnect::prepare(&path, CancellationToken::new()).unwrap();
        let mut client = pending
            .connect(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let os = Arc::new(std::net::TcpListener::bind(held.local_addr().unwrap()).unwrap_err());
        let source = Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component).with_source(os),
        );
        let captured = source.clone();
        scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                Err(ChannelError::retained(captured))
            })
            .unwrap();
        let report = scope
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await;
        assert!(report.is_complete());
        let receiver = Arc::new(ReportReceiver::default());
        let observed = receiver.receive("actual Scope", report);
        assert!(observed.complete);
        let returned = observed.into_result().unwrap_err();
        assert!(
            !native.is_cancelled(),
            "source reception cannot cancel or close unrelated original IO"
        );
        client.write_all(b"still original").await.unwrap();
        let mut bytes = [0; 14];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"still original");
        drop(client);
        drop(pending);
        let closed = native
            .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await
            .unwrap();
        assert!(closed.is_complete());
        assert!(!closed.closed_io.is_empty());
        drop(returned);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_grpc_shutdown_retires_native_and_keeps_complete_scope_failure() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let owner = crate::prepare(
            "127.0.0.1:0".parse().unwrap(),
            crate::GrpcServerSecurity::default(),
        )
        .unwrap();
        let listener = owner
            .originals
            .original
            .lock()
            .await
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        drop(listener);
        let directory = tempfile::tempdir().unwrap();
        let actual = Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(std::fs::File::open(directory.path().join("missing")).unwrap_err()),
        );
        let weak = Arc::downgrade(&actual);
        let source = actual.clone();
        let (_, witness) = owner
            .originals
            .scope
            .lock()
            .await
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                Err(ChannelError::retained(source))
            })
            .unwrap();
        let returned = owner.shutdown().await.unwrap_err();
        assert!(
            owner.retirement_complete(),
            "complete native/Scope reports retire despite the received fault"
        );
        assert!(witness.try_joined().unwrap().registration_released());
        drop(actual);
        drop(witness);
        drop(owner);
        assert!(
            weak.upgrade().is_some(),
            "the returned failure retains the same source after owner Drop"
        );
        drop(returned);
        assert!(weak.upgrade().is_none());
    })
    .await
    .unwrap();
}
