//! Actual task/error and file originals remain independent from report custody.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use zero_platform_tokio::{
    contract::{
        lifecycle::{TaskClass, TaskFailure, Unfinished},
        scope::TaskScope,
    },
    file::OriginalFile,
};
#[tokio::test]
async fn actual_partial_report_is_restored_when_the_next_close_await_is_cancelled() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("native-file");
        std::fs::write(&path, b"actual").unwrap();
        let mut file = OriginalFile::adopt(std::fs::File::open(path).unwrap());
        let mut scope = TaskScope::suspended_hosted(
            tokio::runtime::Handle::current(),
            std::num::NonZeroUsize::new(1),
        );
        scope.enable_registration().unwrap();
        let missing = directory.path().join("actual-task-error");
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Io,
                    "actual report producer failed",
                )
                .with_source(std::fs::File::open(missing).unwrap_err()))
            })
            .unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        let joined = receipt.joined().clone();
        assert!(witness.matches_joined(&joined));
        let error = joined.result().unwrap_err();
        let report = CloseReport {
            failed_tasks: vec![TaskFailure {
                id: joined.id(),
                class: joined.class(),
                error: BuildError::retained(error.clone()),
            }],
            unfinished: vec![Unfinished::Recovery(
                "actual receipt and next original close are held".into(),
            )],
            ..Default::default()
        };
        let storage = Arc::new(Mutex::new(CloseReport::default()));
        let actual_controller = Arc::new(tokio::sync::Mutex::new(()));
        let (release, next) = tokio::sync::oneshot::channel::<()>();
        let held_storage = storage.clone();
        let controller = actual_controller.clone();
        let mut closing = Box::pin(async move {
            let _original = controller.lock().await;
            let mut original_report = PendingReport::take(&held_storage);
            original_report.append(report);
            next.await.unwrap();
            original_report.finish()
        });
        poll_fn(|cx| {
            assert!(closing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            storage.lock().unwrap().failed_tasks.is_empty(),
            "the actual in-progress close owns its report"
        );
        drop(closing);
        drop(release);
        let _controller = actual_controller.lock().await;
        let mut retry = PendingReport::take(&storage);
        assert_eq!(retry.failed_tasks.len(), 1);
        assert_eq!(retry.failed_tasks[0].id, joined.id());
        assert_eq!(retry.failed_tasks[0].class, joined.class());
        assert!(Arc::ptr_eq(
            retry.failed_tasks[0].error.retained_source().unwrap(),
            &error
        ));
        assert!(
            !joined.registration_released(),
            "restoring a report never accepts its TaskScope receipt"
        );
        let closed = file.close().unwrap();
        assert!(file.matches_closed(&closed));
        retry
            .closed_io
            .push("same explicitly closed original file".into());
        scope.begin_shutdown();
        scope.acknowledge(&receipt).unwrap();
        drop(receipt);
        assert!(scope.next_receipt().await.is_none());
        assert!(joined.registration_released());
        retry.unfinished.clear();
        let complete = retry.finish();
        assert!(complete.is_complete() && complete.closed_io.len() == 1);
        assert_eq!(complete.failed_tasks.len(), 1);
        assert!(Arc::ptr_eq(
            complete.failed_tasks[0].error.retained_source().unwrap(),
            &error
        ));
        drop(retry);
        assert!(storage.lock().unwrap().failed_tasks.is_empty());
    })
    .await
    .unwrap();
}
