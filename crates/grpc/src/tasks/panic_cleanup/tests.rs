use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
#[tokio::test]
async fn same_actual_task_preserves_work_panic_and_independent_os_cleanup_fault() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let source = Arc::new(std::fs::File::open(directory.path().join("missing")).unwrap_err());
        let weak = Arc::downgrade(&source);
        let panic = Arc::new(());
        let panic_weak = Arc::downgrade(&panic);
        let captured = panic.clone();
        let os = source.clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            std::num::NonZeroUsize::new(1).unwrap(),
        );
        let (id, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let work = crate::native::catch_work(async move {
                    std::panic::panic_any(captured);
                    #[allow(unreachable_code)]
                    io::Result::Ok(())
                })
                .await;
                finish_caught(work, [Err(io::Error::new(os.kind(), os))]).map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                        .with_source(source)
                })
            })
            .unwrap();
        let report = scope
            .shutdown(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
            .await;
        assert!(report.is_complete());
        assert_eq!(report.failed_tasks.len(), 1);
        assert_eq!(report.failed_tasks[0].id, id);
        let actual = witness.wait().await.result().unwrap_err();
        use std::error::Error;
        let cleanup = actual
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<PanicCleanup>()
            .unwrap();
        let payload = cleanup.payload.lock().unwrap();
        assert!(Arc::ptr_eq(
            payload.downcast_ref::<Arc<()>>().unwrap(),
            &panic
        ));
        drop(payload);
        assert!(Arc::ptr_eq(
            cleanup.cleanup[0]
                .get_ref()
                .unwrap()
                .downcast_ref::<Arc<io::Error>>()
                .unwrap(),
            &source
        ));
        assert!(witness.try_joined().unwrap().registration_released());
        drop(source);
        drop(panic);
        drop(scope);
        drop(witness);
        drop(report);
        assert!(
            weak.upgrade().is_some() && panic_weak.upgrade().is_some(),
            "same actual Scope failure retains both sources"
        );
        drop(actual);
        assert!(weak.upgrade().is_none() && panic_weak.upgrade().is_none());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn no_competing_cleanup_error_keeps_the_original_tokio_join_panic() {
    let original = tokio::spawn(async {
        let work = crate::native::catch_work(async {
            panic!("same actual gRPC work panic");
            #[allow(unreachable_code)]
            io::Result::Ok(())
        })
        .await;
        finish_caught(work, [Ok(())])
    });
    let id = original.id();
    let original = original.await.unwrap_err();
    assert!(original.is_panic());
    assert_eq!(original.id(), id);
}
