use std::{
    error::Error,
    future::Future,
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};
use zero_platform_tokio::contract::{scope::TaskClass, ChannelError, Impact, Reason, Stage};

#[tokio::test]
async fn interrupted_shutdown_preserves_already_collected_failures_and_task_ids() {
    let mut owner = super::scope(2);
    let register = owner.registrar();
    let (finished, observed) = oneshot::channel();
    let first = register
        .spawn(TaskClass::Critical, async move {
            finished.send(()).unwrap();
            Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(std::io::Error::other("first driver cleanup failed")),
            )
        })
        .unwrap();
    let (release, wait) = oneshot::channel();
    let second = register
        .spawn(TaskClass::Auxiliary, async move {
            wait.await.unwrap();
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    let mut shutdown = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(1)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(shutdown.as_mut().poll(&mut cx), Poll::Pending));
    drop(shutdown);
    release.send(()).unwrap();
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 2);
    assert!(report.exited_tasks.contains(&first));
    assert!(report.exited_tasks.contains(&second));
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, first);
    assert_eq!(report.failed_tasks[0].class, TaskClass::Critical);
    assert_eq!(
        report.failed_tasks[0].error.source().unwrap().to_string(),
        "first driver cleanup failed"
    );
}
