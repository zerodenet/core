use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    runtime::Handle,
    sync::oneshot,
    time::{timeout, Instant},
};
use zero_platform_tokio::contract::{
    scope::{SpawnError, TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};

fn scope(limit: usize) -> TaskScope {
    TaskScope::new(Handle::current(), NonZeroUsize::new(limit).unwrap())
}

#[tokio::test]
async fn zero_task_component_cannot_silently_spawn_a_driver() {
    let mut owner = TaskScope::empty(Handle::current());
    assert_eq!(
        owner
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert!(report.exited_tasks.is_empty());
}

#[tokio::test]
async fn scope_bounds_live_tasks_and_undelivered_exits_then_reuses_capacity() {
    let mut owner = scope(1);
    let register = owner.registrar();
    let (tx, rx) = oneshot::channel();
    let first = register
        .spawn(TaskClass::Critical, async move {
            let _ = rx.await;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        register.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    tx.send(()).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(
        register.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let exit = owner.next_exit().await.unwrap();
    assert_eq!(exit.id, first);
    assert_eq!(exit.class, TaskClass::Critical);
    assert!(exit.result.is_ok());
    register
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
    assert_eq!(
        register.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
}

#[tokio::test]
async fn timeout_retains_unfinished_task_ownership_for_cleanup_retry() {
    let mut owner = scope(2);
    let register = owner.registrar();
    let (tx, rx) = oneshot::channel();
    let exited = Arc::new(AtomicBool::new(false));
    let observed = exited.clone();
    register
        .spawn(TaskClass::Critical, async move {
            let _ = rx.await;
            observed.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let report = owner
        .shutdown(Instant::now() + Duration::from_millis(10))
        .await;
    assert!(!report.is_complete());
    assert_eq!(report.unfinished.len(), 1);
    assert!(!exited.load(Ordering::SeqCst));
    tx.send(()).unwrap();
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
    assert!(exited.load(Ordering::SeqCst));
}

#[tokio::test]
async fn closing_cancels_drivers_and_does_not_include_supervisor_in_wait() {
    let mut owner = scope(1);
    let register = owner.registrar();
    let cancel = register.cancellation();
    register
        .spawn(TaskClass::Critical, async move {
            cancel.cancelled().await;
            Ok(())
        })
        .unwrap();
    let report = timeout(
        Duration::from_secs(1),
        owner.shutdown(Instant::now() + Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), 1);
}

#[tokio::test]
async fn supervisor_keeps_failure_class_and_source_including_panics() {
    let mut owner = scope(2);
    let register = owner.registrar();
    register
        .spawn(TaskClass::Critical, async {
            Err(ChannelError::new(
                Stage::Receive,
                Reason::Io,
                Impact::Component,
            ))
        })
        .unwrap();
    let exit = owner.next_exit().await.unwrap();
    assert_eq!(exit.class, TaskClass::Critical);
    assert_eq!(exit.result.unwrap_err().reason, Reason::Io);
    register
        .spawn(TaskClass::Auxiliary, async { panic!("test driver panic") })
        .unwrap();
    let exit = owner.next_exit().await.unwrap();
    assert_eq!(exit.class, TaskClass::Auxiliary);
    let error = exit.result.unwrap_err();
    assert_eq!(error.reason, Reason::Internal);
    assert!(std::error::Error::source(&error).is_some());
}

#[tokio::test]
async fn idle_supervisor_wakes_on_registration_without_extra_driver_task() {
    let mut owner = scope(1);
    let register = owner.registrar();
    let joined = async move {
        tokio::task::yield_now().await;
        register
            .spawn(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap()
    };
    let (exit, id) = timeout(Duration::from_secs(1), async {
        tokio::join!(owner.next_exit(), joined)
    })
    .await
    .unwrap();
    assert_eq!(exit.unwrap().id, id);
}

#[tokio::test]
async fn task_ids_remain_unambiguous_when_close_reports_combine_multiple_scopes() {
    let mut left = scope(1);
    let mut right = scope(1);
    let first = left
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let second = right
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    assert_ne!(first, second);
    let deadline = Instant::now() + Duration::from_secs(1);
    let (mut first_report, second_report) =
        tokio::join!(left.shutdown(deadline), right.shutdown(deadline));
    first_report.append(second_report);
    assert_eq!(first_report.exited_tasks.len(), 2);
    assert!(first_report.is_complete());
}

#[tokio::test]
async fn concurrent_registration_and_closing_leave_no_unowned_task() {
    let mut owner = scope(16);
    let register = owner.registrar();
    let barrier = Arc::new(std::sync::Barrier::new(9));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let register = register.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            let cancel = register.cancellation();
            barrier.wait();
            register.spawn(TaskClass::Critical, async move {
                cancel.cancelled().await;
                Ok(())
            })
        }));
    }
    barrier.wait();
    owner.begin_shutdown();
    let accepted = threads
        .into_iter()
        .filter_map(|thread| match thread.join().unwrap() {
            Ok(id) => Some(id),
            Err(error) => {
                assert_eq!(error, SpawnError::Closing);
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        register.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks.len(), accepted.len());
    assert!(accepted.iter().all(|id| report.exited_tasks.contains(id)));
}

#[tokio::test]
async fn shutdown_keeps_failures_and_their_sources_when_it_collects_the_exit_itself() {
    let mut owner = scope(1);
    let id = owner
        .registrar()
        .spawn(TaskClass::Critical, async {
            Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(std::io::Error::other("driver close failed")),
            )
        })
        .unwrap();
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, [id]);
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, id);
    assert_eq!(report.failed_tasks[0].class, TaskClass::Critical);
    assert!(std::error::Error::source(&report.failed_tasks[0].error).is_some());
}
