use super::*;
use crate::contract::scope::{TaskClass, TaskScope};
use std::{future::Future, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::{sync::oneshot, time::Instant};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
#[tokio::test]
async fn cancelled_native_wait_retains_actual_child_and_close_requires_original_scope_join() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let budget = super::super::ProcessBudget::new(super::super::ProcessBudgetLimits {
            max_jobs: 1,
            max_jobs_per_instance: 1,
        })
        .unwrap();
        let mut request = ProcessRequest::new("/bin/sleep");
        request.arguments = vec!["60".into()];
        let original = OwnedProcess::prepare(request, &scope.registrar()).unwrap();
        budget
            .reserve(
                ComponentInstanceId {
                    component: ComponentId(931),
                    generation: ResourceGeneration(1),
                },
                &original,
            )
            .unwrap();
        let (begin, starting) = oneshot::channel();
        let (pending, readied) = oneshot::channel();
        let (finish, finishing) = oneshot::channel();
        let actual = original.clone();
        let (_, actor) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                actual.start(starting.await.unwrap()).unwrap();
                actual.wait_started(end()).await.unwrap();
                let mut native_wait = Box::pin(actual.wait_root_exit());
                std::future::poll_fn(|cx| {
                    assert!(
                        native_wait.as_mut().poll(cx).is_pending(),
                        "the real retained Child wait must be pending"
                    );
                    Poll::Ready(())
                })
                .await;
                drop(native_wait); // cancels only this waiter, not its actual Child
                pending.send(()).unwrap();
                finishing.await.unwrap();
                Ok(())
            })
            .unwrap();
        begin.send(actor.clone()).unwrap();
        readied.await.unwrap();
        assert!(original.lock().child.is_some());
        assert!(original.root_status().is_none());
        assert!(original.closed().is_none());
        original.request_stop();
        original.wait_child_ended(end()).await.unwrap();
        assert!(original.root_status().is_some());
        assert!(
            original.try_finish().unwrap().is_none(),
            "native reaping alone cannot consume its original actor join"
        );
        assert!(original.wait_closed(Instant::now()).await.is_err());
        assert!(original.closed().is_none());
        assert_eq!(budget.snapshot().charged_jobs, 1);
        finish.send(()).unwrap();
        let report = scope.shutdown(end()).await;
        assert!(report.is_complete(), "{report:?}");
        let closed = original.wait_closed(end()).await.unwrap().unwrap();
        assert!(closed.matches(&original) && closed.had_native_acquisition());
        assert_eq!(closed.exit_status(), original.root_status());
        assert!(actor.matches_joined(&actor.try_joined().unwrap()));
        assert_eq!(budget.snapshot().charged_jobs, 0);
        assert!(
            original.lock().child.is_none(),
            "release the actual Child only after its positive wait and join receipts"
        );
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn native_monitor_and_outer_cleanup_both_observe_the_same_reaped_child() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let mut request = ProcessRequest::new("/bin/sleep");
        request.arguments = vec!["60".into()];
        let original = OwnedProcess::prepare(request, &scope.registrar()).unwrap();
        let (begin, starting) = oneshot::channel();
        let (pending, readied) = oneshot::channel();
        let actual = original.clone();
        let (_, actor) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                actual.start(starting.await.unwrap()).unwrap();
                let mut waiting = Box::pin(actual.wait_root_exit());
                std::future::poll_fn(|cx| {
                    assert!(waiting.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                pending.send(()).unwrap();
                let status = waiting.await.unwrap();
                assert_eq!(Some(status), actual.root_status());
                Ok(())
            })
            .unwrap();
        begin.send(actor.clone()).unwrap();
        readied.await.unwrap();
        let mut cleanup_wait = Box::pin(original.wait_root_exit());
        std::future::poll_fn(|cx| {
            assert!(cleanup_wait.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        original.request_stop();
        let status = cleanup_wait.await.unwrap();
        assert!(scope.next_exit().await.unwrap().result.is_ok());
        let closed = original.wait_closed(end()).await.unwrap().unwrap();
        assert!(closed.matches(&original));
        assert_eq!(closed.exit_status(), Some(status));
        assert!(actor.matches_joined(&actor.try_joined().unwrap()));
        assert!(scope.shutdown(end()).await.is_complete());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn failed_native_spawn_is_terminal_and_preserves_its_original_error_through_cleanup() {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let original = OwnedProcess::prepare(
        ProcessRequest::new("/zero-missing-native-plugin-no-fallback"),
        &scope.registrar(),
    )
    .unwrap();
    let (begin, starting) = oneshot::channel();
    let actual = original.clone();
    let (_, actor) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let witness: crate::contract::scope::TaskJoinWitness = starting.await.unwrap();
            assert_eq!(
                actual.start(witness.clone()).unwrap_err().kind(),
                io::ErrorKind::NotFound
            );
            assert!(actual.start(witness).is_err());
            Ok(())
        })
        .unwrap();
    begin.send(actor.clone()).unwrap();
    assert!(scope.next_exit().await.unwrap().result.is_ok());
    let source = original.first_failure().unwrap();
    original.request_stop();
    let closed = original.wait_closed(end()).await.unwrap().unwrap();
    assert!(closed.matches(&original) && !closed.had_native_acquisition());
    assert!(closed.exit_status().is_none());
    assert!(Arc::ptr_eq(&source, &original.first_failure().unwrap()));
    assert!(scope.shutdown(end()).await.is_complete());
}
