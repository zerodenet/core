use super::*;
use crate::contract::scope::{TaskClass, TaskScope};
use std::future::Future;
use zero_core::component::{ComponentId, ResourceGeneration};
fn instance(n: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(n),
        generation: ResourceGeneration(1),
    }
}
fn scope() -> TaskScope {
    TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    )
}
#[tokio::test]
async fn only_matching_original_and_scope_register_one_actual_wire() {
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(2).unwrap());
    let (mut rx, tx, control) = owner
        .bind_listener(
            "127.0.0.1:0".parse().unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let (_other_rx, other_tx, mut other_control) = owner
        .bind_listener(
            "127.0.0.1:0".parse().unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    let mut tasks = scope();
    let mut foreign = scope();
    let provider = control
        .register_provider(instance(1), &tasks.registrar())
        .unwrap();
    assert!(control
        .register_provider(instance(2), &tasks.registrar())
        .is_err());
    let (release_foreign, wait_foreign) = tokio::sync::oneshot::channel();
    let (_, foreign_driver) = foreign
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let _ = wait_foreign.await;
            Ok(())
        })
        .unwrap();
    assert!(control
        .register_driver(&tx.identity(), &foreign_driver)
        .is_err());
    assert!(provider.origin().is_none());
    let (release, wait) = tokio::sync::oneshot::channel();
    let (observed, pending) = tokio::sync::oneshot::channel();
    let (_, driver) = tasks
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let mut bytes = [0; 64];
            let mut receive = Box::pin(rx.recv_from_addr(&mut bytes));
            std::future::poll_fn(|cx| {
                assert!(receive.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            observed.send(()).unwrap();
            assert!(
                receive.await.is_err(),
                "original native close wakes its actual receive"
            );
            let _ = wait.await;
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    assert!(control
        .register_driver(&other_tx.identity(), &driver)
        .is_err());
    let origin = control.register_driver(&tx.identity(), &driver).unwrap();
    assert!(origin
        .owner()
        .same_owner(&tx.identity().provider().unwrap()));
    assert!(origin.driver().same_registration(&driver));
    assert!(provider.closed().is_none());
    let (release_other, wait_other) = tokio::sync::oneshot::channel();
    let (_, second_driver) = tasks
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let _ = wait_other.await;
            Ok(())
        })
        .unwrap();
    assert!(control
        .register_driver(&tx.identity(), &second_driver)
        .is_err());
    // Native close from the original owner (not a view Drop) is visible
    // even when the same real wire has not yet been consumed by its Scope.
    let report = owner.close_all();
    assert_eq!(report.confirmed_closed, 2);
    let closed = origin.closed().unwrap();
    assert!(closed.matches(&provider));
    assert!(!driver.is_joined());
    assert!(origin.is_retired());
    assert_eq!(closed.receipt().reservation(), control.reservation());
    release.send(()).unwrap();
    release_other.send(()).unwrap();
    tasks.next_exit().await.unwrap();
    tasks.next_exit().await.unwrap();
    assert!(driver.is_joined());
    release_foreign.send(()).unwrap();
    foreign.next_exit().await.unwrap();
    other_control.close().unwrap();
}
