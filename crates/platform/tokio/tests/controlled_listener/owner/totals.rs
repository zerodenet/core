//! Same root capacity is returned only by each actual original's real gates.
use super::*;
use crate::{
    contract::scope::{TaskClass, TaskScope},
    controlled_carrier::CarrierOriginalTotals,
};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
pub(super) fn instance(id: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(id),
        generation: ResourceGeneration(1),
    }
}
#[tokio::test]
async fn source_listener_native_close_and_returned_view_cannot_replace_real_scope_ack() {
    timeout(Duration::from_secs(3), async {
        let roots = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let mut original = owner("127.0.0.1:0".parse().unwrap(), 1);
        original
            .bind_original_totals(instance(1), &scope.registrar(), &roots)
            .unwrap();
        let mut view = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        assert_eq!(roots.snapshot().charged, 1);
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                {
                    let mut accept = Box::pin(view.accept());
                    poll_fn(|cx| {
                        assert!(accept.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    ready.send(()).unwrap();
                    assert_eq!(
                        accept.await.unwrap_err().kind(),
                        io::ErrorKind::ConnectionAborted
                    );
                }
                drop(view);
                Ok(())
            })
            .unwrap();
        waiting.await.unwrap();
        let mut denied = owner("127.0.0.1:0".parse().unwrap(), 1);
        denied
            .bind_original_totals(instance(2), &scope.registrar(), &roots)
            .unwrap();
        let failure = match denied.bind_listener("127.0.0.1:0".parse().unwrap()) {
            Err(failure) => failure,
            Ok(_) => panic!("global limit permitted native listener"),
        };
        assert_eq!(failure.reason, Reason::Capacity);
        assert_eq!(denied.remaining_bind_attempts(), 1);
        assert_eq!(
            denied.confirmed_close_count(),
            0,
            "denial before real native creation"
        );
        let report = original.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(original.confirmed_close_count(), 1);
        assert_eq!(
            roots.snapshot().charged,
            1,
            "native close alone is insufficient"
        );
        scope.begin_shutdown();
        let receipt = scope.next_receipt().await.unwrap();
        assert!(witness.matches_joined(receipt.joined()));
        assert_eq!(
            roots.snapshot().charged,
            1,
            "same actual join still awaits HostACK"
        );
        scope.acknowledge(&receipt).unwrap();
        assert!(scope.is_drained());
        assert_eq!(roots.snapshot().charged, 0);
        assert!(witness.try_joined().unwrap().registration_released());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn source_listener_keeps_unique_view_after_actual_scope_is_drained() {
    let roots = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    let mut original = owner("127.0.0.1:0".parse().unwrap(), 1);
    original
        .bind_original_totals(instance(1), &scope.registrar(), &roots)
        .unwrap();
    let view = original
        .bind_listener("127.0.0.1:0".parse().unwrap())
        .unwrap();
    assert!(
        original
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete()
    );
    scope.begin_shutdown();
    assert!(scope.is_drained());
    assert_eq!(
        roots.snapshot().charged,
        1,
        "actual Scope/Native still does not return a held unique view"
    );
    drop(view);
    assert_eq!(roots.snapshot().charged, 0);
}
#[tokio::test]
async fn failed_listener_attempt_explicit_native_close_preserves_n1_retry_without_scope_fabrication()
 {
    let roots = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let mut original = owner(address, 2);
    original
        .bind_original_totals(instance(1), &scope.registrar(), &roots)
        .unwrap();
    let failure = match original.bind_listener(address) {
        Err(failure) => failure,
        Ok(_) => panic!("actual bind collision succeeded"),
    };
    assert_eq!(io_source(&failure).kind(), io::ErrorKind::AddrInUse);
    assert_eq!(original.confirmed_close_count(), 1);
    assert_eq!(
        roots.snapshot().charged,
        0,
        "unpublished exact failed attempt has an actual native close, no task was manufactured"
    );
    assert!(!scope.is_drained());
    // This is only the fixture's conflicting listener, not a close receipt.
    drop(occupied);
    let view = original.bind_listener(address).unwrap();
    assert_eq!(roots.snapshot().charged, 1);
    assert_eq!(original.remaining_bind_attempts(), 0);
    assert!(
        original
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete()
    );
    assert_eq!(original.confirmed_close_count(), 2);
    drop(view);
    assert_eq!(
        roots.snapshot().charged,
        1,
        "success requires its real Source Scope too"
    );
    scope.begin_shutdown();
    assert_eq!(roots.snapshot().charged, 0);
}

#[tokio::test]
async fn lost_source_owner_keeps_its_same_native_until_existing_host_poll_confirms_close() {
    timeout(Duration::from_secs(3), async {
        let roots = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
        scope.enable_registration().unwrap();
        let mut original = owner("127.0.0.1:0".parse().unwrap(), 1);
        original
            .bind_original_totals(instance(1), &scope.registrar(), &roots)
            .unwrap();
        let view = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let actual = view.shared.clone();
        drop(original);
        drop(view);
        scope.begin_shutdown();
        assert!(scope.is_drained());
        assert_eq!(roots.snapshot().charged, 1);
        assert_eq!(
            actual.state.lock().unwrap().confirmed,
            0,
            "Drop is not native close evidence"
        );
        poll_fn(|cx| {
            roots.poll_cancelled_retirements(cx);
            if roots.snapshot().charged == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        let state = actual.state.lock().unwrap();
        assert!(
            state.closed
                && state.listener.is_none()
                && state.raw.is_none()
                && state.error.is_none()
        );
        assert_eq!(
            state.confirmed, 1,
            "same original native closed explicitly once by retained continuation"
        );
    })
    .await
    .unwrap();
}
