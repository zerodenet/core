//! Positive native, actual RX/TX return and original Scope ACK are independent.
use super::*;
use crate::{
    contract::scope::{TaskClass, TaskScope},
    controlled_carrier::CarrierOriginalTotals,
};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(61),
        generation: ResourceGeneration(1),
    }
}
#[tokio::test]
async fn actual_udp_source_retains_cloned_senders_but_not_observers_after_native_and_scope_close() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(bound(1));
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(1)));
        scope.enable_registration().unwrap();
        let mut owner = ControlledDatagramOwner::new(bound(1));
        owner
            .bind_source_totals(SourceDatagramTotalsBinding::new(
                &root,
                instance(),
                scope.registrar().retirement_observation(),
                scope.registrar().cancellation(),
            ))
            .unwrap();
        let (mut rx, tx, mut control) = owner.bind_listener(address(), bound(2)).unwrap();
        let clone = tx.clone();
        let (notice, entered) = tokio::sync::oneshot::channel();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                let mut bytes = [0; 1];
                let mut pending = Box::pin(rx.recv_from_addr(&mut bytes));
                poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                notice.send(()).unwrap();
                assert_eq!(
                    pending.await.unwrap_err().kind(),
                    std_io::ErrorKind::NotConnected
                );
                Ok(())
            })
            .unwrap();
        entered.await.unwrap();
        assert_eq!(root.snapshot().charged, 1);
        let native = control.close().unwrap();
        assert_eq!(native.reservation(), control.reservation());
        let joined = scope.next_receipt().await.unwrap();
        joined.result().unwrap();
        assert!(witness.is_joined());
        assert!(!witness.try_joined().unwrap().registration_released());
        assert_eq!(root.snapshot().charged, 1);
        scope.acknowledge(&joined).unwrap();
        scope.begin_shutdown();
        assert!(scope.next_receipt().await.is_none());
        assert!(scope.registrar().retirement_observation().is_drained());
        assert_eq!(root.snapshot().charged, 1, "both real sender views remain");
        drop(tx);
        assert_eq!(
            root.snapshot().charged,
            1,
            "the cloned sender still owns IO"
        );
        drop(clone);
        assert_eq!(
            root.snapshot().charged,
            0,
            "retained native controller is not an IO sender"
        );
        assert_eq!(control.close().unwrap(), native);
        assert_eq!(owner.take_report().confirmed_closed, 1);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn exact_failed_udp_bind_refunds_only_matching_native_before_same_scope_retry() {
    let held = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let root = CarrierOriginalTotals::new(bound(1));
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut owner = ControlledDatagramOwner::new(bound(1));
    owner
        .bind_source_totals(SourceDatagramTotalsBinding::new(
            &root,
            instance(),
            scope.registrar().retirement_observation(),
            scope.registrar().cancellation(),
        ))
        .unwrap();
    let failure = owner
        .bind_listener(held.local_addr().unwrap(), bound(1))
        .err()
        .unwrap();
    let AcquisitionOutcome::Closed(closed) = failure.outcome else {
        panic!("actual failed bind did not close its acquired native original");
    };
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(root.snapshot().charged, 0);
    let (rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    assert_ne!(control.reservation(), closed.reservation());
    assert_eq!(root.snapshot().charged, 1);
    control.close().unwrap();
    drop((rx, tx));
    assert_eq!(
        root.snapshot().charged,
        1,
        "successful publication retains actual Scope debt"
    );
    assert!(scope
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .is_complete());
    assert_eq!(root.snapshot().charged, 0);
}
