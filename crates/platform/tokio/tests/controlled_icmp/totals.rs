//! Real repeated ICMP probes under one live Source Scope and global N=1.
use super::*;
use crate::{
    contract::scope::{TaskClass, TaskScope},
    controlled_carrier::CarrierOriginalTotals,
};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
#[tokio::test]
#[ignore = "actual native loopback ICMP access required"]
async fn icmp_source_total_refunds_each_real_probe_only_after_native_unique_view_and_same_registration_ack(
) {
    let _serial = LIVE.lock().await;
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let mut native = owner(1);
    native
        .bind_original_totals(IcmpTotalsBinding::new(
            &root,
            ComponentInstanceId {
                component: ComponentId(63),
                generation: ResourceGeneration(1),
            },
            scope.registrar().retirement_observation(),
            scope.registrar().cancellation(),
        ))
        .unwrap();
    let native = Arc::new(Mutex::new(native));
    for sequence in 0..3 {
        let (start, started) =
            tokio::sync::oneshot::channel::<crate::contract::scope::TaskJoinWitness>();
        let (returned, view) = tokio::sync::oneshot::channel();
        let actual = native.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                let caller = started.await.unwrap();
                let mut view = actual
                    .lock()
                    .unwrap()
                    .bind_for_task(local(), None, &caller)
                    .unwrap();
                echo::echo(&mut view, Ipv4Addr::LOCALHOST.into(), sequence).await;
                assert!(actual.lock().unwrap().close(&view).unwrap().matches(&view));
                assert!(returned.send(view).is_ok());
                Ok(())
            })
            .unwrap();
        start.send(witness.clone()).unwrap();
        let held = view.await.unwrap();
        let joined = scope.next_receipt().await.unwrap();
        joined.result().unwrap();
        assert!(witness.matches_joined(&witness.try_joined().unwrap()));
        assert_eq!(root.snapshot().charged, 1);
        scope.acknowledge(&joined).unwrap();
        assert_eq!(
            root.snapshot().charged,
            1,
            "held actual IO view still owes custody"
        );
        drop(held);
        assert_eq!(root.snapshot().charged, 0);
        assert_eq!(native.lock().unwrap().take_report().confirmed_closed, 1);
        assert!(scope.registrar().scope_identity().resource_open());
    }
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
}
