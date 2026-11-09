use super::*;
use crate::contract::{
    component::PacketDeviceIdentity,
    scope::{TaskClass, TaskScope},
};
use std::num::NonZeroUsize;
use zero_core::component::{ComponentId, ResourceGeneration};
#[tokio::test]
async fn actual_helper_graph_n1_requires_the_original_scope_join_and_host_ack_before_reuse() {
    let root = ProcessBudget::new(ProcessBudgetLimits {
        max_jobs: 1,
        max_jobs_per_instance: 1,
    })
    .unwrap();
    let memory = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = memory.driver_metadata_view();
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    let tasks = scope.registrar();
    let weak = tasks.downgrade();
    let instance = ComponentInstanceId {
        component: ComponentId(41),
        generation: ResourceGeneration(1),
    };
    let identity = PacketDeviceIdentity::new(instance, tasks.scope_identity());
    let owner = zero_tun::AdoptedTunOwner::new();
    root.reserve_macos_tun_helper(&identity, 0, owner.clone(), &weak, &memory)
        .unwrap();
    assert_eq!(root.snapshot().charged_jobs, 1);
    root.reserve_macos_tun_helper(&identity, 0, owner.clone(), &weak, &memory)
        .unwrap();
    let foreign = zero_tun::AdoptedTunOwner::new();
    assert_eq!(
        root.reserve_macos_tun_helper(&identity, 1, foreign, &weak, &memory)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(root
        .check_install(ProcessBudgetLimits {
            max_jobs: 2,
            max_jobs_per_instance: 1
        })
        .is_err());
    scope.enable_registration().unwrap();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let (_, actual) = tasks
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    scope.begin_shutdown();
    finish.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert!(actual.matches_joined(receipt.joined()));
    assert_eq!(root.snapshot().charged_jobs, 1);
    let deferred = scope.defer_callbacks();
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(root.snapshot().charged_jobs, 1);
    drop(deferred);
    assert!(actual.try_joined().unwrap().registration_released());
    drop(receipt);
    drop(tasks);
    drop(scope);
    // Native remains genuine KnownNoNative: no helper was launched in this
    // rejected/cancelled cold source. Expiry alone was not the releasing fact.
    assert!(weak.retirement_observation().is_none());
    assert_eq!(root.snapshot().charged_jobs, 0);
    root.install(ProcessBudgetLimits {
        max_jobs: 2,
        max_jobs_per_instance: 1,
    })
    .unwrap();
}
