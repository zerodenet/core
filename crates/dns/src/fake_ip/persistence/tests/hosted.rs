//! Cold Default-only binding, original native/Scope ACK and retained history.
use super::*;
use std::num::NonZeroUsize;
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, scope::TaskScope, CancellationToken},
    controlled_carrier::CarrierOriginalTotals,
    journal::{JournalOwner, JournalSpec},
};
fn instance(generation: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(1307),
        generation: ResourceGeneration(generation),
    }
}
#[tokio::test]
async fn hosted_pause_preserves_valid_history_and_fresh_journal_waits_for_actual_native_and_host_ack(
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let lease = FakeIpStateLease::hosted(path.clone());
    let allocator = FakeIpAllocator::new_with_state(config(2), Some(lease.clone())).unwrap();
    assert!(!path.exists());
    assert!(!path.with_extension("lock").exists());
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let owner = JournalOwner::new_hosted(
        JournalSpec {
            path: path.clone(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
        },
        instance(1),
        tasks.retirement_observation(),
        CancellationToken::new(),
        &root,
    )
    .unwrap();
    assert_eq!(root.snapshot().charged, 0);
    owner.start().unwrap();
    lease.bind(owner.grant()).unwrap();
    allocator.activate_persistence().unwrap();
    let address = allocator.alloc_ipv4("kept.example").await.unwrap().unwrap();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let (_, witness) = tasks
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    let pause = allocator.pause_journal().unwrap().unwrap();
    let before = std::fs::read(&path).unwrap();
    assert_eq!(
        allocator
            .alloc_ipv4("new.example")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        allocator.lookup(&address).await.as_deref(),
        Some("kept.example")
    );
    assert_eq!(
        allocator.compact_for_limit(1).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let receipt = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&receipt));
    assert!(lease.grant().unwrap().native_closed());
    let mut fresh_scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    fresh_scope.enable_registration().unwrap();
    let next = JournalOwner::new_hosted(
        JournalSpec {
            path: path.clone(),
            max_bytes: NonZeroUsize::new(4096).unwrap(),
        },
        instance(2),
        fresh_scope.registrar().retirement_observation(),
        CancellationToken::new(),
        &root,
    )
    .unwrap();
    assert_eq!(next.start().unwrap_err().kind(), io::ErrorKind::WouldBlock);
    scope.begin_shutdown();
    finish.send(()).unwrap();
    let joined = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(joined.joined()));
    assert_eq!(root.snapshot().charged, 1);
    scope.acknowledge(&joined).unwrap();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
    next.start().unwrap();
    assert!(!owner.grant().same_original(&next.grant()));
    lease.bind(next.grant()).unwrap();
    allocator.activate_persistence().unwrap();
    assert_eq!(
        allocator.lookup(&address).await.as_deref(),
        Some("kept.example")
    );
    drop(pause);
    assert_eq!(
        allocator.alloc_ipv4("kept.example").await.unwrap(),
        Some(address)
    );
    assert_eq!(root.snapshot().charged, 1);
    assert!(next.matches_closed(&next.try_close().unwrap().unwrap()));
    fresh_scope.begin_shutdown();
    assert!(fresh_scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
}
