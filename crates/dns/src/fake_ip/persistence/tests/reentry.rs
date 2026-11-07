//! A real native notification may reenter metadata without a held std mutex.
use super::*;
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
use zero_platform_tokio::{
    contract::{scope::TaskScope, CancellationToken},
    controlled_carrier::CarrierOriginalTotals,
    journal::{JournalOwner, JournalSpec},
};
struct Reenter {
    lease: Weak<FakeIpStateLease>,
    observed: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(lease) = self.lease.upgrade() {
            assert!(
                lease.journal.try_lock().is_ok(),
                "native callback cannot run under lease metadata lock"
            );
            if matches!(lease.begin(), Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
                self.observed.store(true, Ordering::Release);
            }
        }
    }
}
#[tokio::test]
async fn actual_file_write_wakes_original_root_with_unlocked_metadata_and_same_call_fenced() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let lease = FakeIpStateLease::hosted(path.clone());
    let allocator = FakeIpAllocator::new_with_state(config(2), Some(lease.clone())).unwrap();
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), None);
    scope.enable_registration().unwrap();
    let owner = JournalOwner::new_hosted(
        JournalSpec {
            path,
            max_bytes: NonZeroUsize::new(4096).unwrap(),
            parent_private: true,
        },
        zero_core::component::ComponentInstanceId {
            component: zero_core::component::ComponentId(1308),
            generation: zero_core::component::ResourceGeneration(1),
        },
        scope.registrar().retirement_observation(),
        CancellationToken::new(),
        &root,
    )
    .unwrap();
    owner.start().unwrap();
    lease.bind(owner.grant()).unwrap();
    allocator.activate_persistence().unwrap();
    let reenter = Arc::new(Reenter {
        lease: Arc::downgrade(&lease),
        observed: AtomicBool::new(false),
    });
    let wake = Waker::from(reenter.clone());
    root.poll_cancelled_retirements(&mut Context::from_waker(&wake));
    allocator
        .alloc_ipv4("actual-write.example")
        .await
        .unwrap()
        .unwrap();
    assert!(reenter.observed.load(Ordering::Acquire));
    assert!(owner.matches_closed(&owner.try_close().unwrap().unwrap()));
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
}
