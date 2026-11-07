//! Existing durable format with actual bounded, explicitly closed journals.
use super::*;
use crate::fake_ip::FakeIpAllocator;
fn config(entries: usize) -> zero_config::FakeIpConfigRef<'static> {
    zero_config::FakeIpConfigRef {
        cidr: "198.18.0.0/24",
        ipv6_cidr: None,
        ttl_seconds: 3600,
        max_entries: Some(entries),
        exclude_domains: &[],
    }
}
#[tokio::test]
async fn bounded_checkpoint_rejects_new_mapping_before_lru_eviction_and_remains_restart_readable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let lease = FakeIpStateLease::acquire_bounded(path.clone(), 4096).unwrap();
    let allocator = FakeIpAllocator::new_with_state(config(1), Some(lease.clone())).unwrap();
    let old = allocator.alloc_ipv4("old.example").await.unwrap().unwrap();
    let exact = std::fs::metadata(&path).unwrap().len() as usize;
    assert!(lease.close_standalone().unwrap());
    drop(allocator);
    drop(lease);
    let lease = FakeIpStateLease::acquire_bounded(path.clone(), exact).unwrap();
    let allocator = FakeIpAllocator::new_with_state(config(1), Some(lease.clone())).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let error = allocator
        .alloc_ipv4("a-longer-new.example")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::FileTooLarge);
    assert_eq!(allocator.lookup(&old).await.as_deref(), Some("old.example"));
    assert!(allocator
        .lookup_domain("a-longer-new.example")
        .await
        .is_none());
    let stats = allocator.stats().await;
    assert_eq!(
        (
            stats.live_mappings,
            stats.retired_addresses,
            stats.evictions
        ),
        (1, 0, 0)
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    // Actual refresh exceeds append headroom but replaces the same complete
    // current checkpoint; own legal output is always readable at the same cap.
    for _ in 0..8 {
        assert_eq!(
            allocator.alloc_ipv4("old.example").await.unwrap(),
            Some(old)
        );
        assert!(std::fs::metadata(&path).unwrap().len() <= exact as u64);
    }
    assert!(lease.close_standalone().unwrap());
    drop(allocator);
    drop(lease);
    let lease = FakeIpStateLease::acquire_bounded(path, exact).unwrap();
    let restarted = FakeIpAllocator::new_with_state(config(1), Some(lease.clone())).unwrap();
    assert_eq!(restarted.lookup(&old).await.as_deref(), Some("old.example"));
    assert!(lease.close_standalone().unwrap());
}
#[tokio::test]
async fn journal_generation_exhaustion_preserves_original_bytes_binding_and_counter() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let lease = FakeIpStateLease::acquire_bounded(path.clone(), 4096).unwrap();
    let allocator = FakeIpAllocator::new_with_state(config(1), Some(lease.clone())).unwrap();
    let address = allocator.alloc_ipv4("kept.example").await.unwrap().unwrap();
    let metadata = allocator
        .inner
        .try_lock()
        .unwrap()
        .persistence
        .as_ref()
        .unwrap()
        .metadata
        .clone();
    allocator
        .inner
        .try_lock()
        .unwrap()
        .persistence
        .as_mut()
        .unwrap()
        .generation = u64::MAX;
    let old = lease.grant().unwrap();
    let bytes = std::fs::read(&path).unwrap();
    lease.journal.lock().unwrap().generation = u64::MAX;
    let error = FakeIpPersistence::open(lease.clone(), metadata)
        .err()
        .unwrap();
    assert!(error.get_ref().unwrap().is::<JournalGenerationExhausted>());
    assert_eq!(lease.journal.lock().unwrap().generation, u64::MAX);
    assert!(old.same_original(&lease.grant().unwrap()));
    assert!(!old.native_closed());
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    let pause_error = allocator.pause_journal().err().unwrap();
    assert!(pause_error
        .get_ref()
        .unwrap()
        .is::<JournalGenerationExhausted>());
    assert_eq!(
        allocator.lookup(&address).await.as_deref(),
        Some("kept.example")
    );
    assert_eq!(
        allocator.alloc_ipv4("kept.example").await.unwrap(),
        Some(address)
    );
    assert!(lease.close_standalone().unwrap());
}
#[test]
fn failed_genuine_library_open_retains_its_actual_owner_and_preserves_existing_duplicate_error() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let owner = FakeIpStateLease::acquire_bounded(path.clone(), 4096).unwrap();
    let error = FakeIpStateLease::acquire_bounded(path, 4096).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert!(error.to_string().contains("already owned"));
    let failed = error
        .get_ref()
        .unwrap()
        .downcast_ref::<FakeIpStateOpenFailure>()
        .unwrap();
    assert_eq!(failed.work().kind(), io::ErrorKind::AlreadyExists);
    assert!(failed.try_close().unwrap());
    assert!(
        !owner.grant().unwrap().native_closed(),
        "second-owner failure cannot close the first native original"
    );
    assert!(owner.close_standalone().unwrap());
}
mod hosted;
mod reentry;

#[test]
fn genuine_library_decode_failure_retains_the_same_open_original_until_explicit_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let header = serde_json::json!({"record":"header","schema":SCHEMA,
        "cidr":"198.18.0.0/24","ttl_seconds":3600,"max_entries":2,"exclusions":[]});
    std::fs::write(
        &path,
        format!("{header}\n{{\"record\":\"upsert\",\"domain\":37}}\n"),
    )
    .unwrap();
    let lease = FakeIpStateLease::acquire_bounded(path, 4096).unwrap();
    let original = lease.grant().unwrap();
    let error = FakeIpAllocator::new_with_state(config(2), Some(lease))
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!original.native_closed());
    let failure = error
        .get_ref()
        .unwrap()
        .downcast_ref::<FakeIpStateOpenFailure>()
        .unwrap();
    assert_eq!(failure.work().kind(), io::ErrorKind::InvalidData);
    assert!(failure.try_close().unwrap());
    assert!(original.native_closed());
}
