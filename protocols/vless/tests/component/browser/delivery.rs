//! Real fixed delivery backing, cancellation and final closed-weak credit.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
struct Original {
    owner: PacketMetadataWeak<Owner>,
    destroyed: Arc<AtomicBool>,
}
impl Drop for Original {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let data = owner.data::<Self>();
            let _gate = data
                .state
                .try_lock()
                .expect("original destructor runs outside its delivery gate");
        }
        self.destroyed.store(true, Ordering::Release);
    }
}
#[tokio::test]
async fn original_delivery_cancel_destroys_unique_body_outside_gate_and_keeps_last_weak_header_paid(
) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let memory = root.driver_metadata_view();
        let baseline = memory.snapshot().occupied;
        let (sender, mut receiver, weak) = prepare::<Original>(&memory).unwrap();
        let destroyed = Arc::new(AtomicBool::new(false));
        assert!(memory.snapshot().occupied > baseline);
        assert!(sender
            .send(Original {
                owner: weak.clone(),
                destroyed: destroyed.clone()
            })
            .is_ok());
        weak.upgrade().unwrap().discard::<Original>();
        assert!(destroyed.load(Ordering::Acquire));
        assert_eq!(
            receiver.receive().await.err().unwrap().reason,
            Reason::Revoked
        );
        drop(receiver);
        assert!(weak.upgrade().is_none());
        assert!(
            memory.snapshot().occupied > baseline,
            "closed external weak still retains its actual allocated header"
        );
        drop(weak);
        assert_eq!(memory.snapshot().occupied, baseline);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn original_delivery_refused_receiver_returns_same_unique_body_without_replaying_it() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let memory = root.driver_metadata_view();
    let baseline = memory.snapshot().occupied;
    let (sender, receiver, weak) = prepare::<Original>(&memory).unwrap();
    drop(receiver);
    let destroyed = Arc::new(AtomicBool::new(false));
    let original = sender
        .send(Original {
            owner: weak.clone(),
            destroyed: destroyed.clone(),
        })
        .err()
        .unwrap();
    assert!(!destroyed.load(Ordering::Acquire));
    drop(original);
    assert!(destroyed.load(Ordering::Acquire));
    assert!(weak.upgrade().is_none());
    drop(weak);
    assert_eq!(memory.snapshot().occupied, baseline);
}
