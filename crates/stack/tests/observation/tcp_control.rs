use super::*;
use std::sync::atomic::Ordering;
#[derive(Debug, Default)]
struct Counts {
    full: AtomicU64,
    closed: AtomicU64,
}
impl zero_traits::IoObserver for Counts {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {
        panic!("missing discard reason");
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        match reason {
            zero_traits::PacketDropReason::QueueFull => {
                self.full.fetch_add(1, Ordering::Relaxed);
            }
            zero_traits::PacketDropReason::QueueClosed => {
                self.closed.fetch_add(1, Ordering::Relaxed);
            }
            _ => panic!("incorrect reason"),
        }
    }
}
#[tokio::test]
async fn retrying_control_queue_backpressure_is_not_counted_as_packet_loss() {
    let counter = Arc::new(Counts::default());
    let (tx, mut rx) = mpsc::channel::<ObservedPacket>(1);
    let output = PacketSender::from(tx).with_observer(Some(counter.clone()));
    let control = TcpControlPackets::new(output);
    assert!(control.try_send(vec![0]));
    for i in 0..TCP_CONTROL_QUEUE_CAPACITY {
        assert!(control.try_send(vec![i as u8]));
    }
    assert_eq!(counter.full.load(Ordering::Relaxed), 0);
    assert!(!control.try_send(vec![9]));
    assert_eq!(counter.full.load(Ordering::Relaxed), 1);
    control.ensure_worker();
    for _ in 0..=TCP_CONTROL_QUEUE_CAPACITY {
        tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(counter.full.load(Ordering::Relaxed), 1);
    assert_eq!(counter.closed.load(Ordering::Relaxed), 0);
}
#[test]
fn teardown_counts_accepted_control_packets_still_in_the_retry_queue() {
    let counter = Arc::new(Counts::default());
    let (tx, _rx) = mpsc::channel::<ObservedPacket>(1);
    let control =
        TcpControlPackets::new(PacketSender::from(tx).with_observer(Some(counter.clone())));
    assert!(control.try_send(vec![0]));
    assert!(control.try_send(vec![1]));
    assert!(control.try_send(vec![2]));
    drop(control);
    assert_eq!(counter.closed.load(Ordering::Relaxed), 2);
    assert_eq!(counter.full.load(Ordering::Relaxed), 0);
}
