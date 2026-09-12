use super::*;
#[test]
fn sender_keeps_identity_and_fails_closed_at_counter_exhaustion() {
    let mut sender = SenderSession::default();
    let (id, packet) = sender.next().unwrap();
    assert_eq!(packet, 1);
    assert_eq!(sender.next().unwrap(), (id, 2));
    sender.packet = u64::MAX;
    assert!(sender.next().is_err());
    assert!(sender.next().is_err());
}
#[tokio::test(start_paused = true)]
async fn received_windows_preserve_live_entries_at_capacity_and_expire() {
    let mut windows = ReceiveWindows::with_limits(crate::validation::StateLimits {
        udp_capacity: Some(CAPACITY),
        ..Default::default()
    });
    for id in 0..CAPACITY as u64 {
        assert!(windows.accept(id, 1).unwrap());
    }
    assert!(windows.accept(CAPACITY as u64, 1).is_err());
    tokio::time::advance(crate::udp::session::RETENTION - Duration::from_secs(1)).await;
    assert!(!windows.accept(0, 1).unwrap());
    assert!(windows.accept(CAPACITY as u64, 1).is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(windows.accept(CAPACITY as u64, 1).unwrap());
}
#[tokio::test(start_paused = true)]
async fn responses_keep_per_client_counters_and_reclaim_idle_entries() {
    let mut responses = ResponseSessions::default();
    let a = responses.next(1).unwrap();
    let b = responses.next(2).unwrap();
    assert_ne!(a.0, b.0);
    assert_eq!(responses.next(1).unwrap(), (a.0, 2));
    tokio::time::advance(Duration::from_secs(300)).await;
    responses.prune();
    assert!(responses.0.is_empty());
}

#[test]
fn terminal_packet_does_not_consume_receive_capacity() {
    let mut windows = ReceiveWindows::with_limits(crate::validation::StateLimits {
        udp_capacity: Some(1),
        ..Default::default()
    });
    assert!(!windows.accept(1, u64::MAX).unwrap());
    assert!(windows.accept(2, 0).unwrap());
}
