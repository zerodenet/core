use super::*;

#[test]
fn actual_queued_and_dequeued_originals_share_frame_and_byte_credits() {
    let budget = ReturnBudget::new(2, 7);
    let original: PacketBuffer = vec![1, 2, 3, 4].into();
    let allocation = original.as_ptr();
    let charged = budget.retain(original).unwrap();
    assert_eq!(charged.as_ptr(), allocation);
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let replies: zero_stack::packet_output::PacketSender = tx.into();
    replies.try_send_buffer(charged).unwrap();
    let held = rx.try_recv().unwrap();
    assert_eq!(held.as_ptr(), allocation);
    // Dequeue and receiver retirement do not return a held original's credit.
    drop(rx);
    let second = budget.retain(vec![5, 6, 7].into()).unwrap();
    let rejected: PacketBuffer = vec![8].into();
    let rejected_allocation = rejected.as_ptr();
    let rejected = budget.retain(rejected).unwrap_err();
    assert_eq!(rejected.as_ptr(), rejected_allocation);
    drop(second);
    assert!(budget.retain(vec![8, 9, 10, 11].into()).is_err());
    drop(held);
    let recovered = budget.retain(rejected).unwrap();
    assert_eq!(recovered.as_ptr(), rejected_allocation);
}

#[test]
fn failed_delivery_releases_only_when_the_same_returned_buffer_is_dropped() {
    let budget = ReturnBudget::new(1, 4);
    let packet: PacketBuffer = vec![1, 2, 3, 4].into();
    let allocation = packet.as_ptr();
    let packet = budget.retain(packet).unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel::<PacketBuffer>(1);
    drop(rx);
    let returned = tx.try_send(packet).unwrap_err().into_inner();
    assert_eq!(returned.as_ptr(), allocation);
    assert!(budget.retain(vec![0].into()).is_err());
    drop(returned);
    assert!(budget.retain(vec![0].into()).is_ok());
}

#[test]
fn actual_devices_and_retired_buffers_share_one_root_and_block_ceiling_reduction() {
    let totals = Arc::new(PacketReturnTotals::default());
    totals.install(2, 6).unwrap();
    let first = ReturnBudget::with_totals(2, 6, totals.clone());
    let second = ReturnBudget::with_totals(2, 6, totals.clone());
    let held = first.retain(vec![1, 2, 3, 4].into()).unwrap();
    drop(first); // A retired device cannot erase its outstanding buffer debit.
    assert!(second.retain(vec![5, 6, 7].into()).is_err());
    assert_eq!(
        totals.install(1, 3).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let two = second.retain(vec![5, 6].into()).unwrap();
    assert!(second.retain(vec![8].into()).is_err());
    drop(held);
    drop(two);
    totals.install(1, 3).unwrap();
    assert!(second.retain(vec![8, 9, 10].into()).is_ok());
}
