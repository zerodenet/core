use super::*;
fn endpoint() -> Endpoint {
    Endpoint {
        ip: "10.0.0.2".parse().unwrap(),
        port: 53,
    }
}

#[test]
fn transient_slot_pressure_rechecks_after_the_payload_copy_without_counting_a_drop() {
    let (sender, mut receiver) = mpsc::channel(1);
    let budget = QueueBudget::new(Some(65_527), None);
    enqueue_datagram(&sender, &budget, endpoint(), 3, || b"old".to_vec()).unwrap();
    enqueue_datagram(&sender, &budget, endpoint(), 3, || {
        assert!(receiver.try_recv().is_ok());
        b"new".to_vec()
    })
    .unwrap();
    match receiver.try_recv().unwrap().into_event() {
        ClientUdpEvent::Datagram(d) => assert_eq!(d.payload, b"new"),
        _ => panic!("expected datagram"),
    }
    assert_eq!(
        budget
            .state
            .as_ref()
            .unwrap()
            .socket
            .as_ref()
            .unwrap()
            .used
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn byte_admission_is_exact_and_releases_on_delivery_or_queue_destruction() {
    let (sender, mut receiver) = mpsc::channel(4);
    let shared = ByteBudget::new(8);
    let budget = QueueBudget::new(Some(6), Some(shared.clone()));
    for _ in 0..2 {
        enqueue_datagram(&sender, &budget, endpoint(), 3, || vec![1; 3]).unwrap();
    }
    let mut copied = false;
    assert!(matches!(
        enqueue_datagram(&sender, &budget, endpoint(), 1, || {
            copied = true;
            vec![1]
        }),
        Err(mpsc::error::TrySendError::Full(()))
    ));
    assert!(!copied, "no copy for an explicitly exhausted byte budget");
    assert_eq!(shared.used.load(Ordering::Relaxed), 6);
    // Delivery ends the queue charge even while the application owns the payload.
    let held = receiver.try_recv().unwrap().into_event();
    assert_eq!(shared.used.load(Ordering::Relaxed), 3);
    enqueue_datagram(&sender, &budget, endpoint(), 3, || vec![2; 3]).unwrap();
    drop(receiver);
    assert_eq!(shared.used.load(Ordering::Relaxed), 0);
    drop(held);
}

#[test]
fn shared_budget_refusal_rolls_back_the_socket_charge_and_does_not_leak_a_slot() {
    let shared = ByteBudget::new(4);
    let (a, mut ar) = mpsc::channel(4);
    let (b, mut br) = mpsc::channel(4);
    let ba = QueueBudget::new(Some(4), Some(shared.clone()));
    let bb = QueueBudget::new(Some(4), Some(shared.clone()));
    enqueue_datagram(&a, &ba, endpoint(), 4, || vec![1; 4]).unwrap();
    assert!(enqueue_datagram(&b, &bb, endpoint(), 4, || vec![2; 4]).is_err());
    assert_eq!(
        bb.state
            .as_ref()
            .unwrap()
            .socket
            .as_ref()
            .unwrap()
            .used
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(b.capacity(), 4);
    drop(ar.try_recv().unwrap());
    enqueue_datagram(&b, &bb, endpoint(), 4, || vec![2; 4]).unwrap();
    drop(br.try_recv().unwrap());
    assert_eq!(shared.used.load(Ordering::Relaxed), 0);
}

#[test]
fn concurrent_sockets_cannot_exceed_the_shared_byte_budget() {
    let shared = ByteBudget::new(4);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut threads = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..2 {
        let (sender, receiver) = mpsc::channel(2);
        receivers.push(receiver);
        let budget = QueueBudget::new(Some(4), Some(shared.clone()));
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            enqueue_datagram(&sender, &budget, endpoint(), 4, || vec![1; 4]).is_ok()
        }));
    }
    barrier.wait();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
    assert_eq!(shared.used.load(Ordering::Relaxed), 4);
    drop(receivers);
    assert_eq!(shared.used.load(Ordering::Relaxed), 0);
}

#[test]
fn payload_copy_unwind_releases_byte_charges_and_channel_reservation() {
    let (sender, _receiver) = mpsc::channel(1);
    let budget = QueueBudget::new(Some(4), None);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = enqueue_datagram(&sender, &budget, endpoint(), 4, || panic!("copy failed"));
    }))
    .is_err());
    assert_eq!(sender.capacity(), 1);
    assert_eq!(
        budget
            .state
            .as_ref()
            .unwrap()
            .socket
            .as_ref()
            .unwrap()
            .used
            .load(Ordering::Relaxed),
        0
    );
}
