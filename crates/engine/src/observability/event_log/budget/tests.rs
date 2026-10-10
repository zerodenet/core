use super::*;

fn event() -> RawApiEvent {
    ApiEvent::new(
        "memory-fixture",
        event_type::STATS_SCOPES_SAMPLED,
        1,
        json!({ "value": "x".repeat(16_384) }),
    )
}

#[test]
fn consuming_event_moves_payload_and_releases_reservation_once() {
    let budget = Arc::new(AtomicUsize::new(128));
    let queued = QueuedEvent {
        event: Arc::new(event()),
        _reservation: Reservation {
            bytes: 128,
            budget: budget.clone(),
        },
    };
    let address = queued.event.payload["value"].as_str().unwrap().as_ptr();
    let received = queued.into_event();
    assert_eq!(
        received.payload["value"].as_str().unwrap().as_ptr(),
        address
    );
    assert_eq!(budget.load(Ordering::Relaxed), 0);
    drop(received);
    assert_eq!(budget.load(Ordering::Relaxed), 0);
}

#[test]
fn dropping_unconsumed_event_releases_reservation() {
    let budget = Arc::new(AtomicUsize::new(128));
    let queued = QueuedEvent {
        event: Arc::new(event()),
        _reservation: Reservation {
            bytes: 128,
            budget: budget.clone(),
        },
    };
    drop(queued);
    assert_eq!(budget.load(Ordering::Relaxed), 0);
}

#[test]
fn full_queue_and_disconnected_consumer_return_their_byte_budget() {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let subscriber = Subscriber::new(tx, EventFilter::default());
    let event = Arc::new(event());
    assert!(subscriber.send(&event, 128));
    assert!(subscriber.send(&event, 128));
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 128);
    drop(rx.recv().unwrap().into_event());
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 0);
    drop(rx);
    assert!(!subscriber.send(&event, 128));
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 0);
}

#[test]
fn history_and_slow_subscribers_share_one_payload_until_owned_delivery() {
    let event = Arc::new(event());
    let original = event.payload["value"].as_str().unwrap().as_ptr();
    let mut history = History::default();
    history.push(event.clone(), 128, 1);
    let mut queues = Vec::new();
    for _ in 0..8 {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let subscriber = Subscriber::new(tx, EventFilter::default());
        assert!(subscriber.send(&event, 128));
        queues.push((subscriber, rx));
    }
    assert_eq!(Arc::strong_count(&event), 10);
    let (subscriber, rx) = &queues[0];
    let queued = rx.recv().unwrap();
    assert!(Arc::ptr_eq(&queued.event, &event));
    let mut owned = queued.into_event();
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 0);
    owned.payload["value"] = json!("consumer mutation");
    assert_eq!(
        history.front().unwrap().payload["value"]
            .as_str()
            .unwrap()
            .as_ptr(),
        original
    );
    for (_, rx) in queues.iter().skip(1) {
        let queued = rx.recv().unwrap();
        assert!(Arc::ptr_eq(&queued.event, &event));
        drop(queued);
    }
    assert!(queues
        .iter()
        .all(|(subscriber, _)| subscriber.budget.load(Ordering::Relaxed) == 0));
    drop(event);
    history.trim(0);
    assert!(history.is_empty());
}

#[test]
fn history_eviction_preserves_a_queued_event_and_allows_move_on_delivery() {
    let event = Arc::new(event());
    let original = event.payload["value"].as_str().unwrap().as_ptr();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let subscriber = Subscriber::new(tx, EventFilter::default());
    let mut history = History::default();
    history.push(event.clone(), 128, 1);
    assert!(subscriber.send(&event, 128));
    drop(event);
    history.trim(0);
    let owned = rx.recv().unwrap().into_event();
    assert_eq!(owned.payload["value"].as_str().unwrap().as_ptr(), original);
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 0);
}
