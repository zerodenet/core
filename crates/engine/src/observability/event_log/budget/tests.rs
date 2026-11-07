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
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let subscriber = Subscriber::new(tx, EventFilter::default(), 1);
    let budget = subscriber.budget.clone();
    assert!(subscriber.send(&Arc::new(event()), 128));
    let queued = rx.recv().unwrap();
    assert_eq!(budget.load(Ordering::Relaxed), 128);
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
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let subscriber = Subscriber::new(tx, EventFilter::default(), 1);
    let budget = subscriber.budget.clone();
    assert!(subscriber.send(&Arc::new(event()), 128));
    let queued = rx.recv().unwrap();
    assert_eq!(budget.load(Ordering::Relaxed), 128);
    drop(queued);
    assert_eq!(budget.load(Ordering::Relaxed), 0);
}

#[test]
fn full_queue_and_disconnected_consumer_return_their_byte_budget() {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let subscriber = Subscriber::new(tx, EventFilter::default(), 1);
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
        let subscriber = Subscriber::new(tx, EventFilter::default(), 1);
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
    let subscriber = Subscriber::new(tx, EventFilter::default(), 1);
    let mut history = History::default();
    history.push(event.clone(), 128, 1);
    assert!(subscriber.send(&event, 128));
    drop(event);
    history.trim(0);
    let owned = rx.recv().unwrap().into_event();
    assert_eq!(owned.payload["value"].as_str().unwrap().as_ptr(), original);
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 0);
}

mod limits;

#[test]
fn retained_control_event_keeps_same_history_payload_and_subscriber_credit_until_encoding_finishes()
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut subscriber = Subscriber::new(tx, EventFilter::default(), 1);
    let original = Arc::new(event());
    let bytes = stats_size(&original);
    subscriber.stats_limit = bytes;
    let pointer = original.payload["value"].as_str().unwrap().as_ptr();
    assert!(subscriber.send(&original, bytes));
    let retained = crate::RetainedEvent::from_queued(rx.recv().unwrap());
    assert_eq!(
        retained.payload["value"].as_str().unwrap().as_ptr(),
        pointer
    );
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), bytes);
    assert!(subscriber.send(&original, bytes));
    assert!(
        rx.try_recv().is_err(),
        "a held original still consumes its one actual subscriber quota"
    );
    assert_eq!(
        serde_json::to_value(&retained).unwrap(),
        serde_json::to_value(&*original).unwrap()
    );
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), bytes);
    drop(retained);
    assert_eq!(subscriber.budget.load(Ordering::Relaxed), 0);
    assert!(subscriber.send(&original, bytes));
    assert!(
        rx.try_recv().is_ok(),
        "the same existing queue recovers after actual original custody returns"
    );
}
