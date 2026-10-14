use super::*;
use std::collections::HashSet;

#[test]
fn identical_timestamps_and_base_ids_do_not_merge_distinct_generated_facts() {
    let log = EngineEventLog::with_capacity(32);
    for index in 0..24 {
        log.push_generated(ApiEvent::new(
            "warn-1",
            event_type::ENGINE_WARNING,
            1,
            json!({"code":format!("fact-{index}")}),
        ));
    }
    let events = log.snapshot(&EventFilter::default());
    assert_eq!(events.len(), 24);
    assert_eq!(
        events
            .iter()
            .map(|event| &event.event_id)
            .collect::<HashSet<_>>()
            .len(),
        24
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.occurred_at_unix_ms == 1)
            .count(),
        24
    );
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event.sequence, Some(index as u64 + 1));
    }
}

#[test]
fn prepared_facts_keep_identity_and_snapshots_do_not_consume_publication_sequence() {
    let log = EngineEventLog::with_capacity(8);
    let first = log.flow_snapshot_event(&[], 0, &[]);
    let second = log.flow_snapshot_event(&[], 0, &[]);
    assert_ne!(first.event_id, second.event_id);
    assert_eq!(first.sequence, Some(0));
    assert_eq!(second.sequence, Some(0));
    let fact = log.qualify_generated(ApiEvent::new(
        "prepared",
        event_type::FLOW_COMPLETED,
        1,
        json!({"bytes_up":17}),
    ));
    let identity = fact.event_id.clone();
    assert_eq!(log.latest_sequence(), 0);
    log.push_prepared_generated(fact);
    let replay = log.events_since(0, 8, &EventFilter::default());
    assert!(!replay.has_gap);
    assert_eq!(replay.events[0].sequence, Some(1));
    assert_eq!(replay.events[0].event_id, identity);
}

#[test]
fn concurrent_generated_facts_have_unique_ids_even_with_the_same_base_id() {
    let log = Arc::new(EngineEventLog::with_capacity(128));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let log = log.clone();
            scope.spawn(move || {
                for _ in 0..24 {
                    log.push_generated(ApiEvent::new(
                        "same",
                        event_type::CONFIG_CHANGED,
                        1,
                        json!({"revision":1}),
                    ));
                }
            });
        }
    });
    let events = log.snapshot(&EventFilter::default());
    assert_eq!(events.len(), 96);
    assert_eq!(
        events
            .iter()
            .map(|event| &event.event_id)
            .collect::<HashSet<_>>()
            .len(),
        96
    );
}
