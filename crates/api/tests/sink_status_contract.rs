use zero_api::{SinkDeliveryStatus, SinkStatus};

#[test]
fn older_sink_status_without_delivery_state_remains_compatible() {
    let status: SinkStatus = serde_json::from_str(
        r#"{
            "name": "receiver",
            "pending": 0,
            "total_delivered": 4,
            "total_failed": 1,
            "replay_gaps": 0,
            "last_success_at_unix_ms": 10,
            "last_failure_at_unix_ms": 9,
            "last_error": null
        }"#,
    )
    .expect("deserialize pre-delivery-state sink status");

    assert_eq!(status.delivery, SinkDeliveryStatus::default());
    let exported = serde_json::to_value(status).expect("serialize idle sink status");
    assert!(exported.get("delivery").is_none());
}

#[test]
fn sink_status_exports_independent_delivery_lifecycle_facts() {
    let status: SinkStatus = serde_json::from_str(
        r#"{
            "name": "receiver",
            "pending": 5,
            "delivery": {
                "in_flight": true,
                "retry_pending": 2,
                "ack_retry_pending": 1,
                "durable_pending": 5,
                "next_retry_at_unix_ms": 1234
            },
            "total_delivered": 7,
            "total_failed": 3,
            "replay_gaps": 1,
            "last_success_at_unix_ms": 10,
            "last_failure_at_unix_ms": 11,
            "last_error": "receiver unavailable"
        }"#,
    )
    .expect("deserialize delivery lifecycle");

    assert!(status.delivery.in_flight);
    assert_eq!(status.delivery.retry_pending, 2);
    assert_eq!(status.delivery.ack_retry_pending, 1);
    assert_eq!(status.delivery.durable_pending, 5);
    assert_eq!(status.delivery.next_retry_at_unix_ms, Some(1234));
}
