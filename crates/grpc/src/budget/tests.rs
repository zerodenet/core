use super::*;
use prost::Message;
#[test]
fn same_borrowed_original_frame_matches_actual_prost_for_empty_unicode_and_maximum_timestamp() {
    for timestamp in [0, 1, 127, 128, u64::MAX] {
        for text in ["", "id", "雪\n\""] {
            let mut original =
                zero_api::ApiEvent::new(text, text, timestamp, serde_json::json!({"text": text}));
            original.sequence = Some(1);
            let encoded = PbEvent {
                event_type: original.event_type.clone(),
                event_id: original.event_id.clone(),
                occurred_at: original.occurred_at_unix_ms,
                payload: serde_json::to_vec(&original.payload).unwrap(),
            };
            assert_eq!(
                original_frame_len(&original).unwrap(),
                encoded.encoded_len() + 5
            );
        }
    }
}
