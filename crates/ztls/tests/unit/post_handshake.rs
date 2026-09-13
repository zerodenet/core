use super::*;
#[test]
fn fragmented_tickets_are_validated_and_other_messages_return_errors() {
    let ticket = [4, 0, 0, 14, 0, 0, 0, 60, 0, 0, 0, 1, 0, 0, 1, 7, 0, 0];
    let mut sink = TicketSink::default();
    for byte in ticket {
        sink.receive(&[byte]).unwrap();
    }
    assert!(sink.pending.is_empty());
    let mut malformed = ticket;
    malformed[14] = 2;
    assert!(TicketSink::default().receive(&malformed).is_err());
    assert!(TicketSink::default().receive(&[24, 0, 0, 1, 1]).is_err());
    assert!(TicketSink::default().receive(&[4, 255, 255, 255]).is_err());
    assert!(TicketSink::default().receive(&vec![0; 65537]).is_err());
}

#[test]
fn key_update_fragments_require_valid_request_and_record_boundary() {
    for requested in [0, 1] {
        let mut sink = TicketSink::default();
        assert_eq!(sink.receive_record(&[24, 0], false).unwrap(), None);
        assert!(sink.ensure_complete().is_err());
        assert_eq!(sink.receive_record(&[0, 1], false).unwrap(), None);
        assert_eq!(
            sink.receive_record(&[requested], false).unwrap(),
            Some(requested == 1)
        );
        sink.ensure_complete().unwrap();
    }
    for bytes in [
        vec![24, 0, 0, 1, 2],
        vec![24, 0, 0, 2, 0, 0],
        vec![24, 0, 0, 1, 0, 24],
    ] {
        assert!(TicketSink::default().receive_record(&bytes, true).is_err());
    }
    assert!(TicketSink::default()
        .receive_record(&[4, 0, 0, 1], false)
        .is_err());
}

#[test]
fn non_advancing_key_updates_are_bounded_and_application_data_resets_the_limit() {
    let mut sink = TicketSink::default();
    for _ in 0..32 {
        sink.receive_record(&[24, 0, 0, 1, 1], false).unwrap();
    }
    sink.application_data(true).unwrap();
    for _ in 0..32 {
        sink.receive_record(&[24, 0, 0, 1, 1], false).unwrap();
    }
    assert!(sink.receive_record(&[24, 0, 0, 1, 1], false).is_err());
}
