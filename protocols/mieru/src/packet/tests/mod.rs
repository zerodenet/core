use super::{PacketCodec, ReliableSession};
use crate::{metadata::*, segment::Segment, session::MieruSession};
mod congestion;
mod options;
fn data(sequence: u32, ack: u32, window: u16, payload: &[u8]) -> Segment {
    let mut m = DataMetadata::new(DATA_SERVER_TO_CLIENT);
    m.session_id = 1;
    m.sequence_number = sequence;
    m.unack_sequence = ack;
    m.window_size = window;
    m.payload_length = payload.len() as u16;
    Segment {
        session_meta: None,
        data_meta: Some(m),
        payload: payload.into(),
    }
}
#[test]
fn stateless_packets_reject_corruption_truncation_and_bad_padding() {
    let codec = PacketCodec::new([7; 32], "u");
    let mut segment = data(0, 0, 8, b"payload");
    let m = segment.data_meta.as_mut().unwrap();
    m.timestamp = MieruSession::timestamp_minutes();
    m.prefix_length = 7;
    m.suffix_length = 13;
    let wire = codec.encode(&segment).unwrap();
    assert_eq!(codec.decode(&wire).unwrap().payload, b"payload");
    for length in 0..wire.len() {
        assert!(codec.decode(&wire[..length]).is_err());
    }
    let mut corrupted = wire.clone();
    corrupted[80] ^= 1;
    assert!(codec.decode(&corrupted).is_err());
    assert_ne!(&codec.encode(&segment).unwrap()[..24], &wire[..24]);
    assert!(PacketCodec::new([9; 32], "u").decode(&wire).is_err());
}
#[test]
fn duplicate_and_reordered_packets_are_delivered_once_in_sequence() {
    let mut session = ReliableSession::new(1, true);
    session.receive(data(1, 0, 8, b"second")).unwrap();
    assert!(session.peek().is_none());
    session.receive(data(0, 0, 8, b"first")).unwrap();
    session.receive(data(0, 0, 8, b"first")).unwrap();
    assert_eq!(session.peek().unwrap().payload, b"first");
    session.consume().unwrap();
    assert_eq!(session.peek().unwrap().payload, b"second");
    session.consume().unwrap();
    assert!(session.peek().is_none());
    session.receive(data(0, 0, 8, b"first")).unwrap();
    assert!(session.peek().is_none());
    let packets = session.poll_transmissions(8).unwrap();
    assert_eq!(
        packets
            .last()
            .unwrap()
            .segment
            .data_meta
            .as_ref()
            .unwrap()
            .unack_sequence,
        2
    );
}
#[test]
fn zero_window_blocks_new_data_and_invalid_ack_cannot_discard_pending_bytes() {
    let mut session = ReliableSession::new(1, true);
    session.queue_open().unwrap();
    session.poll_transmissions(8).unwrap();
    let mut ack = data(0, 1, 0, &[]);
    ack.data_meta.as_mut().unwrap().protocol_type = ACK_SERVER_TO_CLIENT;
    session.receive(ack.clone()).unwrap();
    session.queue_data(b"keep").unwrap();
    assert!(session
        .poll_transmissions(8)
        .unwrap()
        .iter()
        .all(|p| p.segment.payload.is_empty()));
    assert!(!session.is_flushed(2));
    ack.data_meta.as_mut().unwrap().unack_sequence = 3;
    assert!(session.receive(ack.clone()).is_err());
    assert!(!session.is_flushed(2));
    ack.data_meta.as_mut().unwrap().unack_sequence = 1;
    ack.data_meta.as_mut().unwrap().window_size = 8;
    session.receive(ack).unwrap();
    assert!(session
        .poll_transmissions(8)
        .unwrap()
        .iter()
        .any(|p| p.segment.payload == b"keep"));
}

#[test]
fn remote_window_is_additional_capacity_with_packets_in_flight() {
    let mut session = ReliableSession::new(1, true);
    session.queue_open().unwrap();
    session.poll_transmissions(8).unwrap();

    let mut window = data(0, 0, 4, &[]);
    window.data_meta.as_mut().unwrap().protocol_type = ACK_SERVER_TO_CLIENT;
    session.receive(window).unwrap();
    session.queue_data(&vec![7; 5 * 1100]).unwrap();

    let transmissions = session.poll_transmissions(8).unwrap();
    assert_eq!(
        transmissions
            .iter()
            .filter(|tx| !tx.segment.payload.is_empty())
            .count(),
        4
    );
    assert_eq!(session.queued_len(), 6);
}
#[tokio::test]
async fn lost_segment_retransmits_without_reallocating_sequence() {
    let mut session = ReliableSession::new(1, true);
    session.queue_open().unwrap();
    let first = session.poll_transmissions(8).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(310)).await;
    let retry = session.poll_transmissions(8).unwrap();
    assert_eq!(
        retry[0]
            .segment
            .session_meta
            .as_ref()
            .unwrap()
            .sequence_number,
        first[0]
            .segment
            .session_meta
            .as_ref()
            .unwrap()
            .sequence_number
    );
    assert_eq!(session.next_send, 1);
}

#[test]
fn authenticated_open_replay_is_rejected_across_peer_drivers() {
    let key = rand::random();
    let first = PacketCodec::new(key, "u");
    let next_peer = PacketCodec::new(key, "u");
    let mut meta = SessionMetadata::new(OPEN_SESSION_REQUEST);
    meta.session_id = 42;
    meta.timestamp = MieruSession::timestamp_minutes();
    let segment = Segment {
        session_meta: Some(meta),
        data_meta: None,
        payload: Vec::new(),
    };
    let wire = first.encode(&segment).unwrap();
    assert!(first.decode(&wire).is_ok());
    assert!(first.accept_open(&wire));
    assert!(!next_peer.accept_open(&wire));
    assert!(next_peer.accept_open(&first.encode(&segment).unwrap()));
}

#[test]
fn receive_capacity_is_not_a_sequence_horizon_and_missing_head_has_priority() {
    let mut session = ReliableSession::new(1, true);
    session.receive(data(200, 0, 8, b"later")).unwrap();
    for sequence in 0..200 {
        session.receive(data(sequence, 0, 8, b"head")).unwrap();
        assert_eq!(session.peek().unwrap().payload, b"head");
        session.consume().unwrap();
    }
    assert_eq!(session.peek().unwrap().payload, b"later");
    let mut full = ReliableSession::new(1, true);
    for sequence in 1..=128 {
        full.receive(data(sequence, 0, 8, b"queued")).unwrap();
    }
    full.receive(data(0, 0, 8, b"missing")).unwrap();
    assert_eq!(full.peek().unwrap().payload, b"missing");
    for _ in 0..128 {
        full.consume().unwrap();
    }
    assert!(full.peek().is_none());
    full.receive(data(128, 0, 8, b"retry")).unwrap();
    assert_eq!(full.peek().unwrap().payload, b"retry");
}
