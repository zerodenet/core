use super::{
    state::Connection,
    wire::{Body, Segment},
    Settings,
};
use bytes::Bytes;

#[test]
fn m_kcp_reference_frame_vectors_use_big_endian_lengths_and_sequences() {
    let data = [
        0x12, 0x34, 1, 1, 0, 0, 0, 7, 0, 0, 0, 9, 0, 0, 0, 8, 0, 3, 0xaa, 0xbb, 0xcc,
    ];
    let mut bytes = data.as_slice();
    let segment = Segment::decode(&mut bytes).unwrap();
    assert!(bytes.is_empty());
    assert_eq!(segment.encode(), data);
    assert!(matches!(
        segment.body,
        Body::Data {
            timestamp: 7,
            number: 9,
            sending_next: 8,
            ..
        }
    ));
    let ack = [
        0x12, 0x34, 0, 0, 0, 0, 0, 32, 0, 0, 0, 10, 0, 0, 0, 7, 2, 0, 0, 0, 8, 0, 0, 0, 9,
    ];
    let decoded = Segment::decode(&mut ack.as_slice()).unwrap();
    assert_eq!(decoded.encode(), ack);
    for end in 0..data.len() {
        assert!(Segment::decode(&mut &data[..end]).is_err());
    }
}
#[test]
fn m_kcp_recovers_lost_data_and_acks_without_reordering_or_duplicating_bytes() {
    let settings = Settings {
        mtu: 64,
        tti_ms: 10,
        uplink_capacity_mib: 0,
        downlink_capacity_mib: 0,
        congestion: true,
        ..Settings::default()
    };
    let mut sender = Connection::new(9, settings);
    let mut receiver = Connection::new(9, settings);
    for n in 0u8..8 {
        sender.write(Bytes::from(vec![n; 46]));
    }
    let mut delivered = Vec::new();
    let mut dropped_data = false;
    let mut dropped_ack = false;
    for now in (0..3000).step_by(10) {
        for packet in sender.flush(now).into_iter().rev() {
            let segment = Segment::decode(&mut packet.as_slice()).unwrap();
            if !dropped_data && matches!(segment.body, Body::Data { number: 2, .. }) {
                dropped_data = true;
                continue;
            }
            receiver.input(&packet, now);
            receiver.input(&packet, now);
        }
        while let Some(bytes) = receiver.receiver.front() {
            delivered.extend_from_slice(&bytes);
            receiver.receiver.consume();
        }
        for packet in receiver.flush(now) {
            if !dropped_ack {
                dropped_ack = true;
                continue;
            }
            sender.input(&packet, now);
        }
        if sender.sender.is_empty() {
            break;
        }
    }
    assert!(dropped_data && dropped_ack);
    assert!(sender.sender.is_empty());
    assert_eq!(
        delivered,
        (0u8..8).flat_map(|n| vec![n; 46]).collect::<Vec<_>>()
    );
}
#[test]
fn m_kcp_unresponsive_peers_and_local_close_have_bounded_teardown() {
    let mut connection = Connection::new(7, Settings::default());
    connection.flush(30000);
    connection.flush(38001);
    assert!(connection.finished());
    let mut connection = Connection::new(8, Settings::default());
    connection.close(0);
    assert!(!connection.flush(0).is_empty());
    connection.flush(8001);
    assert!(connection.finished());
}
