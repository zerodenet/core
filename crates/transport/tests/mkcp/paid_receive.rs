use super::{
    state::Connection,
    wire::{Body, Segment},
    Settings,
};
use bytes::Bytes;
use std::num::NonZeroUsize;
use zero_platform_tokio::controlled_carrier::CarrierOriginalTotals;
#[test]
fn original_paid_carrier_frame_survives_demux_decode_reordering_and_last_application_slice() {
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let book = root.buffer_budget();
    let payload = b"same original paid DATA";
    let frame = Segment {
        conv: 9,
        option: 0,
        body: Body::Data {
            timestamp: 7,
            number: 0,
            sending_next: 0,
            payload: Bytes::from_static(payload),
        },
    }
    .encode();
    let bytes = frame.len();
    let lane = book.try_working_lane(bytes).unwrap();
    let credit = lane.try_reserve(bytes).unwrap();
    let original = crate::datagram_queue::Packet {
        bytes: frame,
        peer: "127.0.0.1:443".parse().unwrap(),
        metadata: None,
        _credit: Some(credit.into()),
    };
    let address = original.bytes.as_ptr() as usize;
    let packet = Bytes::from_owner(original);
    let peek = Segment::decode_owned(&mut packet.clone()).unwrap();
    let Body::Data { payload: peek, .. } = peek.body else {
        panic!("original DATA");
    };
    assert_eq!(peek.as_ptr() as usize, address + 18);
    drop(peek);
    drop(lane);
    let mut connection = Connection::new(9, Settings::default());
    connection.input_owned(packet, 17);
    assert_eq!(
        book.used_bytes(),
        bytes,
        "the original frame is still in the actual receiving window"
    );
    let application = connection.receiver.front().unwrap();
    assert_eq!(application.as_ref(), payload);
    assert_eq!(application.as_ptr() as usize, address + 18);
    connection.receiver.consume();
    drop(connection);
    assert_eq!(
        book.used_bytes(),
        bytes,
        "last application slice remains paid after window retirement"
    );
    drop(application);
    assert_eq!(book.used_bytes(), 0);
}
