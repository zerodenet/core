use super::*;
use std::sync::Arc;
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
#[tokio::test]
async fn cached_syn_and_actual_outgoing_cipher_fit_control_plus_three_frame_envelope() {
    let limit = PacketMemoryLimit {
        frames: 4,
        bytes: 65_536,
    };
    let root = Arc::new(PacketReturnTotals::new_with_transmit(limit, limit, limit));
    let read = root.native_grant(128).unwrap();
    let control = root.transmit_workspace(148).unwrap();
    let (tx, _rx) = tokio::sync::mpsc::channel::<PacketBuffer>(1);
    let output = PacketSender::from(tx)
        .with_packet_memory_view(
            read.memory_view().unwrap(),
            crate::packet_output::memory::PacketWireMemory::PaddedEnvelope {
                multiple: 16,
                overhead: 32,
            },
        )
        .unwrap();
    let packet = CachedPacket::build(
        TcpPacketSpec {
            source: "10.0.0.1".parse().unwrap(),
            destination: "198.51.100.10".parse().unwrap(),
            source_port: 40_000,
            destination_port: 80,
            sequence: 7,
            acknowledgment: 0,
            flags: crate::packet::tcp_flags::SYN,
            window: 65_535,
            mss: Some(1380),
            payload: &[],
        },
        &output,
    )
    .unwrap();
    assert_eq!(root.transmit().occupied.frames, 4);
    assert_eq!(root.transmit().limit.frames, 4);
    let outgoing = packet.outgoing().unwrap();
    assert_eq!(outgoing.len(), 44);
    assert!(crate::packet::parse_tcp(&outgoing).unwrap().syn);
    let mut cipher = outgoing.try_derive_zeroed(&[80]).unwrap();
    assert_eq!(cipher.len(), 1);
    assert_eq!(cipher[0].len(), 80);
    assert_eq!(
        root.transmit().occupied.frames,
        4,
        "outgoing and cipher split their existing promise without more credit"
    );
    assert!(root.transmit_packet(1, 0, 0).is_err());
    drop(cipher.pop());
    drop(outgoing);
    assert_eq!(
        root.transmit().occupied.frames,
        4,
        "all promises stay with the actual retransmission original until ACK removes it"
    );
    drop(packet);
    assert_eq!(root.transmit().occupied.frames, 1);
    drop(control);
    assert_eq!(root.transmit().occupied.frames, 0);
}
