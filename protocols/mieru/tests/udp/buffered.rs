use super::*;
use zero_traits::{DatagramCodec, DatagramCodecBufferError, PacketBuffer};
fn storage(lengths: &[usize]) -> Vec<PacketBuffer> {
    lengths.iter().map(|n| alloc::vec![0;*n].into()).collect()
}
#[test]
fn original_mieru_associate_framing_has_fixed_workspace_interoperability() {
    let codec = MieruUdpFlowCodec;
    for target in [
        Address::Ipv4([127, 0, 0, 1]),
        Address::Ipv6([9; 16]),
        Address::Domain("actual.example".into()),
    ] {
        for payload in [alloc::vec![], alloc::vec![29;1480]] {
            let lengths = codec
                .encode_buffer_lengths(&target, 65535, payload.len())
                .unwrap();
            let mut bad = lengths.clone();
            bad[0] -= 1;
            assert!(matches!(
                codec.encode_buffered(&target, 65535, &payload, &mut storage(&bad)),
                Err(DatagramCodecBufferError::Storage(_))
            ));
            let mut buffers = storage(&lengths);
            let output = codec
                .encode_buffered(&target, 65535, &payload, &mut buffers)
                .unwrap();
            let wire = &buffers[output.buffer][output.range];
            assert_eq!(wire, codec.encode(&target, 65535, &payload).unwrap());
            let lengths = codec.decode_buffer_lengths(wire.len()).unwrap();
            for short in 0..lengths.len() {
                let mut bad = lengths.clone();
                bad[short] -= 1;
                assert!(matches!(
                    codec.decode_buffered(wire, &mut storage(&bad)),
                    Err(DatagramCodecBufferError::Storage(_))
                ));
            }
            let mut decoded = storage(&lengths);
            let (actual, port, output) =
                codec.decode_buffered(wire, &mut decoded).unwrap().unwrap();
            assert_eq!(actual, target);
            assert_eq!(port, 65535);
            assert_eq!(&decoded[output.buffer][output.range], payload);
        }
    }
    assert!(codec
        .decode_buffered(&[0; 9], &mut storage(&[9, 255]))
        .unwrap()
        .is_none());
}
