use super::*;
use gotatun::packet::PacketBufPool;

#[test]
fn protocol_packet_keeps_its_pool_owner_until_runtime_drop() {
    let pool = PacketBufPool::<64>::new(1);
    let mut original = pool.get();
    original.buf_mut()[..4].copy_from_slice(b"live");
    original.truncate(4);
    let pointer = original.as_ptr();
    let mut buffer = owned_packet(original);
    assert_eq!(buffer.as_ptr(), pointer);
    assert_eq!(buffer.as_ref(), b"live");
    buffer[0] = b'L';
    let other = pool.get();
    assert_ne!(other.as_ptr(), pointer, "retained storage cannot be reused");
    assert_eq!(buffer.as_ref(), b"Live");
    drop(buffer);
    let returned = pool.get();
    assert_eq!(
        returned.as_ptr(),
        pointer,
        "original guard returns the allocation"
    );
    drop(other);
}

#[test]
#[ignore = "manual buffer-boundary microbenchmark; not end-to-end performance qualification"]
fn measure_protocol_owner_transfer_against_vector_copy() {
    use std::{hint::black_box, time::Instant};
    const COUNT: usize = 20_000;
    for pooled in [false, true] {
        for length in [32, 148, 1428] {
            let pool = PacketBufPool::<2048>::new(1);
            let payload = vec![0x5a; length];
            let mut copied = Vec::new();
            let mut owned = Vec::new();
            for round in 0..6 {
                for transfer in [round % 2 == 0, round % 2 != 0] {
                    let started = Instant::now();
                    for _ in 0..COUNT {
                        let packet = if pooled {
                            let mut packet = pool.get();
                            packet.truncate(length);
                            packet
                        } else {
                            Packet::copy_from(payload.as_slice())
                        };
                        if transfer {
                            let packet = owned_packet(packet);
                            black_box(packet.as_ref());
                        } else {
                            let bytes = packet.as_ref().to_vec();
                            drop(packet);
                            black_box(bytes.as_slice());
                        }
                    }
                    let elapsed = started.elapsed().as_nanos();
                    if round > 0 {
                        if transfer {
                            owned.push(elapsed);
                        } else {
                            copied.push(elapsed);
                        }
                    }
                }
            }
            copied.sort_unstable();
            owned.sort_unstable();
            println!("pooled={pooled} packets={COUNT} bytes={length} copied_median_ns={} owner_median_ns={} eliminated_payload_copy_bytes={}", copied[2], owned[2], COUNT * length);
        }
    }
}

#[test]
fn vector_boundary_reclaims_unique_engine_storage_without_new_payload_allocation() {
    let packet = Packet::copy_from(&b"exclusive storage"[..]);
    let pointer = packet.as_ptr();
    let bytes = owned_packet(packet).into_vec();
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(bytes, b"exclusive storage");
}

#[test]
fn vector_boundary_preserves_pool_return_guard_when_storage_is_shared() {
    let pool = PacketBufPool::<64>::new(1);
    let packet = pool.get().overwrite_with(&b"pooled storage"[..]);
    let pointer = packet.as_ptr();
    let bytes = owned_packet(packet).into_vec();
    let mut reused = pool.get();
    assert_eq!(reused.as_ptr(), pointer);
    reused.buf_mut()[..bytes.len()].fill(0);
    assert_eq!(
        bytes, b"pooled storage",
        "returned pool cannot overwrite accepted output"
    );
}

#[test]
fn unpooled_packet_keeps_its_slice_offset_and_disjoint_prefix_until_vector_boundary() {
    let mut packet = Packet::copy_from(&b"headpayload"[..]);
    let allocation = packet.as_ptr();
    let prefix = packet.buf_mut().split_to(4);
    let slice = packet.as_ptr();
    let mut buffer = owned_packet(packet);
    assert_eq!(buffer.as_ptr(), slice);
    assert_eq!(buffer.as_ref(), b"payload");
    buffer[0] = b'P';
    assert_eq!(prefix.as_ref(), b"head");
    drop(prefix);
    let bytes = buffer.into_vec();
    assert_eq!(bytes.as_ptr(), allocation);
    assert_eq!(bytes, b"Payload");
}
