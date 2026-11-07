use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
use tokio::io::AsyncWriteExt;
fn roots(frames: usize, bytes: usize) -> Arc<PacketReturnTotals> {
    Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit { frames, bytes },
    ))
}
fn received(grant: &NativeReadGrant) -> PacketBuffer {
    let mut read = grant.try_buffer().unwrap();
    read.as_mut_bytes()[..5].copy_from_slice(b"owned");
    read.into_packet(5)
        .unwrap_or_else(|_| panic!("valid actual read length"))
}
#[tokio::test]
async fn whole_transmit_reservation_preserves_read_promise_while_actual_write_is_pending() {
    let root = roots(3, 224);
    let read = root.native_grant(64).unwrap();
    let mut original = received(&read);
    let pointer = original.as_ptr();
    original.prepare_transmit(2, 160).unwrap();
    let mut outputs = original.derive_zeroed(&[64, 96]).await.unwrap();
    assert_eq!(
        original.as_ptr(),
        pointer,
        "handoff moves accounting, never copies the input"
    );
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 3,
            bytes: 224
        }
    );
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    // The real original allocation moves to another exclusive byte holder, but
    // its same credit remains attached to this custody until those bytes end.
    let bytes = original.take_vec_allocation().unwrap();
    assert_eq!(bytes.as_ptr(), pointer);
    assert!(original.is_empty());
    assert_eq!(original.allocation_bytes(), 64);
    let cipher = outputs.pop().unwrap();
    let padding = outputs.pop().unwrap();
    drop(padding);
    assert_eq!(root.transmit().occupied.bytes, 224);
    // An actual completed plaintext may be reused by retransmission/PMTU
    // processing without releasing or competing for this packet's envelope.
    let mut reused = original.derive_zeroed(&[64]).await.unwrap();
    let padding = reused.pop().unwrap();
    assert_eq!(root.transmit().occupied.frames, 3);
    let (_reader, mut writer) = tokio::io::duplex(1);
    let mut send = Box::pin(async move {
        let result = writer.write_all(&cipher).await;
        (result, original, bytes, padding, cipher)
    });
    poll_fn(|cx| {
        assert!(
            send.as_mut().poll(cx).is_pending(),
            "the actual write is blocked on its peer"
        );
        Poll::Ready(())
    })
    .await;
    // This is the same single configured read slot. Its promised next buffer
    // remains available even while the old transmit payload really is held.
    let next_read = read.try_buffer().unwrap();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert_eq!(root.transmit().occupied.frames, 3);
    assert!(root.reserve_transmit(&[1]).is_err());
    drop(send); // Only Rust packet allocations end; no native/Scope proof.
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
    assert_eq!(root.native().occupied.bytes, 64);
    drop(next_read);
    assert_eq!(
        root.native().occupied.bytes,
        64,
        "the original IO promise remains live"
    );
    drop(read);
    assert_eq!(root.native().occupied.bytes, 0);
}
#[test]
fn whole_request_capacity_and_temporary_full_preserve_original_and_its_read_credit() {
    let root = roots(2, 127);
    let read = root.native_grant(64).unwrap();
    let mut first = received(&read);
    first.prepare_transmit(0, 0).unwrap();
    let mut next = received(&read);
    let pointer = next.as_ptr();
    assert_eq!(
        next.prepare_transmit(1, 32).unwrap_err(),
        zero_traits::PacketDeriveError::Busy
    );
    assert_eq!(
        next.prepare_transmit(1, 64).unwrap_err(),
        zero_traits::PacketDeriveError::Capacity
    );
    assert_eq!(next.as_ptr(), pointer);
    assert_eq!(&next[..], b"owned");
    assert!(!read.workspace_returned());
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    assert_eq!(
        root.transmit().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    drop(next);
    assert!(read.workspace_returned());
    drop(first);
    assert_eq!(root.transmit().occupied.frames, 0);
    assert_eq!(root.native().occupied.frames, 1);
    drop(read);
    assert_eq!(root.native().occupied.frames, 0);
}
