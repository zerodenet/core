use super::*;
use std::{future::Future, task::Poll};
fn root(frames: usize, bytes: usize) -> Arc<PacketReturnTotals> {
    Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 8,
            bytes: 1024,
        },
        PacketMemoryLimit { frames, bytes },
    ))
}
#[tokio::test]
async fn native_read_credit_moves_with_exact_buffer_and_busy_wait_uses_real_release() {
    let root = root(1, 64);
    let grant = root.native_grant(64).unwrap();
    let mut read = grant.try_buffer().unwrap();
    read.as_mut_bytes()[..5].copy_from_slice(b"owned");
    let original = read.as_bytes().as_ptr();
    let payload = match read.into_packet(5) {
        Ok(packet) => packet,
        Err(_) => panic!("valid read length"),
    };
    assert_eq!(payload.as_ptr(), original);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    let mut waiting = Box::pin(grant.buffer());
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert_eq!(
        root.native().occupied.bytes,
        64,
        "cancelling allocation cannot refund another original"
    );
    drop(payload);
    let actual_next = grant.buffer().await.unwrap();
    assert_eq!(root.native().occupied.frames, 1);
    drop(actual_next);
    assert_eq!(
        root.native().occupied.bytes,
        64,
        "same active IO still prepays its next actual read"
    );
    grant.revoke();
    assert_eq!(root.native().occupied.bytes, 0);
}
#[test]
fn codec_and_native_workspaces_are_reserved_atomically_before_any_native_acquisition() {
    let root = root(1, 128);
    assert!(matches!(
        root.native_grants(&[64, 64]),
        Err(PacketMemoryError::Capacity)
    ));
    assert_eq!(root.native().occupied.frames, 0);
    root.install_native(2, 128).unwrap();
    let grants = root.native_grants(&[64, 64]).unwrap();
    let scratch = grants[0].try_buffer().unwrap();
    let decode = grants[1].try_workspace().unwrap();
    assert_eq!(root.native().occupied.frames, 2);
    let packet = match decode.retain(vec![1, 2, 3].into()) {
        Ok(packet) => packet,
        Err(_) => panic!("bounded actual codec packet"),
    };
    drop(scratch);
    grants[0].revoke();
    grants[1].revoke();
    assert_eq!(
        root.native().occupied.bytes,
        64,
        "actual decrypted packet still owns its workspace"
    );
    assert_eq!(packet.as_ref(), &[1, 2, 3]);
    drop(packet);
    assert_eq!(root.native().occupied.frames, 0);
}
#[test]
fn actual_retained_read_and_return_buffers_prevent_lower_limit_and_pause_refunds_nothing() {
    let root = root(2, 128);
    let grant = root.native_grant(64).unwrap();
    let read = grant.try_buffer().unwrap();
    let returns = root.reserve_returns(&[40, 60]).unwrap();
    assert_eq!(root.returns().occupied.bytes, 100);
    assert_eq!(
        root.install(1, 80).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    let pause = root.pause_allocations().unwrap();
    assert!(matches!(
        root.native_grant(32),
        Err(PacketMemoryError::Paused)
    ));
    assert_eq!(root.native().occupied.bytes, 64);
    assert_eq!(root.returns().occupied.frames, 2);
    drop(pause);
    drop(read);
    grant.revoke();
    drop(returns);
    assert_eq!(root.native().occupied.frames, 0);
    assert_eq!(root.returns().occupied.bytes, 0);
    root.install(1, 80).unwrap();
}

#[test]
fn full_fragment_cache_cannot_take_the_original_io_next_read_credit() {
    let root = root(2, 128);
    let grant = root.native_grant(64).unwrap();
    let mut input = grant.try_buffer().unwrap();
    input.as_mut_bytes()[..3].copy_from_slice(b"one");
    let input = match input.into_packet(3) {
        Ok(packet) => packet,
        Err(_) => panic!("valid original fragment length"),
    };
    let mut assembly = grant.try_assembly(64).unwrap();
    assembly.as_mut_bytes()[..3].copy_from_slice(&input);
    assert_eq!(root.native().occupied.frames, 2);
    assert!(matches!(
        root.native_grant(1),
        Err(PacketMemoryError::Busy)
    ));
    drop(input); // Its actual data has entered the separately charged assembly.
    let mut next = grant
        .try_buffer()
        .expect("same prepaid IO credit cannot be stolen by the full cache");
    next.as_mut_bytes()[..3].copy_from_slice(b"two");
    assembly.as_mut_bytes()[3..6].copy_from_slice(&next.as_bytes()[..3]);
    assert_eq!(&assembly.as_bytes()[..6], b"onetwo");
    grant.revoke();
    assert_eq!(
        root.native().occupied.frames,
        2,
        "revoking admission cannot refund real held allocations"
    );
    drop(next);
    assert_eq!(root.native().occupied.frames, 1);
    drop(assembly);
    assert_eq!(root.native().occupied.frames, 0);
}
