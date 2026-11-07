use super::TcpReceiveBuffer;
use std::{
    task::{Context, Waker},
    time::{Duration, Instant},
};
use tokio::io::ReadBuf;

#[test]
fn rejection_reports_are_bounded_and_reads_reset_the_episode() {
    let buffer = TcpReceiveBuffer::new(8);
    assert!(buffer.push(b"12345678"));
    let now = Instant::now();
    let first = buffer.rejection_report(now).unwrap();
    assert_eq!(
        (first.buffered, first.available, first.suppressed),
        (8, 0, 0)
    );
    assert!(buffer
        .rejection_report(now + Duration::from_secs(5))
        .is_none());
    assert!(buffer
        .rejection_report(now + Duration::from_secs(10))
        .is_none());
    assert_eq!(
        buffer
            .rejection_report(now + Duration::from_secs(30))
            .unwrap()
            .suppressed,
        2
    );
    let mut byte = [0; 1];
    let mut read = ReadBuf::new(&mut byte);
    let (result, reopened) =
        buffer.poll_read(&mut Context::from_waker(Waker::noop()), &mut read, 4);
    assert!(result.is_ready() && reopened);
    let recovered = buffer
        .rejection_report(now + Duration::from_secs(31))
        .unwrap();
    assert_eq!((recovered.available, recovered.suppressed), (1, 0));
}

#[test]
fn many_small_reads_accumulate_a_window_update_without_ack_per_byte() {
    let buffer = TcpReceiveBuffer::new(16);
    assert!(buffer.push(b"123456789012345"));
    let mut cx = Context::from_waker(Waker::noop());
    for expected in [false, false, false, true] {
        let mut byte = [0; 1];
        let (_, update) = buffer.poll_read(&mut cx, &mut ReadBuf::new(&mut byte), 4);
        assert_eq!(update, expected);
    }
    assert_eq!(buffer.window(), 5);
}

#[cfg(feature = "packet-memory")]
#[test]
fn same_native_receive_backing_wraps_without_per_packet_allocation_or_early_refund() {
    use std::sync::Arc;
    use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
    let root = Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 9,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
    ));
    let read = root.native_grant(1).unwrap();
    let (sender, _receiver) = tokio::sync::mpsc::channel::<zero_traits::PacketBuffer>(1);
    let output = crate::packet_output::PacketSender::from(sender)
        .with_packet_memory_view(
            read.memory_view().unwrap(),
            crate::packet_output::memory::PacketWireMemory::Native,
        )
        .unwrap();
    let buffer = TcpReceiveBuffer::new_for_output(8, &output).unwrap();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 9
        }
    );
    assert!(buffer.push(b"123456"));
    let mut cx = Context::from_waker(Waker::noop());
    let mut first = [0; 4];
    assert!(buffer
        .poll_read(&mut cx, &mut ReadBuf::new(&mut first), 4)
        .0
        .is_ready());
    assert_eq!(&first, b"1234");
    assert!(buffer.push(b"78ABCD"));
    assert!(!buffer.push(b"x"), "same actual window is full");
    let mut rest = [0; 8];
    assert!(buffer
        .poll_read(&mut cx, &mut ReadBuf::new(&mut rest), 4)
        .0
        .is_ready());
    assert_eq!(&rest, b"5678ABCD");
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 9
        },
        "empty ring still retains its actual allocation"
    );
    drop(buffer);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 1
        }
    );
    drop(output);
    drop(read);
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 0,
            bytes: 0
        }
    );
}
