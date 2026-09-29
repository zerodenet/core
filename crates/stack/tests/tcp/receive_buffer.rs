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
