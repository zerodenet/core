//! Callbacks only after the original native and poller gates have been released.
use super::super::observation::Observer;
use super::*;

pub(super) fn send(
    observer: Observer,
    result: &Result<MetadataSendOutcome, MetadataSendFailure>,
    stride: Option<usize>,
) {
    let Some(observer) = observer else { return };
    match result {
        Ok(outcome) => {
            let mut remaining = outcome.bytes;
            for _ in 0..outcome.datagrams {
                let bytes = stride.map_or(remaining, |stride| remaining.min(stride));
                observer.sent_datagram(bytes);
                remaining -= bytes;
            }
        }
        Err(failure) if failure.error.kind() == std_io::ErrorKind::WouldBlock => {}
        Err(_) => observer.error(),
    }
}
pub(super) fn receive(
    observer: Observer,
    result: &Poll<std_io::Result<usize>>,
    metadata: &[MetadataReceive],
) {
    let Some(observer) = observer else { return };
    match result {
        Poll::Ready(Ok(count)) => {
            for item in &metadata[..*count] {
                let mut remaining = item.len;
                // A zero-length datagram is one actually received packet.
                loop {
                    let bytes = if item.stride == 0 {
                        remaining
                    } else {
                        remaining.min(item.stride)
                    };
                    observer.received_datagram(bytes);
                    remaining -= bytes;
                    if remaining == 0 {
                        break;
                    }
                }
            }
        }
        Poll::Ready(Err(error)) if error.kind() != std_io::ErrorKind::WouldBlock => {
            observer.error()
        }
        _ => {}
    }
}
