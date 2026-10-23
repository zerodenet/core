//! External observers run only after the native/listener gates are released.
use super::*;
use std::{net::SocketAddr, task::Poll};
pub(super) type Observer = Option<Arc<dyn zero_traits::IoObserver>>;
pub(super) type ReceivePoll = Poll<std_io::Result<(usize, SocketAddr)>>;
pub(super) fn send(observer: Observer, result: &Poll<std_io::Result<usize>>) {
    if let (Some(observer), Poll::Ready(result)) = (observer, result) {
        match result {
            Ok(bytes) => observer.sent_datagram(*bytes),
            Err(_) => observer.error(),
        }
    }
}
pub(super) fn receive(observer: Observer, result: &ReceivePoll) {
    if let (Some(observer), Poll::Ready(result)) = (observer, result) {
        match result {
            Ok((bytes, _)) => observer.received_datagram(*bytes),
            Err(_) => observer.error(),
        }
    }
}
pub(super) fn boundary(observer: &Observer, old: Observer) {
    // Keep old observation alive through the callback, then drop it with no
    // original-I/O mutex held. Observer implementations may themselves Drop
    // another same-listener view or synchronously retire the original.
    if let Some(observer) = observer {
        observer.datagram_boundary();
    }
    drop(old);
}
