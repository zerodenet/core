//! A synchronous original-socket borrow. It never crosses an await or a task.
use super::*;
use std::ops::{Deref, DerefMut};
pub(super) struct SocketLease<'a> {
    shared: &'a Shared,
    socket: Option<Socket>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl Shared {
    pub(super) fn lease(&self) -> Option<SocketLease<'_>> {
        let observer = self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let unknown = Arc::new(io::Error::other(
            "original TCP is in a synchronous native operation",
        ));
        self.wakes.enter();
        let socket = {
            let mut state = self.state();
            if let Some(socket) = state.socket.as_ref() {
                state.metadata = Some(Metadata {
                    local: socket.local_addr().ok(),
                    peer: socket.peer_addr().ok(),
                    interface: socket.egress_interface().cloned(),
                });
                state.unknown = Some(unknown);
                state.polling = true;
                state.socket.take()
            } else {
                None
            }
        };
        match socket {
            Some(socket) => Some(SocketLease {
                shared: self,
                socket: Some(socket),
                observer,
            }),
            None => {
                self.wakes.leave();
                None
            }
        }
    }
}
impl Deref for SocketLease<'_> {
    type Target = Socket;
    fn deref(&self) -> &Socket {
        self.socket.as_ref().expect("synchronous socket borrow")
    }
}
impl DerefMut for SocketLease<'_> {
    fn deref_mut(&mut self) -> &mut Socket {
        self.socket.as_mut().expect("synchronous socket borrow")
    }
}
struct Release<'a>(&'a IoRelays);
impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.0.leave();
    }
}
impl Drop for SocketLease<'_> {
    fn drop(&mut self) {
        let _release = Release(&self.shared.wakes);
        let observed = self.shared.observations.take();
        let (unknown, metadata) = {
            let mut state = self.shared.state();
            // This is the same original object, including any Windows
            // registration's consumed-unknown marker. No fd is reconstructed.
            state.socket = self.socket.take();
            state.polling = false;
            (state.unknown.take(), state.metadata.take())
        };
        drop((unknown, metadata));
        // Even a poll panic restores the original owner before external user
        // code. Native registration may drop old caller wakers while borrowed;
        // those callbacks can observe only unfinished ownership, never Closed.
        observed.dispatch(self.observer.as_deref());
    }
}
