use super::*;

impl ClientUdpStack {
    /// Active original sockets and stored fragments remain business custody.
    pub fn quiescent(&self) -> bool {
        if !self.inner.sockets.lock().unwrap().is_empty() {
            return false;
        }
        self.inner.fragments.lock().unwrap().pending_datagrams() == 0
    }
}
