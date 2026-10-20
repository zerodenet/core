//! Crate-private accepted adoption. Persistent caller entry precedes this call;
//! its control must be installed before configuration, metadata or external code.
use super::*;
impl ControlledTcpStream {
    pub(crate) fn from_accepted(
        socket: tokio::net::TcpStream,
        parent: CancellationToken,
    ) -> (Self, ControlledTcpControl) {
        Self::new_labeled(
            TokioSocket::new(socket),
            parent,
            "native accepted TCP".into(),
        )
    }
    pub(crate) fn configure_accepted(&self) -> io::Result<()> {
        let Some(socket) = self.shared.lease() else {
            return Err(closed());
        };
        match &*socket {
            Socket::Tokio(socket) => socket.inner.native()?.set_nodelay(true),
            #[cfg(any(unix, windows))]
            Socket::Native(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not an accepted Tokio original",
            )),
        }
    }
}
