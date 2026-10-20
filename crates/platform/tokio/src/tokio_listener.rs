use std::{fmt, io, net::SocketAddr};

use tokio::{net::TcpListener, sync::Mutex};
use tokio_util::sync::CancellationToken;
use zero_traits::{SocketAddress, TcpListener as TcpListenerTrait};

use crate::{
    socket_addr_to_socket_address, ControlledTcpListener, ControlledTcpListenerControl, TokioSocket,
};

enum Inner {
    Native(TcpListener),
    Controlled {
        listener: Mutex<ControlledTcpListener>,
        address: SocketAddr,
        accepted: Option<crate::AcceptedTcpTaskReceiver>,
    },
}

/// A listener's accept view. A controlled view keeps its existing native owner;
/// adapting it never creates another socket or another close authority.
pub struct TokioListener {
    inner: Inner,
}

impl fmt::Debug for TokioListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioListener")
            .field("local_addr", &self.local_addr())
            .field(
                "controlled",
                &matches!(self.inner, Inner::Controlled { .. }),
            )
            .finish()
    }
}

impl TokioListener {
    #[cfg(test)]
    pub(crate) fn native_listener_for_test(&self) -> &TcpListener {
        match &self.inner {
            Inner::Native(listener) => listener,
            Inner::Controlled { .. } => panic!("test requires the original ordinary listener"),
        }
    }

    pub async fn bind_addr(addr: SocketAddr) -> io::Result<Self> {
        TcpListener::bind(addr).await.map(|listener| Self {
            inner: Inner::Native(listener),
        })
    }

    pub async fn bind(addr: &str) -> io::Result<Self> {
        TcpListener::bind(addr).await.map(|listener| Self {
            inner: Inner::Native(listener),
        })
    }

    /// Consume the same unique accept view after its control has been installed
    /// in the persistent owner. This method does no native acquisition.
    pub fn from_controlled(
        listener: ControlledTcpListener,
    ) -> Result<Self, crate::contract::ChannelError> {
        let address = listener.activation_address()?;
        let accepted = listener.accepted_task_receiver();
        Ok(Self {
            inner: Inner::Controlled {
                listener: Mutex::new(listener),
                address,
                accepted,
            },
        })
    }

    pub async fn accept(&self) -> io::Result<(TokioSocket, Option<SocketAddress>)> {
        <Self as TcpListenerTrait>::accept(self).await
    }

    /// Same-entry task graph identity, cached before handing out this view.
    /// None does not grant unlimited accepted children or closure proof.
    pub fn accepted_task_receiver(&self) -> Option<crate::AcceptedTcpTaskReceiver> {
        match &self.inner {
            Inner::Native(_) => None,
            Inner::Controlled { accepted, .. } => accepted.clone(),
        }
    }
    /// Owned accept cannot fall back to a bare native socket or second owner.
    pub async fn accept_owned(
        &self,
    ) -> Result<(crate::AcceptedTcp, Option<SocketAddress>), crate::contract::ChannelError> {
        match &self.inner {
            Inner::Controlled {
                listener,
                accepted: Some(_),
                ..
            } => {
                let (io, address) = listener.lock().await.accept_owned().await?;
                Ok((io, Some(socket_addr_to_socket_address(address))))
            }
            _ => Err(crate::contract::ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::Unsupported,
                crate::contract::Impact::Component,
            )),
        }
    }
    /// Return the original bound address. For a controlled view this remains
    /// available while an accept is pending; it is not a native-close probe.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.inner {
            Inner::Native(listener) => listener.local_addr(),
            Inner::Controlled { address, .. } => Ok(*address),
        }
    }

    /// Transfer an ordinary listener into explicit close ownership. An already
    /// controlled view is returned intact because its owner retains the unique
    /// control; no second close authority can be extracted from that view.
    pub fn try_into_controlled(
        self,
        parent: CancellationToken,
    ) -> Result<(ControlledTcpListener, ControlledTcpListenerControl), Self> {
        match self.inner {
            Inner::Native(listener) => Ok(ControlledTcpListener::new(listener, parent)),
            inner @ Inner::Controlled { .. } => Err(Self { inner }),
        }
    }

    /// Move an ordinary bound listener into explicit close ownership.
    ///
    /// # Panics
    /// Panics if this view was constructed with [`Self::from_controlled`]. Use
    /// [`Self::try_into_controlled`] when the ownership mode is not known.
    pub fn into_controlled(
        self,
        parent: CancellationToken,
    ) -> (ControlledTcpListener, ControlledTcpListenerControl) {
        self.try_into_controlled(parent)
            .expect("an existing controlled listener retains its original close owner")
    }
}

impl TcpListenerTrait for TokioListener {
    type Stream = TokioSocket;
    type Error = io::Error;

    async fn accept(&self) -> Result<(Self::Stream, Option<SocketAddress>), Self::Error> {
        match &self.inner {
            Inner::Native(listener) => {
                let (stream, remote) = listener.accept().await?;
                stream.set_nodelay(true)?;
                Ok((
                    TokioSocket::new(stream),
                    Some(socket_addr_to_socket_address(remote)),
                ))
            }
            Inner::Controlled { listener, .. } => {
                // One unique view keeps one native accept waker. A canceled
                // caller releases this guard without retiring the listener.
                let (stream, remote) = listener.lock().await.accept().await?;
                Ok((stream, Some(socket_addr_to_socket_address(remote))))
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/tokio_listener/mod.rs"]
mod tests;
