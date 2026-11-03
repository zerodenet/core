use super::users::{Permit, protocol};
use super::*;
/// Cloneable admission capability only. It retains no raw Endpoint/Connection
/// or native control and becomes unusable when the persistent owner retires.
#[derive(Clone)]
pub struct OwnedEndpointHandle(pub(super) Weak<Shared>);
struct Pending {
    endpoint: quinn::Endpoint,
    permit: Permit,
}
impl OwnedEndpointHandle {
    fn pending(&self) -> Result<(Arc<Shared>, Pending), ChannelError> {
        let state = self
            .0
            .upgrade()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        state.check()?;
        let permit = state.users.acquire()?;
        let endpoint = state
            .endpoint
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        Ok((state, Pending { endpoint, permit }))
    }
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, ChannelError> {
        let (_state, pending) = self.pending()?;
        pending
            .endpoint
            .local_addr()
            .map_err(|source| protocol(Stage::Prepare, source))
    }
    pub async fn connect(
        &self,
        peer: std::net::SocketAddr,
        server_name: &str,
    ) -> Result<OwnedConnection, ChannelError> {
        let (state, pending) = self.pending()?;
        let connecting = pending
            .endpoint
            .connect(peer, server_name)
            .map_err(|source| protocol(Stage::Prepare, source))?;
        // The pinned hook is infallible. Check this same owner's sticky actual
        // SpawnError synchronously before handing Connecting to a user. On
        // rejection it drops here, with the original owner/FD ledger retained.
        state.admission.check()?;
        let result = tokio::select! { biased;
            _ = state.users.cancellation().cancelled() => Err(error(Stage::Handshake, Reason::Cancelled)),
            result = connecting => result.map_err(|source| protocol(Stage::Handshake, source)),
        };
        let connection = result?;
        if let Err(failure) = state.check() {
            connection.close(0u32.into(), b"owner no longer admits users");
            return Err(failure);
        }
        let Pending { endpoint, permit } = pending;
        drop(endpoint);
        Ok(OwnedConnection { connection, permit })
    }
    pub async fn accept_incoming(&self) -> Result<OwnedIncoming, ChannelError> {
        let (state, pending) = self.pending()?;
        let connecting = loop {
            let incoming = tokio::select! {biased;
                _=state.users.cancellation().cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled)),
                incoming=pending.endpoint.accept()=>incoming.ok_or_else(||error(Stage::Prepare,Reason::Revoked))?,
            };
            let callbacks = state
                .callbacks
                .upgrade()
                .ok_or_else(|| error(Stage::Handshake, Reason::Revoked))?;
            match callbacks.call(|| incoming.accept()) {
                Ok(connecting) => break connecting,
                Err(source) => {
                    // A rejected Initial (including ALPN mismatch) may fail
                    // synchronously, before a Connecting exists. Keep driving
                    // the original endpoint so its rejection reaches the peer;
                    // this is not the listener or its native provider failing.
                    state.admission.check()?;
                    state.check()?;
                    tracing::debug!(%source,"original QUIC incoming rejected");
                }
            }
        };
        state.admission.check()?;
        let local = pending
            .endpoint
            .local_addr()
            .map_err(|source| protocol(Stage::Prepare, source))?;
        Ok(OwnedIncoming {
            connecting,
            state,
            pending,
            local,
        })
    }
    pub async fn accept(&self) -> Result<OwnedConnection, ChannelError> {
        self.accept_incoming().await?.establish().await
    }
}

/// Original TLS continuation: one real incoming and its sealed user reservation.
pub struct OwnedIncoming {
    connecting: quinn::Connecting,
    state: Arc<Shared>,
    pending: Pending,
    local: std::net::SocketAddr,
}
impl OwnedIncoming {
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local
    }
    pub fn remote_address(&self) -> std::net::SocketAddr {
        self.connecting.remote_address()
    }
    pub async fn establish(self) -> Result<OwnedConnection, ChannelError> {
        let Self {
            connecting,
            state,
            pending,
            ..
        } = self;
        let connection = tokio::select! {biased;
            _=state.users.cancellation().cancelled()=>return Err(error(Stage::Handshake,Reason::Cancelled)),
            result=connecting=>result.map_err(|source|protocol(Stage::Handshake,source))?,
        };
        if let Err(failure) = state.check() {
            connection.close(0u32.into(), b"owner retired");
            return Err(failure);
        }
        let Pending { endpoint, permit } = pending;
        drop(endpoint);
        Ok(OwnedConnection { connection, permit })
    }
}
