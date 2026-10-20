use super::users::{protocol, Permit};
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
            _ = state.users.cancel.cancelled() => Err(error(Stage::Handshake, Reason::Cancelled)),
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
    pub async fn accept(&self) -> Result<OwnedConnection, ChannelError> {
        let (state, pending) = self.pending()?;
        let incoming = tokio::select! { biased;
            _ = state.users.cancel.cancelled() => return Err(error(Stage::Prepare, Reason::Cancelled)),
            incoming = pending.endpoint.accept() => incoming.ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?,
        };
        let connecting = incoming
            .accept()
            .map_err(|source| protocol(Stage::Handshake, source))?;
        state.admission.check()?;
        let result = tokio::select! { biased;
            _ = state.users.cancel.cancelled() => Err(error(Stage::Handshake, Reason::Cancelled)),
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
}
