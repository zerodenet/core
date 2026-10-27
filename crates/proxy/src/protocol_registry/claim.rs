use std::path::Path;

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
use zero_engine::EngineError;

use super::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpRelayOperation, SocketTcpHandshake, SocketTcpOperation,
};
#[cfg(feature = "tcp-transport-session-runtime")]
use crate::runtime::tcp_dispatch::operation::{SessionTcpConnectOperation, SessionTcpHandshake};
use crate::transport::TcpOutboundFailure;

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) fn claim_socket_tcp_leaf<'a, T>(handshake: T) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    T: SocketTcpHandshake + Clone + Send + Sync + 'static,
{
    Box::new(SocketTcpOperation { handshake })
}

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
impl<'a, T> ClaimedTcpOutboundLeaf<'a> for SocketTcpOperation<T>
where
    T: SocketTcpHandshake + Clone + Send + Sync + 'static,
{
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        Ok(Box::new(Self {
            handshake: self.handshake.clone(),
        }))
    }
    fn prepare_tcp_relay_hop(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpRelayOperation>, EngineError> {
        Ok(Box::new(Self {
            handshake: self.handshake.clone(),
        }))
    }
}

#[cfg(feature = "tcp-transport-session-runtime")]
pub(crate) fn claim_session_tcp_leaf<'a, T>(
    handshake: T,
) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    T: SessionTcpHandshake + Clone + Send + Sync + 'static,
{
    Box::new(SessionTcpConnectOperation { handshake })
}

#[cfg(feature = "tcp-transport-session-runtime")]
impl<'a, T> ClaimedTcpOutboundLeaf<'a> for SessionTcpConnectOperation<T>
where
    T: SessionTcpHandshake + Clone + Send + Sync + 'static,
{
    fn prepare_tcp_connect(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        Ok(Box::new(Self {
            handshake: self.handshake.prepare_for_source(source_dir),
        }))
    }
}
