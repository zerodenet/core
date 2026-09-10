use std::path::Path;

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
use zero_engine::EngineError;

use super::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpRelayOperation, SocketTcpConnectOperation, SocketTcpHandshake,
    SocketTcpRelayOperation,
};
#[cfg(feature = "tcp-transport-session-runtime")]
use crate::runtime::tcp_dispatch::operation::{SessionTcpConnectOperation, SessionTcpHandshake};
use crate::transport::TcpOutboundFailure;

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) fn claim_socket_tcp_leaf<'a, T>(handshake: T) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    T: SocketTcpHandshake + Clone + Send + Sync + 'a,
{
    let relay_handshake = handshake.clone();
    claim_socket_tcp_leaf_with_relay(handshake, move || {
        Box::new(SocketTcpRelayOperation {
            handshake: relay_handshake.clone(),
        })
    })
}

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
pub(crate) fn claim_socket_tcp_leaf_with_relay<'a, T, F>(
    handshake: T,
    relay: F,
) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    T: SocketTcpHandshake + Clone + Send + Sync + 'a,
    F: Fn() -> Box<dyn PreparedTcpRelayOperation + 'a> + Send + Sync + 'a,
{
    Box::new(ClaimedSocketTcpLeaf { handshake, relay })
}

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
struct ClaimedSocketTcpLeaf<T, F> {
    handshake: T,
    relay: F,
}

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
impl<'a, T, F> ClaimedTcpOutboundLeaf<'a> for ClaimedSocketTcpLeaf<T, F>
where
    T: SocketTcpHandshake + Clone + Send + Sync + 'a,
    F: Fn() -> Box<dyn PreparedTcpRelayOperation + 'a> + Send + Sync + 'a,
{
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure> {
        Ok(Box::new(SocketTcpConnectOperation {
            handshake: self.handshake.clone(),
        }))
    }

    fn prepare_tcp_relay_hop(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpRelayOperation + 'a>, EngineError> {
        Ok((self.relay)())
    }
}

#[cfg(feature = "tcp-transport-session-runtime")]
pub(crate) fn claim_session_tcp_leaf<'a, T>(
    handshake: T,
) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    T: SessionTcpHandshake + Clone + Send + Sync + 'a,
{
    Box::new(ClaimedSessionTcpLeaf { handshake })
}

#[cfg(feature = "tcp-transport-session-runtime")]
struct ClaimedSessionTcpLeaf<T> {
    handshake: T,
}

#[cfg(feature = "tcp-transport-session-runtime")]
impl<'a, T> ClaimedTcpOutboundLeaf<'a> for ClaimedSessionTcpLeaf<T>
where
    T: SessionTcpHandshake + Clone + Send + Sync + 'a,
{
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure> {
        Ok(Box::new(SessionTcpConnectOperation {
            handshake: self.handshake.clone(),
        }))
    }
}
