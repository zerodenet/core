//! Cold transport projection into the existing connect and relay executors.
use std::sync::Arc;

use crate::protocol_registry::{TcpConnectResult, TcpRelayResult};
use crate::runtime::tcp_dispatch::operation::TransportLeafTcpOperation;
use crate::runtime::transport_leaf::ProxyTransportTcpLeaf;

pub(super) fn transport_operations<T, E>(leaf: Result<T, E>) -> (TcpConnectResult, TcpRelayResult)
where
    T: ProxyTransportTcpLeaf + 'static,
    E: std::fmt::Display,
{
    match leaf {
        Ok(leaf) => {
            let operation = Arc::new(TransportLeafTcpOperation { leaf });
            (Ok(operation.clone()), Ok(operation))
        }
        Err(error) => (
            Err((
                T::TCP_CONNECT_STAGE,
                format!("{}: {error}", T::TCP_INVALID_CONNECT_CONFIG),
            )),
            Err(format!("{}: {error}", T::TCP_INVALID_RELAY_CONFIG)),
        ),
    }
}
