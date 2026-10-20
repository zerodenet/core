//! Additive Unix preparation; existing production dial entrypoints are unchanged.
use super::prepare;
use crate::{
    controlled_tcp::PendingTcpConnect, egress::bind_tcp_to_interface, ControlledTcpControl,
    EgressSelection, TcpConnectError,
};
use std::{
    net::{SocketAddr, TcpStream},
    os::fd::{FromRawFd, IntoRawFd},
    sync::Arc,
};
use tokio::net::TcpSocket;
use tokio_util::sync::CancellationToken;
use zero_traits::{DialPolicy, IoObserver};
impl PendingTcpConnect {
    /// Reuses the production policy validation, source and interface binding.
    /// Transfers (never duplicates) the raw socket into a persistent owner before
    /// any connect future exists. A Unix Tokio runtime with I/O is required when
    /// polling `connect`, just as for other native Tokio socket operations.
    pub fn prepare(
        peer: SocketAddr,
        policy: &DialPolicy,
        selection: &EgressSelection,
        parent: CancellationToken,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<(Self, ControlledTcpControl), TcpConnectError> {
        let dial = prepare(peer, policy, selection).map_err(|error| TcpConnectError {
            stage: "validate_dial_policy",
            interface_bound: false,
            local_addr: None,
            error,
        })?;
        let socket = if dial.peer.is_ipv4() {
            TcpSocket::new_v4()
        } else {
            TcpSocket::new_v6()
        }
        .map_err(|error| TcpConnectError {
            stage: "create_socket",
            interface_bound: false,
            local_addr: None,
            error,
        })?;
        if let Some(source) = dial.source {
            socket.bind(source).map_err(|error| TcpConnectError {
                stage: "bind_source",
                interface_bound: false,
                local_addr: socket.local_addr().ok(),
                error,
            })?;
        }
        if let Some(interface) = &dial.interface {
            bind_tcp_to_interface(&socket, dial.peer, interface).map_err(|error| {
                TcpConnectError {
                    stage: "bind_interface",
                    interface_bound: false,
                    local_addr: socket.local_addr().ok(),
                    error,
                }
            })?;
        }
        // SAFETY: IntoRawFd relinquishes the one socket. The std wrapper takes
        // that exact ownership once; there is no clone, dup, or connect await.
        let original = unsafe { TcpStream::from_raw_fd(socket.into_raw_fd()) };
        Ok(Self::from_socket(
            original,
            dial.peer,
            dial.interface,
            parent,
            observer,
        ))
    }
}
