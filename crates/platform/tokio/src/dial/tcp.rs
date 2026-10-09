use super::prepare;
use crate::{egress::bind_tcp_to_interface, EgressSelection, TcpConnectError, TokioSocket};
use std::{io, net::SocketAddr};
use tokio::net::TcpSocket;
use zero_traits::DialPolicy;
impl TokioSocket {
    pub async fn connect_addr_with_policy(
        peer: SocketAddr,
        policy: &DialPolicy,
        selection: &EgressSelection,
    ) -> io::Result<Self> {
        Self::connect_addr_with_policy_observed(peer, policy, selection)
            .await
            .map_err(TcpConnectError::into_inner)
    }
    pub async fn connect_addr_with_policy_observed(
        peer: SocketAddr,
        policy: &DialPolicy,
        selection: &EgressSelection,
    ) -> Result<Self, TcpConnectError> {
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
        let interface_bound = dial.interface.is_some();
        let local_addr = socket.local_addr().ok();
        let stream = socket
            .connect(dial.peer)
            .await
            .map_err(|error| TcpConnectError {
                stage: "connect_socket",
                interface_bound,
                local_addr,
                error,
            })?;
        let local_addr = stream.local_addr().ok();
        stream.set_nodelay(true).map_err(|error| TcpConnectError {
            stage: "configure_socket",
            interface_bound,
            local_addr,
            error,
        })?;
        Ok(Self {
            inner: stream,
            effective_addresses: None,
            egress_interface: dial.interface,
            observer: None,
        })
    }
}
