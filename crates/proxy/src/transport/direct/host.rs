use std::io;
use std::net::{IpAddr, SocketAddr};

use zero_dns::DnsSystem;
use zero_platform_tokio::{EgressInterfaceControl, TokioSocket};
use zero_transport::RuntimeError;

use super::{dial_tcp_candidates, log_dial_failure, socket_addr_from_ip, DirectConnector};
use crate::transport::failure::{attributed_error, is_local_network_error, TransportFailureOrigin};

impl DirectConnector {
    /// Keep DNS and local socket failures distinguishable from node failures.
    pub(crate) async fn connect_host(
        &self,
        host: &str,
        port: u16,
        resolver: &DnsSystem,
        egress: &EgressInterfaceControl,
    ) -> Result<TokioSocket, RuntimeError> {
        if port == 0 {
            return Err(zero_core::Error::Config("target port is required").into());
        }
        let candidates = match host.parse::<IpAddr>() {
            Ok(address) => vec![SocketAddr::new(address, port)],
            Err(_) => resolver
                .resolve_node(host)
                .await
                .map_err(|error| {
                    attributed_error(
                        TransportFailureOrigin::NameResolution,
                        "failed to resolve upstream target",
                        error,
                    )
                })?
                .into_iter()
                .map(|address| socket_addr_from_ip(address, port))
                .collect(),
        };
        if candidates.is_empty() {
            return Err(attributed_error(
                TransportFailureOrigin::NameResolution,
                "failed to resolve upstream target",
                io::Error::new(io::ErrorKind::NotFound, "target resolved to no addresses"),
            )
            .into());
        }
        dial_tcp_candidates(candidates, egress)
            .await
            .map(|success| success.socket)
            .map_err(|failure| {
                log_dial_failure("upstream", &failure);
                let origin = if matches!(
                    failure.stage,
                    "select_egress" | "bind_interface" | "create_socket" | "configure_socket"
                ) || is_local_network_error(&failure.error)
                {
                    TransportFailureOrigin::LocalNetwork
                } else {
                    TransportFailureOrigin::Upstream
                };
                attributed_error(origin, failure.stage, failure.error).into()
            })
    }
}
