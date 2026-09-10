use std::future::Future;
use std::sync::Arc;

use zero_platform_tokio::{TcpRelayStream, TokioSocket};
use zero_transport::RuntimeError;

#[derive(Debug, Clone)]
pub struct MieruManagedUdpFlowResume {
    server: String,
    port: u16,
    protocol: crate::udp::MieruUdpFlowResume,
    leaf: Option<super::MieruTransportLeaf>,
}

#[derive(Debug, Clone, Copy)]
pub struct MieruManagedUdpFlowConfig<'a> {
    server: &'a str,
    port: u16,
    protocol: crate::udp::MieruUdpFlowConfig<'a>,
}

pub type MieruManagedUdpConnectorFlow = crate::udp::MieruUdpConnectorFlow;

impl<'a> MieruManagedUdpFlowConfig<'a> {
    pub fn new(server: &'a str, port: u16, username: &'a str, password: &'a str) -> Self {
        Self {
            server,
            port,
            protocol: crate::udp::MieruUdpFlowConfig::new(username, password),
        }
    }

    pub fn flow_resume(&self, relay_chain: bool) -> MieruManagedUdpFlowResume {
        MieruManagedUdpFlowResume::new(
            self.server,
            self.port,
            self.protocol.flow_resume(relay_chain),
        )
    }
}

impl MieruManagedUdpFlowResume {
    pub(super) fn from_leaf(leaf: super::MieruTransportLeaf, relay: bool) -> Self {
        Self {
            server: leaf.server.clone(),
            port: leaf.port,
            protocol: crate::udp::MieruUdpFlowResume::new(&leaf.username, &leaf.password, relay),
            leaf: Some(leaf),
        }
    }

    fn new(server: &str, port: u16, protocol: crate::udp::MieruUdpFlowResume) -> Self {
        Self {
            server: server.to_owned(),
            port,
            protocol,
            leaf: None,
        }
    }

    pub fn connector_flow(&self, session_id: u64) -> MieruManagedUdpConnectorFlow {
        let flow = crate::udp::connector_flow_from_resume(
            &self.protocol,
            &self.server,
            self.port,
            session_id,
        );
        if let Some(leaf) = &self.leaf {
            flow.with_namespace(&leaf.cache_identity())
        } else {
            flow
        }
    }

    pub async fn open_direct_connection<OpenSocket, OpenSocketFut>(
        &self,
        open_socket: OpenSocket,
        factory: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<crate::udp::MieruUdpFlowConnection, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFut + Send + Sync,
        OpenSocketFut: Future<Output = Result<TokioSocket, RuntimeError>> + Send,
    {
        if let Some(leaf) = &self.leaf {
            let stream = leaf.open_pooled(open_socket, factory).await?;
            return crate::outbound::logical_udp::establish(stream)
                .await
                .map_err(|e| RuntimeError::Io(std::io::Error::other(e.to_string())));
        }
        let stream = open_socket(&self.server, self.port).await?;
        crate::udp::establish_udp_flow_with_resume(stream, &self.protocol)
            .await
            .map_err(|error| {
                RuntimeError::Io(std::io::Error::other(format!(
                    "mieru udp associate: {error}"
                )))
            })
    }

    pub async fn open_relay_connection(
        &self,
        stream: TcpRelayStream,
    ) -> Result<crate::udp::MieruUdpFlowConnection, RuntimeError> {
        if self.leaf.as_ref().is_some_and(|leaf| leaf.udp) {
            return Err(RuntimeError::Io(std::io::Error::other(
                "mieru UDP underlay requires a datagram carrier",
            )));
        }
        if let Some(leaf) = &self.leaf {
            let connection = crate::client::ClientConnection::tcp_with_options(
                stream,
                &leaf.username,
                &leaf.password,
                &leaf.options,
            )
            .await
            .map_err(RuntimeError::Io)?;
            let stream = connection.open().await.map_err(RuntimeError::Io)?;
            return crate::outbound::logical_udp::establish(stream)
                .await
                .map_err(|e| RuntimeError::Io(std::io::Error::other(e.to_string())));
        }
        crate::udp::establish_udp_flow_with_resume(stream, &self.protocol)
            .await
            .map_err(|error| {
                RuntimeError::Io(std::io::Error::other(format!(
                    "mieru udp associate: {error}"
                )))
            })
    }

    pub async fn open_pooled_relay_connection<OpenCarrier, OpenCarrierFut, E>(
        &self,
        generation: u64,
        relay_identity: &str,
        open_carrier: OpenCarrier,
    ) -> Result<crate::udp::MieruUdpFlowConnection, RuntimeError>
    where
        OpenCarrier: FnMut() -> OpenCarrierFut,
        OpenCarrierFut: Future<Output = Result<TcpRelayStream, E>> + Send,
        E: Into<RuntimeError>,
    {
        let leaf = self.leaf.as_ref().ok_or_else(|| {
            RuntimeError::Io(std::io::Error::other(
                "mieru pooled relay requires a transport leaf",
            ))
        })?;
        if leaf.udp {
            return Err(RuntimeError::Io(std::io::Error::other(
                "mieru TCP relay pool requires transport=tcp",
            )));
        }
        let stream = leaf
            .open_pooled_relay_tcp(generation, relay_identity, open_carrier)
            .await?;
        crate::outbound::logical_udp::establish(stream)
            .await
            .map_err(|error| RuntimeError::Io(std::io::Error::other(error.to_string())))
    }

    pub async fn open_datagram_relay_connection<OpenCarrier, OpenCarrierFut, E>(
        &self,
        generation: u64,
        relay_identity: &str,
        open_carrier: OpenCarrier,
    ) -> Result<crate::udp::MieruUdpFlowConnection, RuntimeError>
    where
        OpenCarrier: FnMut() -> OpenCarrierFut,
        OpenCarrierFut:
            Future<Output = Result<Arc<dyn crate::client::ClientDatagramCarrier>, E>> + Send,
        E: Into<RuntimeError>,
    {
        let leaf = self.leaf.as_ref().ok_or_else(|| {
            RuntimeError::Io(std::io::Error::other(
                "mieru datagram relay requires a transport leaf",
            ))
        })?;
        if !leaf.udp {
            return Err(RuntimeError::Io(std::io::Error::other(
                "mieru datagram relay requires transport=udp",
            )));
        }
        let stream = leaf
            .open_pooled_relay_datagram(generation, relay_identity, open_carrier)
            .await?;
        crate::outbound::logical_udp::establish(stream)
            .await
            .map_err(|error| RuntimeError::Io(std::io::Error::other(error.to_string())))
    }
}
