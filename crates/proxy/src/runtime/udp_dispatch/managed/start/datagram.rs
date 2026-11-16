use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::{
    path::OutboundEndpoint,
    udp_dispatch::{
        operation::ManagedDatagramStartPlan, FlowFailure, FlowStartResult, UdpDispatch,
    },
    udp_flow::{
        managed::{
            datagram::managed_datagram_connection_from_flow,
            datagram_manager::ManagedDatagramSocketResumeConnector,
        },
        outbound::UdpFlowOutbound,
        registered::state::RegisteredUdpConnection,
    },
};
impl UdpDispatch {
    pub(crate) async fn start_transport_managed_socket<T: ManagedDatagramSocketResumeConnector>(
        &mut self,
        services: Option<UdpRuntimeServices>,
        session: &zero_core::Session,
        payload: &[u8],
        plan: ManagedDatagramStartPlan<T>,
    ) -> Result<FlowStartResult, FlowFailure> {
        let endpoint = OutboundEndpoint {
            server: plan.server.clone(),
            port: plan.port,
        };
        let failure = |stage, error| FlowFailure {
            stage,
            error,
            upstream: Some(endpoint.upstream()),
        };
        let key = plan
            .resume
            .connector_flow(endpoint.clone())
            .into_cache_key();
        let connection = if let Some(connection) = self
            .flow_state
            .registered
            .datagram_connections
            .get::<T>(&key)
        {
            connection
        } else {
            let network = services.as_ref().ok_or_else(|| {
                failure(
                    T::ESTABLISH_STAGE,
                    zero_engine::EngineError::Io(std::io::Error::other(T::PROXY_CONTEXT_MESSAGE)),
                )
            })?;
            let address = network
                .resolve_node_address(
                    &endpoint.address(),
                    endpoint.port,
                    T::RESOLVE_UPSTREAM_MESSAGE,
                )
                .await
                .map_err(|error| failure(T::ESTABLISH_STAGE, error))?;
            let flow = plan
                .resume
                .open_connection(
                    address,
                    network.network().outbound_datagram_socket_factory(),
                )
                .await
                .map_err(|error| failure(T::ESTABLISH_STAGE, error))?;
            let connection = managed_datagram_connection_from_flow(flow);
            self.flow_state
                .registered
                .datagram_connections
                .insert::<T>(key, connection.clone());
            connection
        };
        let sent = connection
            .send_datagram(
                &mut self.flow_state.chain_tasks,
                services,
                session.id,
                &session.target,
                session.port,
                payload,
            )
            .await
            .map_err(|error| failure(T::SEND_STAGE, error))?;
        let managed = self.flow_state.registered.register_connection(
            Some(session.id),
            RegisteredUdpConnection::Datagram {
                connection,
                stage: T::SEND_STAGE,
            },
        );
        Ok(FlowStartResult::Flow {
            outbound: Box::new(UdpFlowOutbound::Datagram {
                tag: plan.tag,
                server: plan.server,
                port: plan.port,
                managed,
            }),
            tx_bytes: sent as u64,
        })
    }
}
