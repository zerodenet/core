use super::request::ManagedStreamPacketStartBridge;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::{
    managed::stream_manager::ManagedStreamFlowConnector,
    outbound::UdpFlowOutbound,
    registered::state::RegisteredUdpConnection,
    result::{FlowFailure, FlowStartResult},
    state::UdpFlowStartContext,
};

async fn start_managed_stream_packet<T: ManagedStreamFlowConnector>(
    context: &mut UdpFlowStartContext<'_>,
    request: ManagedStreamPacketStartBridge<'_, T>,
) -> Result<FlowStartResult, FlowFailure> {
    let endpoint = OutboundEndpoint {
        server: request.server.to_owned(),
        port: request.port,
    };
    let failure = |stage, error| FlowFailure {
        stage,
        error,
        upstream: Some(endpoint.upstream()),
    };
    let (key, requires_relay) = request
        .resume
        .connector_flow(endpoint.clone(), request.session.id)
        .into_parts();
    let stage = if requires_relay {
        T::RELAY_SEND_STAGE
    } else {
        T::ESTABLISH_STAGE
    };
    // The cold constructor is selected by Rust. No erased recipe survives
    // registration, and ordinary packets never search a handler/type registry.
    let connection = if request.relay_chain {
        let carrier = request
            .carrier
            .expect("prepared relay requires its actual carrier");
        let connection = request
            .resume
            .establish_relay(
                carrier,
                request.tls_server_name,
                request.services,
                request.session,
                endpoint.clone(),
            )
            .await
            .map_err(|error| failure(T::RELAY_ESTABLISH_STAGE, error))?;
        connection.spawn_response_bridge(&mut context.state.chain_tasks, request.session.id);
        let sent = connection
            .send(
                &request.session.target,
                request.session.port,
                request.payload,
            )
            .await
            .map_err(|error| failure(T::RELAY_SEND_STAGE, error))?;
        context
            .state
            .registered
            .tuple_connections
            .insert::<T>(key, connection.clone());
        return finish(
            context,
            request.tag,
            request.session.id,
            &endpoint,
            true,
            connection,
            T::RELAY_SEND_STAGE,
            sent,
        );
    } else if let Some(connection) = context.state.registered.tuple_connections.get::<T>(&key) {
        connection
    } else {
        if requires_relay {
            return Err(failure(
                T::RELAY_UPSTREAM_STAGE,
                zero_engine::EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "relay upstream is not established",
                )),
            ));
        }
        let connection = request
            .resume
            .establish_direct(
                request
                    .services
                    .expect("direct managed stream operation requires runtime services"),
                request.session,
                endpoint.clone(),
            )
            .await
            .map_err(|error| failure(T::ESTABLISH_STAGE, error))?;
        // Keep the previous establish/send/insert order: an unsuccessful first
        // write must not publish a ready cached connection.
        connection.spawn_response_bridge(&mut context.state.chain_tasks, request.session.id);
        let sent = connection
            .send(
                &request.session.target,
                request.session.port,
                request.payload,
            )
            .await
            .map_err(|error| failure(stage, error))?;
        context
            .state
            .registered
            .tuple_connections
            .insert::<T>(key, connection.clone());
        return finish(
            context,
            request.tag,
            request.session.id,
            &endpoint,
            false,
            connection,
            stage,
            sent,
        );
    };
    connection.spawn_response_bridge(&mut context.state.chain_tasks, request.session.id);
    let sent = connection
        .send(
            &request.session.target,
            request.session.port,
            request.payload,
        )
        .await
        .map_err(|error| failure(stage, error))?;
    finish(
        context,
        request.tag,
        request.session.id,
        &endpoint,
        false,
        connection,
        stage,
        sent,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "the existing managed-flow commit carries actual session, original control and delivered byte facts"
)]
fn finish(
    context: &mut UdpFlowStartContext<'_>,
    tag: &str,
    session_id: u64,
    endpoint: &OutboundEndpoint,
    relay: bool,
    connection: crate::runtime::udp_flow::managed::connection::SharedManagedUdpConnection,
    stage: &'static str,
    sent: usize,
) -> Result<FlowStartResult, FlowFailure> {
    let managed = context.state.registered.register_connection(
        Some(session_id),
        RegisteredUdpConnection::Tuple { connection, stage },
    );
    let tag = tag.to_owned();
    let server = endpoint.server.clone();
    let port = endpoint.port;
    let outbound = if relay {
        UdpFlowOutbound::Relay {
            tag,
            server,
            port,
            managed,
        }
    } else {
        UdpFlowOutbound::StreamPacket {
            tag,
            server,
            port,
            managed,
            remote: None,
        }
    };
    Ok(FlowStartResult::Flow {
        outbound: Box::new(outbound),
        tx_bytes: sent as u64,
    })
}

pub(crate) async fn start_direct_managed_stream_packet<T: ManagedStreamFlowConnector>(
    context: &mut UdpFlowStartContext<'_>,
    request: ManagedStreamPacketStartBridge<'_, T>,
) -> Result<FlowStartResult, FlowFailure> {
    start_managed_stream_packet(context, request).await
}
pub(crate) async fn start_relay_managed_stream_packet<T: ManagedStreamFlowConnector>(
    context: &mut UdpFlowStartContext<'_>,
    request: ManagedStreamPacketStartBridge<'_, T>,
) -> Result<FlowStartResult, FlowFailure> {
    start_managed_stream_packet(context, request).await
}
