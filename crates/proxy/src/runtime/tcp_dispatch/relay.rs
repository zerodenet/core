use zero_core::Session;

use crate::inventory::PreparedTcpRelayChain;
use crate::protocol_registry::TcpRuntimeServices;
#[cfg(feature = "udp-runtime")]
use crate::transport::RelayCarrier;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

use super::TcpDispatchIntent;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;

pub(crate) async fn dispatch_prepared_tcp_relay_chain(
    services: TcpRuntimeServices,
    session: &Session,
    prepared: PreparedTcpRelayChain<'_>,
    intent: TcpDispatchIntent,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let upstream_endpoint = prepared.first.endpoint.clone();
    let mut relay_chain = vec![(
        prepared
            .first
            .tag
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        prepared.first.protocol.clone(),
    )];
    relay_chain.extend(
        prepared
            .relay_hops
            .iter()
            .map(|hop| (hop.tag.clone(), hop.protocol.clone())),
    );
    let outbound_tag = relay_chain
        .last()
        .map(|(tag, _)| tag.clone())
        .unwrap_or_else(|| "relay".to_owned());
    let generation = egress_generation(&services);
    let relay_identity = relay_identity(&prepared, generation);
    let (prefix, final_hop) = split_relay_prefix(prepared);
    let carrier = lazy_relay_carrier(services.clone(), prefix, intent, relay_identity, generation);
    let stream = final_hop
        .operation
        .execute_lazy(carrier, session)
        .await
        .map_err(|error| TcpOutboundFailure {
            stage: "relay_last",
            error,
            upstream_endpoint: None,
            network: None,
        })?;

    Ok(EstablishedTcpOutbound::relay(
        outbound_tag,
        upstream_endpoint,
        relay_chain,
        stream,
    ))
}

pub(crate) fn prepare_lazy_tcp_relay_carrier<'a>(
    services: TcpRuntimeServices,
    prepared: PreparedTcpRelayChain<'a>,
) -> LazyTcpRelayCarrier<'a> {
    let generation = egress_generation(&services);
    let relay_identity = relay_identity(&prepared, generation);
    let (prefix, _final_hop) = split_relay_prefix(prepared);
    lazy_relay_carrier(
        services,
        prefix,
        TcpDispatchIntent::Traffic,
        relay_identity,
        generation,
    )
}

pub(crate) async fn dispatch_prepared_tcp_relay_hop(
    stream: crate::transport::TcpRelayStream,
    session: &Session,
    prepared: crate::inventory::PreparedTcpRelayHop<'_>,
) -> Result<crate::transport::TcpRelayStream, zero_engine::EngineError> {
    prepared.operation.execute(stream, session).await
}

#[cfg(feature = "udp-runtime")]
pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
    services: TcpRuntimeServices,
    prepared: PreparedTcpRelayChain<'_>,
) -> Result<RelayCarrier, TcpOutboundFailure> {
    let (stream, final_hop) =
        execute_relay_prefix(services, prepared, TcpDispatchIntent::Traffic).await?;
    let (server, port) = final_hop.upstream();
    Ok(RelayCarrier {
        stream,
        server,
        port,
    })
}

async fn execute_relay_prefix<'a>(
    services: TcpRuntimeServices,
    prepared: PreparedTcpRelayChain<'a>,
    intent: TcpDispatchIntent,
) -> Result<
    (
        crate::transport::TcpRelayStream,
        crate::inventory::PreparedTcpRelayHop<'a>,
    ),
    TcpOutboundFailure,
> {
    let (prefix, final_hop) = split_relay_prefix(prepared);
    let stream = execute_relay_prefix_stream(services, prefix, intent).await?;
    Ok((stream, final_hop))
}

struct PreparedTcpRelayPrefix<'a> {
    first: crate::inventory::PreparedTcpCandidate<'a>,
    relay_hops: Vec<crate::inventory::PreparedTcpRelayHop<'a>>,
    final_server: String,
    final_port: u16,
}

fn split_relay_prefix(
    mut prepared: PreparedTcpRelayChain<'_>,
) -> (
    PreparedTcpRelayPrefix<'_>,
    crate::inventory::PreparedTcpRelayHop<'_>,
) {
    let final_hop = prepared
        .relay_hops
        .pop()
        .expect("relay chain must have at least one prepared hop");
    let prefix = PreparedTcpRelayPrefix {
        first: prepared.first,
        relay_hops: prepared.relay_hops,
        final_server: final_hop.server.clone(),
        final_port: final_hop.port,
    };
    (prefix, final_hop)
}

async fn execute_relay_prefix_stream(
    services: TcpRuntimeServices,
    prepared: PreparedTcpRelayPrefix<'_>,
    intent: TcpDispatchIntent,
) -> Result<crate::transport::TcpRelayStream, TcpOutboundFailure> {
    let first_target = prepared
        .relay_hops
        .first()
        .map(|hop| (hop.server.clone(), hop.port))
        .unwrap_or_else(|| (prepared.final_server.clone(), prepared.final_port));
    let mut session_for_next = relay_session(first_target.0, first_target.1);
    let outbound = super::candidate::dispatch_prepared_tcp_candidate(
        services.clone(),
        &session_for_next,
        prepared.first,
        intent,
    )
    .await?;
    let mut stream = outbound
        .into_relay_stream()
        .map_err(|error| TcpOutboundFailure {
            stage: "relay_first_hop",
            error,
            upstream_endpoint: None,
            network: None,
        })?;

    let mut relay_hops = prepared.relay_hops.into_iter().peekable();
    while let Some(current_prepared) = relay_hops.next() {
        let (server, port) = relay_hops
            .peek()
            .map(|hop| (hop.server.clone(), hop.port))
            .unwrap_or_else(|| (prepared.final_server.clone(), prepared.final_port));
        session_for_next = relay_session(server, port);
        stream = dispatch_prepared_tcp_relay_hop(stream, &session_for_next, current_prepared)
            .await
            .map_err(|error| TcpOutboundFailure {
                stage: "relay_hop",
                error,
                upstream_endpoint: None,
                network: None,
            })?;
    }
    Ok(stream)
}

fn relay_session(server: String, port: u16) -> Session {
    Session::new(
        0,
        zero_core::Address::Domain(server),
        port,
        zero_core::Network::Tcp,
        zero_core::ProtocolType::UNKNOWN,
    )
}

fn relay_identity(prepared: &PreparedTcpRelayChain<'_>, generation: u64) -> String {
    let mut identity = format!("egress={generation}");
    if let Some(tag) = &prepared.first.tag {
        identity.push_str(&format!("|{}:{}", tag.len(), tag));
    }
    identity.push_str(&format!(
        "|{}:{}",
        prepared.first.protocol.len(),
        prepared.first.protocol
    ));
    if let Some((server, port)) = &prepared.first.endpoint {
        identity.push_str(&format!("|{}:{server}:{port}", server.len()));
    }
    for hop in &prepared.relay_hops {
        identity.push_str(&format!(
            "|{}:{}|{}:{}|{}:{}:{}",
            hop.tag.len(),
            hop.tag,
            hop.protocol.len(),
            hop.protocol,
            hop.server.len(),
            hop.server,
            hop.port
        ));
    }
    identity
}

fn egress_generation(services: &TcpRuntimeServices) -> u64 {
    services
        .upstream()
        .outbound_datagram_socket_factory()
        .egress_generation()
}

fn lazy_relay_carrier<'a>(
    services: TcpRuntimeServices,
    prefix: PreparedTcpRelayPrefix<'a>,
    intent: TcpDispatchIntent,
    identity: String,
    generation: u64,
) -> LazyTcpRelayCarrier<'a> {
    LazyTcpRelayCarrier::new(
        identity,
        generation,
        Box::pin(async move {
            execute_relay_prefix_stream(services, prefix, intent)
                .await
                .map_err(|failure| failure.error)
        }),
    )
}
