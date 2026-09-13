#[cfg(feature = "udp-runtime")]
mod datagram;
mod identity;
use zero_core::Session;

use crate::inventory::{PreparedTcpRelayChain, PreparedTcpRelayPrefix};
use crate::protocol_registry::TcpExecutionServices;
#[cfg(feature = "udp-runtime")]
use crate::transport::RelayCarrier;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

use super::TcpDispatchIntent;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use identity::{relay_chain_identity, relay_prefix_identity};

pub(crate) async fn dispatch_prepared_tcp_relay_chain(
    services: TcpExecutionServices,
    session: &Session,
    prepared: PreparedTcpRelayChain,
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
    let relay_identity = relay_chain_identity(&prepared, generation);
    let (prefix, final_hop) = split_relay_prefix(prepared);
    let carrier = lazy_relay_carrier(services.clone(), prefix, intent, relay_identity, generation);
    let stream = final_hop
        .operation
        .execute_lazy(services.upstream(), carrier, session)
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
    services: TcpExecutionServices,
    prepared: PreparedTcpRelayChain,
) -> LazyTcpRelayCarrier<'a> {
    let generation = egress_generation(&services);
    let (prefix, _final_hop) = split_relay_prefix(prepared);
    let relay_identity = relay_prefix_identity(&prefix, generation);
    lazy_relay_carrier(
        services,
        prefix,
        TcpDispatchIntent::Traffic,
        relay_identity,
        generation,
    )
}

pub(crate) fn prepare_lazy_tcp_relay_prefix<'a>(
    services: TcpExecutionServices,
    prefix: PreparedTcpRelayPrefix,
) -> LazyTcpRelayCarrier<'a> {
    let generation = egress_generation(&services);
    let identity = relay_prefix_identity(&prefix, generation);
    lazy_relay_carrier(
        services,
        prefix,
        TcpDispatchIntent::Traffic,
        identity,
        generation,
    )
}

#[cfg(feature = "udp-runtime")]
pub(crate) async fn dispatch_prepared_tcp_relay_carrier(
    services: TcpExecutionServices,
    prepared: PreparedTcpRelayChain,
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

async fn execute_relay_prefix(
    services: TcpExecutionServices,
    prepared: PreparedTcpRelayChain,
    intent: TcpDispatchIntent,
) -> Result<
    (
        crate::transport::TcpRelayStream,
        crate::inventory::PreparedTcpRelayHop,
    ),
    TcpOutboundFailure,
> {
    let (prefix, final_hop) = split_relay_prefix(prepared);
    let stream = execute_relay_prefix_stream(services, prefix, intent).await?;
    Ok((stream, final_hop))
}

fn split_relay_prefix(
    mut prepared: PreparedTcpRelayChain,
) -> (
    PreparedTcpRelayPrefix,
    crate::inventory::PreparedTcpRelayHop,
) {
    let final_hop = prepared
        .relay_hops
        .pop()
        .expect("relay chain must have at least one prepared hop");
    let prefix = PreparedTcpRelayPrefix {
        first: prepared.first,
        #[cfg(feature = "udp-runtime")]
        datagram_prefixes: prepared.datagram_prefixes,
        relay_hops: prepared.relay_hops,
        final_tag: final_hop.tag.clone(),
        final_protocol: final_hop.protocol.clone(),
        final_server: final_hop.server.clone(),
        final_port: final_hop.port,
    };
    (prefix, final_hop)
}

fn execute_relay_prefix_stream(
    services: TcpExecutionServices,
    mut prepared: PreparedTcpRelayPrefix,
    intent: TcpDispatchIntent,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<crate::transport::TcpRelayStream, TcpOutboundFailure>,
            > + Send,
    >,
> {
    // Each nested carrier factory owns a strictly shorter prepared prefix.
    // This permits multiplexed transports in intermediate hops as well as the
    // final hop, without dialing any carrier outside the configured chain.
    Box::pin(async move {
        let session = relay_session(prepared.final_server.clone(), prepared.final_port);
        if !prepared.relay_hops.is_empty() {
            let generation = egress_generation(&services);
            let identity = relay_chain_identity(
                &PreparedTcpRelayChain {
                    first: prepared.first.clone(),
                    #[cfg(feature = "udp-runtime")]
                    datagram_prefixes: prepared.datagram_prefixes.clone(),
                    relay_hops: prepared.relay_hops.clone(),
                },
                generation,
            );
            let current = prepared.relay_hops.pop().unwrap();
            #[cfg(feature = "udp-runtime")]
            prepared.datagram_prefixes.pop();
            prepared.final_tag = current.tag;
            prepared.final_protocol = current.protocol;
            prepared.final_server = current.server;
            prepared.final_port = current.port;
            let upstream = services.upstream();
            let carrier = lazy_relay_carrier(services, prepared, intent, identity, generation);
            return current
                .operation
                .execute_lazy(upstream, carrier, &session)
                .await
                .map_err(|error| TcpOutboundFailure {
                    stage: "relay_hop",
                    error,
                    upstream_endpoint: None,
                    network: None,
                });
        }
        super::candidate::dispatch_prepared_tcp_candidate(
            services,
            &session,
            prepared.first,
            intent,
        )
        .await?
        .into_relay_stream()
        .map_err(|error| TcpOutboundFailure {
            stage: "relay_first_hop",
            error,
            upstream_endpoint: None,
            network: None,
        })
    })
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

fn egress_generation(services: &TcpExecutionServices) -> u64 {
    services
        .upstream()
        .outbound_datagram_socket_factory()
        .egress_generation()
}

fn lazy_relay_carrier<'a>(
    services: TcpExecutionServices,
    prefix: PreparedTcpRelayPrefix,
    intent: TcpDispatchIntent,
    identity: String,
    generation: u64,
) -> LazyTcpRelayCarrier<'a> {
    let endpoint = (prefix.final_server.clone(), prefix.final_port);
    #[cfg(feature = "udp-runtime")]
    let datagrams = prefix
        .datagram_prefixes
        .last()
        .and_then(Option::as_ref)
        .map(|plan| datagram::factory(&services, plan.clone()));
    let connector = zero_transport::relay_connector::RelayStreamConnector::new(
        identity.clone(),
        generation,
        std::sync::Arc::new(move |server, port| {
            let mut prefix = prefix.clone();
            prefix.final_server = server;
            prefix.final_port = port;
            let services = services.clone();
            Box::pin(async move {
                execute_relay_prefix_stream(services, prefix, intent)
                    .await
                    .map_err(|failure| {
                        zero_transport::RuntimeError::Io(std::io::Error::other(failure.error))
                    })
            })
        }),
    );
    #[cfg(feature = "udp-runtime")]
    let connector = if let Some(factory) = datagrams {
        connector.with_datagrams(factory)
    } else {
        connector
    };
    let first = connector.clone();
    LazyTcpRelayCarrier::new(
        identity,
        generation,
        Box::pin(async move {
            first
                .connect(endpoint.0, endpoint.1)
                .await
                .map_err(Into::into)
        }),
    )
    .with_connector(connector)
}
