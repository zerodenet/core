mod failure;
mod hop;
use zero_core::Session;

use crate::inventory::{PreparedTcpRelayHop, PreparedTcpRelayPrefix};
use crate::protocol_registry::TcpExecutionServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

use super::TcpDispatchIntent;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;

pub(crate) async fn dispatch_tcp_relay(
    services: TcpExecutionServices,
    session: &Session,
    prefix: PreparedTcpRelayPrefix,
    final_hop: PreparedTcpRelayHop,
    intent: TcpDispatchIntent,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let upstream_endpoint = prefix.first.endpoint.clone();
    let mut relay_chain = vec![(
        prefix
            .first
            .tag
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        prefix.first.protocol.clone(),
    )];
    relay_chain.extend(
        prefix
            .relay_hops
            .iter()
            .map(|hop| (hop.tag.clone(), hop.protocol.clone())),
    );
    relay_chain.push((final_hop.tag.clone(), final_hop.protocol.clone()));
    let outbound_tag = final_hop.tag.clone();
    let carrier = lazy_relay_carrier(services.clone(), prefix, intent);
    let outbound = hop::execute(&services, &final_hop, carrier, session)
        .await
        .map_err(|error| failure::hop_failure(&final_hop, error, "relay_last"))?;

    outbound
        .with_relay_route(outbound_tag, upstream_endpoint, relay_chain)
        .map_err(|error| failure::hop_failure(&final_hop, error, "relay_last"))
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
    // Qualified donors execute through their exact standard originals even
    // when the receiver still has explicitly deferred process ownership.
    let native = prepared.standard_nodes();
    // Each nested carrier factory owns a strictly shorter prepared prefix.
    // This permits multiplexed transports in intermediate hops as well as the
    // final hop, without dialing any carrier outside the configured chain.
    Box::pin(async move {
        if let Some(nodes) = native.map_err(|mut source| {
            source.session = source.session.or_else(|| {
                services
                    .invocation
                    .as_ref()
                    .and_then(|invocation| invocation.identity.session())
            });
            TcpOutboundFailure {
                stage: "relay_first_hop",
                error: std::io::Error::other(source).into(),
                upstream_endpoint: prepared.first.endpoint.clone().map(Box::new),
                network: None,
            }
        })? {
            return crate::component::stream_bridge::relay::connect_prefix(
                &services,
                &prepared,
                nodes,
                intent.records_outbound_health(),
            )
            .await;
        }

        let session = relay_session(prepared.final_server.clone(), prepared.final_port);
        if !prepared.relay_hops.is_empty() {
            let current = prepared.relay_hops.pop().unwrap();
            prepared.final_tag = current.tag.clone();
            prepared.final_protocol = current.protocol.clone();
            prepared.final_server = current.server.clone();
            prepared.final_port = current.port;
            let carrier = lazy_relay_carrier(services.clone(), prepared, intent);
            return hop::execute(&services, &current, carrier, &session)
                .await
                .and_then(EstablishedTcpOutbound::into_compatibility_stream)
                .map_err(|error| failure::hop_failure(&current, error, "relay_hop"));
        }
        super::candidate::dispatch_prepared_tcp_candidate(
            services,
            &session,
            prepared.first,
            intent,
        )
        .await?
        .into_compatibility_stream()
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

fn lazy_relay_carrier(
    services: TcpExecutionServices,
    prefix: PreparedTcpRelayPrefix,
    intent: TcpDispatchIntent,
) -> LazyTcpRelayCarrier<'static> {
    LazyTcpRelayCarrier::new(Box::pin(async move {
        execute_relay_prefix_stream(services, prefix, intent)
            .await
            .map_err(|failure| failure.error)
    }))
}
