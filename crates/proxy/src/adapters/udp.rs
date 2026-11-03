//! Source-materialized transport results for the existing owned executors.
use crate::protocol_registry::{UdpFlowResult, UdpRelayResult};
use crate::runtime::transport_leaf::{
    PreparedTransportLeaf, ProxyRelayTwoStreamTransportLeaf, ProxyTransportUdpLeaf,
};
use crate::runtime::udp_dispatch::{operation::transport, FlowFailure};

pub(super) fn transport_operations<'a, T, E>(
    upstream: Option<(&str, u16)>,
    direct_compat: bool,
    leaf: Result<&T, &E>,
) -> (UdpFlowResult<'a>, UdpRelayResult<'a>)
where
    T: ProxyTransportUdpLeaf + Send + Clone + 'static,
    E: std::fmt::Display,
{
    let direct = if direct_compat {
        leaf.map(|leaf| {
            transport::prepare_transport_udp_direct(PreparedTransportLeaf::new(leaf.clone()))
        })
        .map_err(|error| failure::<T>(upstream, error, T::UDP_DIRECT_STAGE))
    } else {
        Err(FlowFailure {
            stage: "udp_standard_role",
            error: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "ordinary UDP uses the standard Datagram role",
            )
            .into(),
            upstream: upstream.map(|(host, port)| (host.into(), port)),
        })
    };
    let relay = leaf
        .map(|leaf| {
            Box::new(transport::PreparedTransportUdpRelay {
                prepared: PreparedTransportLeaf::new(leaf.clone()),
            })
                as Box<dyn crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation<'a> + 'a>
        })
        .map_err(|error| failure::<T>(upstream, error, T::UDP_RELAY_FINAL_STAGE));
    (direct, relay)
}

pub(super) fn two_stream_transport_operations<'a, T, E>(
    upstream: Option<(&str, u16)>,
    leaf: Result<&T, &E>,
) -> (UdpFlowResult<'a>, UdpRelayResult<'a>)
where
    T: ProxyRelayTwoStreamTransportLeaf + Clone + 'static,
    E: std::fmt::Display,
{
    let direct = leaf
        .map(|leaf| {
            transport::prepare_transport_udp_direct(PreparedTransportLeaf::new(leaf.clone()))
        })
        .map_err(|error| failure::<T>(upstream, error, T::UDP_DIRECT_STAGE));
    let relay = leaf
        .map(|leaf| {
            let prepared = PreparedTransportLeaf::new(leaf.clone());
            let two_stream =
                !prepared.udp_relay_uses_connector() && prepared.udp_relay_needs_two_streams();
            Box::new(transport::PreparedTwoStreamTransportUdpRelay {
                prepared,
                two_stream,
            })
                as Box<dyn crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation<'a> + 'a>
        })
        .map_err(|error| failure::<T>(upstream, error, T::UDP_RELAY_FINAL_STAGE));
    (direct, relay)
}

fn failure<T: ProxyTransportUdpLeaf>(
    upstream: Option<(&str, u16)>,
    error: impl std::fmt::Display,
    stage: &'static str,
) -> FlowFailure {
    FlowFailure {
        stage,
        error: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{}: {error}", T::UDP_INVALID_CONFIG),
        )
        .into(),
        upstream: upstream.map(|(server, port)| (server.into(), port)),
    }
}
