use std::path::Path;

use super::super::ClaimedUdpFlowLeaf;
use crate::runtime::transport_leaf::{
    PreparedTransportLeaf, ProxyRelayTwoStreamTransportLeaf, ProxyTransportLeaf,
    ProxyTransportUdpLeaf,
};
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::transport::RelayCarrier;
use zero_engine::EngineError;

pub(crate) fn claim_transport_udp_leaf<'a, TLeaf, F, E>(
    upstream: Option<(&'a str, u16)>,
    prepare_leaf: F,
) -> Box<dyn ClaimedUdpFlowLeaf<'a> + 'a>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send + 'a,
    F: Fn(Option<&Path>) -> Result<TLeaf, E> + Send + Sync + 'a,
    E: std::fmt::Display,
{
    Box::new(ClaimedTransportUdpLeaf {
        upstream,
        prepare_leaf,
    })
}

pub(crate) fn claim_relay_two_stream_transport_udp_leaf<'a, TLeaf, F, E>(
    upstream: Option<(&'a str, u16)>,
    prepare_leaf: F,
) -> Box<dyn ClaimedUdpFlowLeaf<'a> + 'a>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync + 'a,
    F: Fn(Option<&Path>) -> Result<TLeaf, E> + Send + Sync + 'a,
    E: std::fmt::Display,
{
    Box::new(ClaimedRelayTwoStreamTransportUdpLeaf {
        upstream,
        prepare_leaf,
    })
}

struct PreparedTransportUdpRelay<TLeaf> {
    prepared: PreparedTransportLeaf<TLeaf>,
}

struct PreparedTwoStreamTransportUdpRelay<TLeaf> {
    prepared: PreparedTransportLeaf<TLeaf>,
    two_stream: bool,
}

struct ClaimedTransportUdpLeaf<'a, F> {
    upstream: Option<(&'a str, u16)>,
    prepare_leaf: F,
}

struct ClaimedRelayTwoStreamTransportUdpLeaf<'a, F> {
    upstream: Option<(&'a str, u16)>,
    prepare_leaf: F,
}

pub(crate) fn transport_udp_relay_needs_two_streams<TLeaf>(
    prepared: &PreparedTransportLeaf<TLeaf>,
) -> bool
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf,
{
    !prepared.udp_relay_uses_connector() && prepared.udp_relay_needs_two_streams()
}

impl<'a, TLeaf, F, E> ClaimedUdpFlowLeaf<'a> for ClaimedTransportUdpLeaf<'a, F>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send + 'a,
    F: Fn(Option<&Path>) -> Result<TLeaf, E> + Send + Sync + 'a,
    E: std::fmt::Display,
{
    fn prepare_udp_flow(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let prepared = (self.prepare_leaf)(source_dir)
            .map(PreparedTransportLeaf::new)
            .map_err(|error| {
                transport_udp_direct_claim_prepare_failure::<TLeaf, _>(self.upstream, error)
            })?;
        Ok(
            crate::runtime::udp_dispatch::operation::transport::prepare_transport_udp_direct(
                prepared,
            ),
        )
    }

    fn prepare_udp_relay(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedUdpRelayOperation<'a> + 'a>, FlowFailure> {
        let prepared = (self.prepare_leaf)(source_dir)
            .map(PreparedTransportLeaf::new)
            .map_err(|error| {
                transport_udp_relay_final_claim_prepare_failure::<TLeaf, _>(self.upstream, error)
            })?;
        Ok(Box::new(PreparedTransportUdpRelay { prepared }))
    }
}

impl<'a, TLeaf, F, E> ClaimedUdpFlowLeaf<'a> for ClaimedRelayTwoStreamTransportUdpLeaf<'a, F>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync + 'a,
    F: Fn(Option<&Path>) -> Result<TLeaf, E> + Send + Sync + 'a,
    E: std::fmt::Display,
{
    fn prepare_udp_flow(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let prepared = (self.prepare_leaf)(source_dir)
            .map(PreparedTransportLeaf::new)
            .map_err(|error| {
                transport_udp_direct_claim_prepare_failure::<TLeaf, _>(self.upstream, error)
            })?;
        Ok(
            crate::runtime::udp_dispatch::operation::transport::prepare_transport_udp_direct(
                prepared,
            ),
        )
    }

    fn prepare_udp_relay(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedUdpRelayOperation<'a> + 'a>, FlowFailure> {
        let prepared = (self.prepare_leaf)(source_dir)
            .map(PreparedTransportLeaf::new)
            .map_err(|error| {
                transport_udp_relay_final_claim_prepare_failure::<TLeaf, _>(self.upstream, error)
            })?;
        let two_stream = transport_udp_relay_needs_two_streams(&prepared);
        Ok(Box::new(PreparedTwoStreamTransportUdpRelay {
            prepared,
            two_stream,
        }))
    }
}

impl<'a, TLeaf> PreparedUdpRelayOperation<'a> for PreparedTransportUdpRelay<TLeaf>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send + 'a,
{
    fn bind_final_hop(
        self: Box<Self>,
        carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(crate::runtime::udp_dispatch::operation::transport::prepare_transport_udp_relay_final_hop(
            carrier,
            self.prepared,
        ))
    }
}

impl<'a, TLeaf> PreparedUdpRelayOperation<'a> for PreparedTwoStreamTransportUdpRelay<TLeaf>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync + 'a,
{
    fn uses_lazy_stream_carrier(&self) -> bool {
        self.prepared.udp_relay_uses_connector()
    }
    fn bind_lazy_stream_carrier(
        self: Box<Self>,
        carrier: crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let connector = carrier.connector().ok_or_else(|| FlowFailure {
            stage: "udp_relay_connector",
            error: EngineError::Io(std::io::Error::other(
                "relay prefix has no reusable connector",
            )),
            upstream: None,
        })?;
        Ok(crate::runtime::udp_dispatch::operation::transport::prepare_transport_udp_relay_connector(connector, self.prepared))
    }
    fn needs_two_streams(&self) -> bool {
        self.two_stream
    }

    fn bind_final_hop(
        self: Box<Self>,
        carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(crate::runtime::udp_dispatch::operation::transport::prepare_transport_udp_relay_final_hop(
            carrier,
            self.prepared,
        ))
    }

    fn bind_two_stream(
        self: Box<Self>,
        post_carrier: RelayCarrier,
        get_carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Ok(crate::runtime::udp_dispatch::operation::transport::prepare_transport_udp_relay_two_stream(
            post_carrier,
            get_carrier,
            self.prepared,
        ))
    }
}

fn transport_udp_direct_claim_prepare_failure<TLeaf, E>(
    upstream: Option<(&str, u16)>,
    error: E,
) -> FlowFailure
where
    TLeaf: ProxyTransportUdpLeaf,
    E: std::fmt::Display,
{
    transport_udp_claim_prepare_failure::<TLeaf, E>(upstream, error, TLeaf::UDP_DIRECT_STAGE)
}

fn transport_udp_relay_final_claim_prepare_failure<TLeaf, E>(
    upstream: Option<(&str, u16)>,
    error: E,
) -> FlowFailure
where
    TLeaf: ProxyTransportUdpLeaf,
    E: std::fmt::Display,
{
    transport_udp_claim_prepare_failure::<TLeaf, E>(upstream, error, TLeaf::UDP_RELAY_FINAL_STAGE)
}

fn transport_udp_claim_prepare_failure<TLeaf, E>(
    upstream: Option<(&str, u16)>,
    error: E,
    stage: &'static str,
) -> FlowFailure
where
    TLeaf: ProxyTransportUdpLeaf,
    E: std::fmt::Display,
{
    FlowFailure {
        stage,
        error: invalid_input(TLeaf::UDP_INVALID_CONFIG, error),
        upstream: upstream.map(|(server, port)| (server.to_owned(), port)),
    }
}

fn invalid_input(stage: &'static str, error: impl std::fmt::Display) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{stage}: {error}"),
    ))
}
