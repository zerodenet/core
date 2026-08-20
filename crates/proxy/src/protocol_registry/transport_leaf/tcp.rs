use std::path::Path;

use zero_engine::EngineError;

use super::super::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
use crate::runtime::transport_leaf::{
    PreparedTransportLeaf, ProxyTransportLeaf, ProxyTransportTcpLeaf,
};
use crate::transport::TcpOutboundFailure;

pub(crate) fn claim_transport_tcp_leaf<'a, TLeaf, F, E>(
    upstream: Option<(&'a str, u16)>,
    prepare_leaf: F,
) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportTcpLeaf + Send + Sync + 'a,
    F: Fn(Option<&Path>) -> Result<TLeaf, E> + Send + Sync + 'a,
    E: std::fmt::Display,
{
    Box::new(ClaimedTransportTcpLeaf {
        upstream,
        prepare_leaf,
    })
}

struct ClaimedTransportTcpLeaf<'a, F> {
    upstream: Option<(&'a str, u16)>,
    prepare_leaf: F,
}

impl<'a, TLeaf, F, E> ClaimedTcpOutboundLeaf<'a> for ClaimedTransportTcpLeaf<'a, F>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportTcpLeaf + Send + Sync + 'a,
    F: Fn(Option<&Path>) -> Result<TLeaf, E> + Send + Sync + 'a,
    E: std::fmt::Display,
{
    fn prepare_tcp_connect(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, TcpOutboundFailure> {
        let prepared = (self.prepare_leaf)(source_dir)
            .map(PreparedTransportLeaf::new)
            .map_err(|error| {
                transport_tcp_connect_claim_prepare_failure::<TLeaf, _>(self.upstream, error)
            })?;
        Ok(prepare_transport_tcp_connect(prepared))
    }

    fn prepare_tcp_relay_hop(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpRelayOperation + 'a>, EngineError> {
        let prepared = (self.prepare_leaf)(source_dir)
            .map(PreparedTransportLeaf::new)
            .map_err(transport_tcp_relay_claim_prepare_error::<TLeaf, _>)?;
        Ok(prepare_transport_tcp_relay(prepared))
    }
}

pub(crate) fn prepare_transport_tcp_connect<'a, TLeaf>(
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Box<dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation + 'a>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportTcpLeaf + Send + Sync + 'a,
{
    Box::new(crate::runtime::tcp_dispatch::operation::TransportLeafTcpConnectOperation { prepared })
}

pub(crate) fn prepare_transport_tcp_relay<'a, TLeaf>(
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Box<dyn crate::runtime::tcp_dispatch::operation::PreparedTcpRelayOperation + 'a>
where
    TLeaf: ProxyTransportTcpLeaf + Send + Sync + 'a,
{
    Box::new(crate::runtime::tcp_dispatch::operation::TransportLeafTcpRelayOperation { prepared })
}

fn transport_tcp_connect_claim_prepare_failure<TLeaf, E>(
    upstream: Option<(&str, u16)>,
    error: E,
) -> TcpOutboundFailure
where
    TLeaf: ProxyTransportTcpLeaf,
    E: std::fmt::Display,
{
    TcpOutboundFailure {
        stage: TLeaf::TCP_CONNECT_STAGE,
        error: invalid_input(TLeaf::TCP_INVALID_CONNECT_CONFIG, error),
        upstream_endpoint: upstream.map(|(server, port)| Box::new((server.to_owned(), port))),
        network: None,
    }
}

fn transport_tcp_relay_claim_prepare_error<TLeaf, E>(error: E) -> EngineError
where
    TLeaf: ProxyTransportTcpLeaf,
    E: std::fmt::Display,
{
    invalid_input(TLeaf::TCP_INVALID_RELAY_CONFIG, error)
}

fn invalid_input(stage: &'static str, error: impl std::fmt::Display) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("{stage}: {error}"),
    ))
}
