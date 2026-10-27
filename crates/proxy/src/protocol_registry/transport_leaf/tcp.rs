use std::path::Path;

use zero_engine::EngineError;

use super::super::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation, TransportLeafTcpOperation,
};
use crate::runtime::transport_leaf::ProxyTransportTcpLeaf;
use crate::transport::TcpOutboundFailure;

pub(crate) fn claim_transport_tcp_leaf<'a, TLeaf, E>(
    upstream: Option<(&'a str, u16)>,
    leaf: Result<TLeaf, E>,
) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a>
where
    TLeaf: ProxyTransportTcpLeaf + Clone + 'static,
    E: std::fmt::Display + Send + Sync + 'a,
{
    Box::new(TransportLeafTcpOperation {
        leaf: (upstream, leaf),
    })
}

// Cold projection materializes once with the actual configuration source. An
// unused TCP error must not reject a UDP/Packet selection. The same operation
// owns either that cold result or its executable protocol leaf; no factory
// closure retains the adapter's entire runtime until execution.
impl<'a, TLeaf, E> ClaimedTcpOutboundLeaf<'a>
    for TransportLeafTcpOperation<(Option<(&'a str, u16)>, Result<TLeaf, E>)>
where
    TLeaf: ProxyTransportTcpLeaf + Clone + 'static,
    E: std::fmt::Display + Send + Sync + 'a,
{
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        let prepared = self.leaf.1.as_ref().map_err(|error| {
            transport_tcp_connect_claim_prepare_failure::<TLeaf, _>(self.leaf.0, error)
        })?;
        Ok(Box::new(TransportLeafTcpOperation {
            leaf: prepared.clone(),
        }))
    }

    fn prepare_tcp_relay_hop(
        &self,
        _source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpRelayOperation>, EngineError> {
        let prepared = self
            .leaf
            .1
            .as_ref()
            .map_err(transport_tcp_relay_claim_prepare_error::<TLeaf, _>)?;
        Ok(Box::new(TransportLeafTcpOperation {
            leaf: prepared.clone(),
        }))
    }
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
