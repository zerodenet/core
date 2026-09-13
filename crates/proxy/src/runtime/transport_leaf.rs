#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
use zero_core::Session;
#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
use zero_transport::StreamTraffic;
use zero_transport::{RuntimeError, TcpRelayStream};

#[cfg(any(
    feature = "tcp-tunnel-runtime",
    feature = "tcp-session-runtime",
    feature = "managed-stream-runtime"
))]
use crate::protocol_registry::UpstreamConnectServices;

pub(crate) trait ProxyTransportLeaf {
    fn tag(&self) -> &str;

    fn server(&self) -> &str;

    fn port(&self) -> u16;

    #[cfg(feature = "managed-stream-runtime")]
    fn validate_udp_relay_final_hop(&self) -> Result<(), RuntimeError> {
        Ok(())
    }
}

impl<T> ProxyTransportLeaf for T
where
    T: zero_traits::ProtocolOutboundLeaf,
{
    fn tag(&self) -> &str {
        zero_traits::ProtocolOutboundLeaf::tag(self)
    }

    fn server(&self) -> &str {
        zero_traits::ProtocolOutboundLeaf::server(self)
    }

    fn port(&self) -> u16 {
        zero_traits::ProtocolOutboundLeaf::port(self)
    }

    #[cfg(feature = "managed-stream-runtime")]
    fn validate_udp_relay_final_hop(&self) -> Result<(), RuntimeError> {
        match zero_traits::ProtocolOutboundLeaf::udp_relay_final_hop_error(self) {
            Some(message) => Err(zero_core::Error::Unsupported(message).into()),
            None => Ok(()),
        }
    }
}

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
#[async_trait::async_trait]
pub(crate) trait ProxyTransportTcpLeaf: ProxyTransportLeaf + Send + Sync {
    const TCP_CONNECT_STAGE: &'static str;
    const TCP_INVALID_CONNECT_CONFIG: &'static str;
    const TCP_INVALID_RELAY_CONFIG: &'static str;

    async fn open_tcp_stream(
        &self,
        services: UpstreamConnectServices,
        session: &Session,
    ) -> Result<(TcpRelayStream, StreamTraffic), RuntimeError>;

    async fn open_tcp_relay_hop(
        &self,
        _services: UpstreamConnectServices,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError>;
    async fn open_tcp_relay_carrier(
        &self,
        services: UpstreamConnectServices,
        carrier: crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'_>,
        session: &Session,
    ) -> Result<TcpRelayStream, zero_engine::EngineError> {
        self.open_tcp_relay_hop(services, carrier.open().await?, session)
            .await
            .map_err(Into::into)
    }
}

#[cfg(feature = "managed-stream-runtime")]
pub(crate) trait ProxyTransportUdpLeaf: ProxyTransportLeaf {
    type RuntimeResume: Send + Sync + std::fmt::Debug + 'static;

    const UDP_DIRECT_STAGE: &'static str;
    const UDP_INVALID_CONFIG: &'static str;
    const UDP_RELAY_FINAL_STAGE: &'static str;

    fn direct_udp_resume(&self) -> Self::RuntimeResume;

    fn relay_final_hop_udp_resume(&self) -> Self::RuntimeResume;
}

#[cfg(feature = "managed-stream-runtime")]
#[async_trait::async_trait]
pub(crate) trait ProxyRelayTwoStreamTransportLeaf:
    ProxyTransportUdpLeaf + Send + Sync
{
    const UDP_RELAY_CHAIN_STAGE: &'static str;

    fn udp_relay_needs_two_streams(&self) -> bool;

    fn relay_two_stream_udp_resume(&self) -> Self::RuntimeResume;

    async fn open_relay_two_stream_udp_transport(
        &self,
        services: UpstreamConnectServices,
        post_stream: TcpRelayStream,
        get_stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError>;
    fn udp_relay_uses_connector(&self) -> bool {
        false
    }
    async fn open_udp_relay_connector(
        &self,
        _services: UpstreamConnectServices,
        _connector: zero_transport::relay_connector::RelayStreamConnector,
    ) -> Result<TcpRelayStream, RuntimeError> {
        Err(zero_core::Error::Unsupported("reusable UDP relay carrier is unsupported").into())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedTransportEndpoint<'a> {
    pub(crate) tag: &'a str,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
}

pub(crate) struct PreparedTransportLeaf<TLeaf> {
    leaf: TLeaf,
}

impl<TLeaf> PreparedTransportLeaf<TLeaf> {
    pub(crate) fn new(leaf: TLeaf) -> Self {
        Self { leaf }
    }
}

impl<TLeaf> PreparedTransportLeaf<TLeaf>
where
    TLeaf: ProxyTransportLeaf,
{
    pub(crate) fn endpoint(&self) -> PreparedTransportEndpoint<'_> {
        PreparedTransportEndpoint {
            tag: self.leaf.tag(),
            server: self.leaf.server(),
            port: self.leaf.port(),
        }
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) fn validate_udp_relay_final_hop(&self) -> Result<(), RuntimeError> {
        self.leaf.validate_udp_relay_final_hop()
    }
}

#[cfg(any(feature = "tcp-tunnel-runtime", feature = "tcp-session-runtime"))]
impl<TLeaf> PreparedTransportLeaf<TLeaf>
where
    TLeaf: ProxyTransportTcpLeaf,
{
    pub(crate) async fn open_tcp_stream(
        &self,
        services: UpstreamConnectServices,
        session: &Session,
    ) -> Result<(TcpRelayStream, StreamTraffic), RuntimeError> {
        self.leaf.open_tcp_stream(services, session).await
    }

    pub(crate) async fn open_tcp_relay_hop(
        &self,
        services: UpstreamConnectServices,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.leaf
            .open_tcp_relay_hop(services, stream, session)
            .await
    }
    pub(crate) async fn open_tcp_relay_carrier(
        &self,
        services: UpstreamConnectServices,
        carrier: crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'_>,
        session: &Session,
    ) -> Result<TcpRelayStream, zero_engine::EngineError> {
        self.leaf
            .open_tcp_relay_carrier(services, carrier, session)
            .await
    }
}

#[cfg(feature = "managed-stream-runtime")]
impl<TLeaf> PreparedTransportLeaf<TLeaf>
where
    TLeaf: ProxyTransportUdpLeaf,
{
    pub(crate) fn direct_udp_resume(&self) -> TLeaf::RuntimeResume {
        self.leaf.direct_udp_resume()
    }

    pub(crate) fn relay_final_hop_udp_resume(&self) -> TLeaf::RuntimeResume {
        self.leaf.relay_final_hop_udp_resume()
    }
}

#[cfg(feature = "managed-stream-runtime")]
impl<TLeaf> PreparedTransportLeaf<TLeaf>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf,
{
    pub(crate) fn udp_relay_needs_two_streams(&self) -> bool {
        self.leaf.udp_relay_needs_two_streams()
    }

    pub(crate) fn relay_two_stream_udp_resume(&self) -> TLeaf::RuntimeResume {
        self.leaf.relay_two_stream_udp_resume()
    }
}

#[cfg(feature = "managed-stream-runtime")]
impl<TLeaf> PreparedTransportLeaf<TLeaf>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf,
{
    pub(crate) async fn open_relay_two_stream_udp_transport(
        &self,
        services: UpstreamConnectServices,
        post_stream: TcpRelayStream,
        get_stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.leaf
            .open_relay_two_stream_udp_transport(services, post_stream, get_stream)
            .await
    }
    pub(crate) fn udp_relay_uses_connector(&self) -> bool {
        self.leaf.udp_relay_uses_connector()
    }
    pub(crate) async fn open_udp_relay_connector(
        &self,
        services: UpstreamConnectServices,
        connector: zero_transport::relay_connector::RelayStreamConnector,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.leaf
            .open_udp_relay_connector(services, connector)
            .await
    }
}
