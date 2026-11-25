//! Generic factory & outbound runner for stateless (per-connection) protocols.

mod datagram;
mod factory;

pub use datagram::{DatagramProtocolHandshake, DatagramRoleBuilder, NoDatagram, WithDatagram};
pub use factory::StatelessOutboundFactory;

use super::{ExclusiveIo, GuardedCall};
use crate::TcpRelayStream;
use std::{future::Future, sync::Arc};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    component::ComponentSpec,
    stream::{BoxStream, ConnectRequest, Connected},
    ChannelError, DeliveryState, Reason, RetryClass, Stage,
};
use zero_traits::DialPolicy;

#[cfg(any(
    feature = "tls",
    feature = "ws",
    feature = "grpc",
    feature = "h2",
    feature = "http_upgrade"
))]
use super::{wrap_stateless_carrier, StatelessTransportStack};

#[derive(Debug)]
struct BoxedError(Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for BoxedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for BoxedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Protocol-level handshake error normalized into a host `ChannelError`.
#[derive(Debug)]
pub struct HandshakeError {
    pub stage: Stage,
    pub reason: Reason,
    pub delivery: DeliveryState,
    pub retry: Option<RetryClass>,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl HandshakeError {
    pub fn invalid_config(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self {
            stage: Stage::Prepare,
            reason: Reason::InvalidConfig,
            delivery: DeliveryState::NotSubmitted,
            retry: None,
            source: Some(Box::new(source)),
        }
    }

    pub fn unsupported() -> Self {
        Self {
            stage: Stage::Prepare,
            reason: Reason::Unsupported,
            delivery: DeliveryState::NotSubmitted,
            retry: None,
            source: None,
        }
    }

    pub fn into_channel_error(self, call: &GuardedCall<'_>) -> ChannelError {
        let mut error = call.error(self.stage, self.reason, self.delivery);
        if let Some(retry) = self.retry {
            error.retry = retry;
        }
        if let Some(source) = self.source {
            error = error.with_source(BoxedError(source));
        }
        error
    }
}

/// Static options contract for a stateless outbound component.
pub trait StatelessOutboundOptions: Send + Sync + 'static {
    fn endpoint(&self) -> &Destination;
    fn dial(&self) -> &DialPolicy;
    fn static_identity(&self) -> Arc<[u8]>;

    fn supports_datagram(&self) -> bool {
        false
    }

    #[cfg(any(feature = "ws", feature = "split_http"))]
    fn browser(&self) -> Option<&Arc<crate::browser_dialer::component::Options>> {
        None
    }

    #[cfg(any(
        feature = "tls",
        feature = "ws",
        feature = "grpc",
        feature = "h2",
        feature = "http_upgrade"
    ))]
    fn transport_stack(&self) -> Option<StatelessTransportStack<'_>> {
        None
    }

    fn describe_extra(&self, _spec: &mut ComponentSpec) {}
}

/// Pure stream protocol handshake plugged into `StatelessOutboundFactory`.
pub trait StreamProtocolHandshake: Clone + Send + Sync + 'static {
    type Options: StatelessOutboundOptions;
    type Prepared: Send;

    /// Validate and pre-encode the protocol request before dialing a carrier.
    fn prepare(
        &self,
        options: &Self::Options,
        request: &ConnectRequest,
    ) -> Result<Self::Prepared, HandshakeError>;

    /// Perform protocol framing/handshake over the ready carrier stream.
    fn handshake(
        &self,
        options: &Self::Options,
        io: TcpRelayStream,
        prepared: Self::Prepared,
        call: &GuardedCall<'_>,
    ) -> impl Future<Output = Result<Connected<BoxStream>, ChannelError>> + Send;

    /// Whether `StreamOutbound::connect_carrier` is supported for these options.
    fn supports_carrier_loan(&self, options: &Self::Options) -> bool {
        #[cfg(any(feature = "ws", feature = "split_http"))]
        {
            options.browser().is_some()
        }
        #[cfg(not(any(feature = "ws", feature = "split_http")))]
        {
            let _ = options;
            false
        }
    }
}

/// Shared carrier opener & admission handle used by both stream and datagram roles.
pub struct CarrierHandle<O: StatelessOutboundOptions> {
    #[cfg(any(feature = "ws", feature = "split_http"))]
    pub(super) browser: Option<crate::browser_dialer::component::Handle>,
    pub(super) options: Arc<O>,
    pub(super) admission: Arc<SessionAdmission>,
}

impl<O: StatelessOutboundOptions> Clone for CarrierHandle<O> {
    fn clone(&self) -> Self {
        Self {
            #[cfg(any(feature = "ws", feature = "split_http"))]
            browser: self.browser.clone(),
            options: self.options.clone(),
            admission: self.admission.clone(),
        }
    }
}

impl<O: StatelessOutboundOptions> CarrierHandle<O> {
    pub(super) async fn open_carrier(
        &self,
        call: &GuardedCall<'_>,
    ) -> Result<TcpRelayStream, ChannelError> {
        #[cfg(any(feature = "ws", feature = "split_http"))]
        if call.context().upstream.binding().prefix.is_empty() {
            if let Some(browser) = &self.browser {
                return browser.open(call.context()).await;
            }
        }
        let io = call.open_stream(self.options.endpoint().clone()).await?;
        self.wrap_carrier(call, io).await
    }

    pub(super) async fn wrap_carrier(
        &self,
        call: &GuardedCall<'_>,
        io: BoxStream,
    ) -> Result<TcpRelayStream, ChannelError> {
        #[cfg(any(
            feature = "tls",
            feature = "ws",
            feature = "grpc",
            feature = "h2",
            feature = "http_upgrade"
        ))]
        if let Some(stack) = self.options.transport_stack() {
            return wrap_stateless_carrier(call, io, &stack).await;
        }
        let _ = call;
        Ok(TcpRelayStream::new(ExclusiveIo::new(io)))
    }
}
