//! Generic factory & outbound runner for stateless (per-connection) protocols.

use super::{ExclusiveIo, GuardedCall, LifecycleChain};
use crate::TcpRelayStream;
use std::{future::Future, sync::Arc};
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        admission::{SessionAdmission, SessionOwner},
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceOwner, RoleKind,
        },
        context::OpenContext,
        datagram::{
            erase_datagram, DatagramOutbound, DatagramRequest, DatagramRx, DatagramSession,
            DatagramTx, DynDatagramOutbound,
        },
        lifecycle::{BuildError, ChannelControl},
        stream::{erase_stream, BoxStream, ConnectRequest, Connected, StreamOutbound},
        ChannelError, DeliveryState, Reason, RetryClass, Stage,
    },
    controlled_carrier::loan::StreamLoan,
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

/// Optional pure datagram-over-stream protocol handshake plugged into
/// `StatelessOutboundFactory`.
pub trait DatagramProtocolHandshake<O: StatelessOutboundOptions>:
    Clone + Send + Sync + 'static
{
    type Prepared: Send;
    type Tx: DatagramTx + 'static;
    type Rx: DatagramRx + 'static;
    type Control: ChannelControl + 'static;

    /// Whether datagram open enforces `context.upstream.binding().dial == options.dial()`.
    fn enforce_dial_policy(&self) -> bool {
        true
    }

    fn prepare_datagram(
        &self,
        options: &O,
        request: &DatagramRequest,
    ) -> Result<Self::Prepared, HandshakeError>;

    fn handshake_datagram(
        &self,
        options: &O,
        io: TcpRelayStream,
        prepared: Self::Prepared,
        request: DatagramRequest,
        context: OpenContext,
        admission: Arc<SessionAdmission>,
        session: Option<zero_core::component::SessionId>,
    ) -> impl Future<Output = Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError>>
           + Send;
}

/// Marker for stream-only stateless outbounds without a datagram role.
#[derive(Clone, Copy, Default)]
pub struct NoDatagram;

pub trait DatagramRoleBuilder<O: StatelessOutboundOptions>: Clone + Send + Sync + 'static {
    fn build_datagram_role(
        &self,
        carrier: CarrierHandle<O>,
    ) -> Option<Arc<dyn DynDatagramOutbound>>;
}

impl<O: StatelessOutboundOptions> DatagramRoleBuilder<O> for NoDatagram {
    fn build_datagram_role(
        &self,
        _carrier: CarrierHandle<O>,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        None
    }
}

/// Shared carrier opener & admission handle used by both stream and datagram roles.
pub struct CarrierHandle<O: StatelessOutboundOptions> {
    #[cfg(any(feature = "ws", feature = "split_http"))]
    browser: Option<crate::browser_dialer::component::Handle>,
    options: Arc<O>,
    admission: Arc<SessionAdmission>,
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
    async fn open_carrier(&self, call: &GuardedCall<'_>) -> Result<TcpRelayStream, ChannelError> {
        #[cfg(any(feature = "ws", feature = "split_http"))]
        if call.context().upstream.binding().prefix.is_empty() {
            if let Some(browser) = &self.browser {
                return browser.open(call.context()).await;
            }
        }
        let io = call.open_stream(self.options.endpoint().clone()).await?;
        self.wrap_carrier(call, io).await
    }

    async fn wrap_carrier(
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

struct StatelessDatagramRole<O: StatelessOutboundOptions, D> {
    datagram: D,
    carrier: CarrierHandle<O>,
}

impl<O, D> DatagramOutbound for StatelessDatagramRole<O, D>
where
    O: StatelessOutboundOptions,
    D: DatagramProtocolHandshake<O>,
{
    type Tx = D::Tx;
    type Rx = D::Rx;
    type Control = D::Control;

    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let expected_dial = self
            .datagram
            .enforce_dial_policy()
            .then(|| self.carrier.options.dial());
        let call = GuardedCall::enter(&self.carrier.admission, &context, expected_dial)?;
        let prepared = self
            .datagram
            .prepare_datagram(&self.carrier.options, &request)
            .map_err(|err| err.into_channel_error(&call))?;
        let io = self.carrier.open_carrier(&call).await?;
        let session = call.session();
        drop(call);
        self.datagram
            .handshake_datagram(
                &self.carrier.options,
                io,
                prepared,
                request,
                context,
                self.carrier.admission.clone(),
                session,
            )
            .await
    }
}

/// Wrapper implementing `DatagramRoleBuilder` for any `DatagramProtocolHandshake`.
#[derive(Clone, Copy)]
pub struct WithDatagram<D>(pub D);

impl<O, D> DatagramRoleBuilder<O> for WithDatagram<D>
where
    O: StatelessOutboundOptions,
    D: DatagramProtocolHandshake<O>,
{
    fn build_datagram_role(
        &self,
        carrier: CarrierHandle<O>,
    ) -> Option<Arc<dyn DynDatagramOutbound>> {
        carrier.options.supports_datagram().then(|| {
            erase_datagram(StatelessDatagramRole {
                datagram: self.0.clone(),
                carrier,
            })
        })
    }
}

struct StatelessStreamRole<P: StreamProtocolHandshake> {
    protocol: P,
    carrier: CarrierHandle<P::Options>,
}

impl<P: StreamProtocolHandshake> StreamOutbound for StatelessStreamRole<P> {
    type Stream = BoxStream;

    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let call = GuardedCall::enter(
            &self.carrier.admission,
            &context,
            Some(self.carrier.options.dial()),
        )?;
        let prepared = self
            .protocol
            .prepare(&self.carrier.options, &request)
            .map_err(|err| err.into_channel_error(&call))?;
        let io = self.carrier.open_carrier(&call).await?;
        self.protocol
            .handshake(&self.carrier.options, io, prepared, &call)
            .await
    }

    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        if !self.protocol.supports_carrier_loan(&self.carrier.options) {
            let session = context.session_identity().ok().flatten();
            return Err(self.carrier.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        }
        let call = GuardedCall::enter(
            &self.carrier.admission,
            &context,
            Some(self.carrier.options.dial()),
        )?;
        let prepared = self
            .protocol
            .prepare(&self.carrier.options, &request)
            .map_err(|err| err.into_channel_error(&call))?;
        #[cfg(any(feature = "ws", feature = "split_http"))]
        if context.upstream.binding().prefix.is_empty() {
            if let Some(browser) = &self.carrier.browser {
                let (io, control, original) = browser.open_parts(&context).await?;
                let connected = self
                    .protocol
                    .handshake(&self.carrier.options, io, prepared, &call)
                    .await?;
                return Ok(Connected {
                    io: StreamLoan::new(connected.io, control, original),
                    peer_confirmation: connected.peer_confirmation,
                });
            }
        }
        let StreamLoan { io, owner } = call
            .borrow_stream(self.carrier.options.endpoint().clone())
            .await?;
        let mut peer_confirmation =
            zero_platform_tokio::contract::stream::PeerConfirmation::Deferred;
        let confirmation = &mut peer_confirmation;
        let io = io
            .try_map(|io| async move {
                let io = self.carrier.wrap_carrier(&call, io).await?;
                let connected = self
                    .protocol
                    .handshake(&self.carrier.options, io, prepared, &call)
                    .await?;
                *confirmation = connected.peer_confirmation;
                Ok::<_, ChannelError>(connected.io)
            })
            .await?;
        Ok(Connected {
            io: StreamLoan { io, owner },
            peer_confirmation,
        })
    }
}

/// Generic `ComponentFactory` for stateless (per-connection) outbounds.
#[derive(Clone, Copy)]
pub struct StatelessOutboundFactory<P, D = NoDatagram> {
    protocol: P,
    datagram: D,
}

impl<P: Default> StatelessOutboundFactory<P, NoDatagram> {
    pub fn stream_only() -> Self {
        Self {
            protocol: P::default(),
            datagram: NoDatagram,
        }
    }
}

impl<P, D> StatelessOutboundFactory<P, WithDatagram<D>> {
    pub fn with_datagram(protocol: P, datagram: D) -> Self {
        Self {
            protocol,
            datagram: WithDatagram(datagram),
        }
    }
}

impl<P, D> ComponentFactory for StatelessOutboundFactory<P, D>
where
    P: StreamProtocolHandshake,
    D: DatagramRoleBuilder<P::Options>,
{
    type Options = P::Options;

    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut roles = vec![RoleKind::Stream];
        if options.supports_datagram() {
            roles.push(RoleKind::Datagram);
        }
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles,
            dependencies: vec![],
            static_identity: options.static_identity(),
            resources: vec![],
            task_limit: None,
        };
        #[cfg(any(feature = "ws", feature = "split_http"))]
        if let Some(browser) = options.browser() {
            browser.declare(&mut spec);
        }
        options.describe_extra(&mut spec);
        spec
    }

    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        #[cfg(any(feature = "ws", feature = "split_http"))]
        let prepared_browser = options
            .browser()
            .map(|browser_opts| {
                crate::browser_dialer::component::Owner::build(browser_opts.clone(), &context)
            })
            .transpose()?;
        #[cfg(any(feature = "ws", feature = "split_http"))]
        let (browser_owner, browser) = match prepared_browser {
            Some((owner, handle)) => (Some(owner), Some(handle)),
            None => (None, None),
        };
        let (session_owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let chain = LifecycleChain::new();
        #[cfg(any(feature = "ws", feature = "split_http"))]
        let chain = chain.push_typed_opt(browser_owner);
        let lifecycle = chain.push_typed(session_owner).into_boxed();
        let carrier = CarrierHandle {
            #[cfg(any(feature = "ws", feature = "split_http"))]
            browser,
            options,
            admission,
        };
        let datagram = self.datagram.build_datagram_role(carrier.clone());
        let stream = Some(erase_stream(StatelessStreamRole {
            protocol: self.protocol.clone(),
            carrier,
        }));
        Ok(ComponentInstance {
            lifecycle,
            roles: ComponentRoles {
                stream,
                datagram,
                ..Default::default()
            },
        })
    }
}
