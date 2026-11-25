use super::{
    CarrierHandle, DatagramRoleBuilder, NoDatagram, StatelessOutboundOptions,
    StreamProtocolHandshake, WithDatagram,
};
use crate::runner::{GuardedCall, LifecycleChain};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        admission::SessionOwner,
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceOwner, RoleKind,
        },
        context::OpenContext,
        lifecycle::BuildError,
        stream::{erase_stream, BoxStream, ConnectRequest, Connected, StreamOutbound},
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::loan::StreamLoan,
};

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
