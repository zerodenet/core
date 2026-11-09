//! Protocol-owned ordinary VLESS Stream roles over a constrained real carrier.
mod carrier;
mod connect;
mod datagram;
pub mod grpc;
pub mod inbound;
pub(crate) mod material;
#[cfg(feature = "reality")]
pub mod mux;
pub(crate) mod navigation;
mod options;
mod ordinary_datagram;
mod owner;
pub mod quic;
mod transport;
pub use options::Options;
use std::sync::Arc;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{with_deadline, OpenContext},
    lifecycle::{erase_lifecycle, BuildError},
    network::CarrierContext,
    scope::TaskRegistrar,
    stream::{erase_stream, BoxStream, ConnectRequest, Connected, StreamOutbound},
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_transport::outbound_stack::{
    connect_layered_transport_stack, connect_relay_transport_stack, StreamTransportStack,
};

pub struct Factory;
struct Outbound {
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    tasks: TaskRegistrar,
    ech_resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: std::iter::once(RoleKind::Stream)
                .chain(options.flow.is_none().then_some(RoleKind::Datagram))
                .collect(),
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: options
                .material
                .as_ref()
                .and_then(|material| material.resource())
                .into_iter()
                .collect(),
            // Even failed REALITY proxy authentication can retain the actual
            // verified decoy carrier; this requires a real host task owner.
            task_limit: options
                .reality
                .as_ref()
                .and_then(|_| std::num::NonZeroUsize::new(64)),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, None)
    }
}
impl Factory {
    pub fn build_with_ech_resolver(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_owned(options, context, Some(resolver))
    }
    fn build_owned(
        &self,
        options: Arc<Options>,
        context: BuildContext,
        resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    ) -> Result<ComponentInstance, BuildError> {
        let (session, admission) = SessionOwner::new(context.component, &context.cancel);
        let dynamic = options
            .tls
            .as_ref()
            .map(|tls| tls.options.ech.source())
            .transpose()
            .map_err(|source| {
                admission
                    .error(
                        None,
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(std::io::Error::other(source))
            })?
            .flatten()
            .is_some_and(|source| matches!(source, zero_traits::EchConfigSource::Dns { .. }));
        if options
            .tls
            .as_ref()
            .is_some_and(|tls| tls.ca_cert_path.is_some())
            && options.material.is_none()
            || dynamic && (options.material.is_none() || resolver.is_none())
        {
            return Err(admission.error(
                None,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner::Owner {
                session,
                material: options.material.clone(),
                instance: context.instance(),
                resources: context.resources,
                cancel: context.cancel.clone(),
            }),
            roles: ComponentRoles {
                datagram: options.flow.is_none().then(|| {
                    zero_platform_tokio::contract::datagram::erase_datagram(Outbound {
                        options: options.clone(),
                        admission: admission.clone(),
                        tasks: context.tasks.clone(),
                        ech_resolver: resolver.clone(),
                    })
                }),
                stream: Some(erase_stream(Outbound {
                    options,
                    admission,
                    tasks: context.tasks,
                    ech_resolver: resolver,
                })),
                ..Default::default()
            },
        })
    }
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let session = self.admission.check(&context)?;
        if context.upstream.binding().dial != self.options.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if request.destination.port == 0 {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let bytes = crate::outbound::build_tcp_request_to(
            &request.destination.address,
            request.destination.port,
            &self.options.uuid,
            self.options.flow,
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let io = self.open_transport(&context, session).await?;
        connect::submit(&self.options, &self.admission, session, context, io, bytes).await
    }
}
