//! SOCKS5 owns validated options, authentication and CONNECT wire state.
use crate::outbound::wire::{self, Progress};
use alloc::{sync::Arc, vec};
use std::sync::atomic::{AtomicBool, Ordering};
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{with_deadline, ConnectFact, OpenContext},
    lifecycle::{erase_lifecycle, BuildError},
    network::CarrierContext,
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, DeliveryState, Reason, Stage,
};
mod carrier;
mod datagram;
mod error;
pub mod inbound;
pub mod listener;
mod options;
mod owned;
mod owner;
mod socket;
pub use options::Options;

pub struct Factory;
struct Outbound {
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
    carriers: Option<zero_platform_tokio::controlled_carrier::CarrierGrants>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream, RoleKind::Datagram],
            dependencies: options.node_dns.into_iter().collect(),
            static_identity: options.identity.clone(),
            resources: options.carrier_limit.into_iter().map(|max_sockets|
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledAssociationPool {
                    endpoint: options.endpoint.clone(), dial: options.dial.clone(), max_sockets,
                }).collect(),
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if context.dependencies.len() != usize::from(options.node_dns.is_some())
            || options
                .node_dns
                .is_some_and(|node| context.dependency(node).is_none())
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let carrier = options.carrier_limit.map(|limit| {
            zero_platform_tokio::controlled_carrier::CarrierOwner::new(
                context.instance(),
                limit,
                &context.cancel,
            )
        });
        let carriers = carrier.as_ref().map(|owner| owner.grants());
        let associations = Arc::new(datagram::association::Associations::default());
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner::Owner {
                session: owner,
                carrier,
                resources: context.resources,
                tasks: context.tasks,
                associations: associations.clone(),
            }),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options: options.clone(),
                    admission: admission.clone(),
                    carriers: carriers.clone(),
                })),
                datagram: Some(zero_platform_tokio::contract::datagram::erase_datagram(
                    datagram::Outbound::new(options, admission, carriers, associations),
                )),
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
    ) -> Result<Connected<Self::Stream>, ChannelError> {
        let call = zero_transport::runner::GuardedCall::enter(
            &self.admission,
            &context,
            Some(&self.options.dial),
        )?;
        let session = call.session();
        let request =
            wire::build_connect_request(&request.destination.address, request.destination.port)
                .map_err(|source| {
                    call.error(
                        Stage::Prepare,
                        Reason::InvalidConfig,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source)
                })?;
        if self.carriers.is_some()
            && (session.is_none() || context.upstream.has_tracked_business_caller())
        {
            return self.connect_owned(request, context, session).await;
        }
        let io = call.open_stream(self.options.endpoint.clone()).await?;
        call.facts().record(ConnectFact::CarrierReady);
        self.handshake(io, request, &context, session).await
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        self.borrow(request, context).await
    }
}
impl Outbound {
    async fn handshake<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send>(
        &self,
        io: S,
        request: alloc::vec::Vec<u8>,
        context: &OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<Connected<S>, ChannelError> {
        let mut socket = socket::Socket::new(io);

        let submitting = AtomicBool::new(false);
        let rejected = AtomicBool::new(false);
        let handshake = wire::connect_prepared(
            &mut socket,
            &request,
            self.options.auth.as_ref().map(|auth| auth.as_ref()),
            |step| {
                if matches!(step, Progress::Submitting) {
                    submitting.store(true, Ordering::Release);
                }
                if matches!(step, Progress::Rejected) {
                    rejected.store(true, Ordering::Release);
                }
                if let Some(observer) = &context.observer {
                    match step {
                        Progress::Submitted => {
                            observer.record(session, ConnectFact::RequestSubmitted)
                        }
                        Progress::Confirmed => observer.record(session, ConnectFact::PeerConfirmed),
                        _ => {}
                    }
                }
            },
        );
        let call =
            zero_transport::runner::GuardedCall::from_admitted(&self.admission, context, session);
        let result = call
            .run(Stage::Handshake, DeliveryState::Unknown, handshake)
            .await?;
        if let Err(source) = result {
            let original = socket.take_source();
            let reason = if original.is_some() || matches!(source, zero_core::Error::Io(_)) {
                Reason::Io
            } else {
                Reason::Protocol
            };
            if reason == Reason::Protocol {
                if let Some(observer) = &context.observer {
                    observer.record(session, ConnectFact::PeerRejected);
                }
            }
            let mut error =
                self.admission
                    .error(session, Stage::Handshake, reason, DeliveryState::Unknown);
            // A root carrier and a handshake that has not started CONNECT
            // prove that no proxy request to the business destination was sent.
            // Once a prefix or a partial CONNECT is possible, never replay it.
            if context.upstream.binding().prefix.is_empty() && !submitting.load(Ordering::Acquire) {
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
            }
            let source = error::HandshakeError {
                wire: source,
                source: original,
            };
            return Err(if rejected.load(Ordering::Acquire) {
                error.with_source(
                    zero_platform_tokio::contract::network::UpstreamRejection::new(source),
                )
            } else {
                error.with_source(source)
            });
        }
        Ok(Connected {
            io: socket.into_inner(),
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
