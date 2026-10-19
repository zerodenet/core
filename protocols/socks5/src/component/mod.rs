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
mod error;
mod options;
mod socket;
pub use options::Options;

pub struct Factory;
struct Outbound {
    options: Arc<Options>,
    admission: Arc<SessionAdmission>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound { options, admission })),
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
        let session = self.admission.check(&context)?;
        if context.upstream.binding().dial != self.options.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let request =
            wire::build_connect_request(&request.destination.address, request.destination.port)
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
        let carrier = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let open = context
            .upstream
            .open_stream(self.options.endpoint.clone(), carrier);
        let io = tokio::select! {
            biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session, Stage::Dial, Reason::Revoked, DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session, Stage::Dial, Reason::Cancelled, DeliveryState::Unknown)),
            result = with_deadline(context.deadline, open) => result.map_err(|source| self.admission.error(session, Stage::Dial, Reason::Timeout, DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error| self.admission.attach(error, session))?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let mut socket = socket::Socket::new(io);
        let submitting = AtomicBool::new(false);
        let handshake = wire::connect_prepared(
            &mut socket,
            &request,
            self.options.auth.as_ref().map(|auth| auth.as_ref()),
            |step| {
                if matches!(step, Progress::Submitting) {
                    submitting.store(true, Ordering::Release);
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
        let result = tokio::select! {
            biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session, Stage::Handshake, Reason::Revoked, DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session, Stage::Handshake, Reason::Cancelled, DeliveryState::Unknown)),
            result = with_deadline(context.deadline, handshake) => result.map_err(|source| self.admission.error(session, Stage::Handshake, Reason::Timeout, DeliveryState::Unknown).with_source(source))?,
        };
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
            return Err(error.with_source(error::HandshakeError {
                wire: source,
                source: original,
            }));
        }
        Ok(Connected {
            io: socket.into_inner(),
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
