//! Direct's domain decision is made by engine; this role opens its bound carrier.
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
mod controlled;
use controlled::{Control, DirectOwner};
use zero_core::component::SessionId;
use zero_platform_tokio::contract::{
    admission::{SessionAdmission, SessionOwner},
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::erase_datagram,
    lifecycle::{erase_lifecycle, BuildError},
    network::CarrierContext,
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_traits::DialPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectOptions {
    pub(super) policy: DialPolicy,
    control: Option<DirectBudgets>,
}
impl DirectOptions {
    pub fn validate(policy: DialPolicy) -> Result<Self, zero_traits::DialPolicyError> {
        policy.validate()?;
        Ok(Self {
            policy,
            control: None,
        })
    }
    /// Explicit library preparation only. Production decoding supplies no
    /// socket/task/retirement defaults and retains its current legacy gate.
    pub fn controlled(
        mut self,
        max_sockets: NonZeroUsize,
        max_refresh_tasks: NonZeroUsize,
        retirement: Duration,
    ) -> Result<Self, BuildError> {
        if retirement.is_zero() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        self.control = Some(DirectBudgets {
            max_sockets,
            max_refresh_tasks,
            retirement,
        });
        Ok(self)
    }
    pub fn policy(&self) -> &DialPolicy {
        &self.policy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectBudgets {
    max_sockets: NonZeroUsize,
    max_refresh_tasks: NonZeroUsize,
    retirement: Duration,
}
pub struct DirectFactory;
pub(crate) fn decode(
    input: &zero_config::OutboundConfig,
) -> Result<DirectOptions, zero_config::ConfigError> {
    if !matches!(input.protocol, zero_config::OutboundProtocolConfig::Direct) {
        return Err(zero_config::ConfigError::InvalidOutbound(format!(
            "outbound `{}` is not Direct",
            input.tag
        )));
    }
    DirectOptions::validate(input.dial.to_policy()).map_err(|error| {
        zero_config::ConfigError::InvalidOutbound(format!("outbound `{}` dial: {error}", input.tag))
    })
}
pub(super) struct Outbound {
    pub(super) options: Arc<DirectOptions>,
    pub(super) state: Arc<SessionAdmission>,
    control: Option<Control>,
}
pub(super) fn during_open(
    state: &SessionAdmission,
    session: Option<SessionId>,
    reason: Reason,
) -> ChannelError {
    // A prefix may already have submitted a request; cancellation is not proof
    // that nothing reached the selected upstream.
    state.error(session, Stage::Dial, reason, DeliveryState::Unknown)
}

impl ComponentFactory for DirectFactory {
    type Options = DirectOptions;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        let mut static_identity = format!("{:?}", options.policy).into_bytes();
        if let Some(control) = &options.control {
            static_identity.extend_from_slice(b"controlled");
            static_identity.extend_from_slice(&control.retirement.as_nanos().to_be_bytes());
        }
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: if options.control.is_some() { vec![RoleKind::Stream] } else { vec![RoleKind::Stream, RoleKind::Datagram] },
            dependencies: vec![],
            // The neutral dial policy contains no credentials.
            static_identity: static_identity.into(),
            resources: options.control.as_ref().map_or_else(Vec::new, |control| vec![zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {
                dial: options.policy.clone(), max_sockets: control.max_sockets,
            }]),
            task_limit: options.control.as_ref().map(|control| control.max_refresh_tasks),
        }
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, state) = SessionOwner::new(context.component, &context.cancel);
        let (owner, control) = DirectOwner::new(owner, options.control.as_ref(), &context);
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound {
                    options: options.clone(),
                    state: state.clone(),
                    control: control.clone(),
                })),
                datagram: if control.is_none() {
                    Some(erase_datagram(Outbound {
                        options,
                        state,
                        control: None,
                    }))
                } else {
                    None
                },
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
        let session = self.state.check(&context)?;
        if context.upstream.binding().dial != self.options.policy {
            return Err(self.state.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let carrier = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let open = match &self.control {
            Some(control) => context.upstream.open_controlled_stream(
                zero_platform_tokio::contract::network::ControlledStreamRequest {
                    destination: request.destination,
                    context: carrier,
                    grants: control.grants.clone(),
                    tasks: control.tasks.clone(),
                    retirement: control.retirement,
                },
            ),
            None => context.upstream.open_stream(request.destination, carrier),
        };
        let io = tokio::select! {
            biased;
            _ = self.state.cancelled() => return Err(during_open(&self.state, session, Reason::Revoked)),
            _ = context.cancel.cancelled() => return Err(during_open(&self.state, session, Reason::Cancelled)),
            result = with_deadline(context.deadline, open) => result.map_err(|source| during_open(&self.state, session, Reason::Timeout).with_source(source))?,
        };
        let io = io.map_err(|error| self.state.attach(error, session))?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::PeerConfirmed);
        }
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
