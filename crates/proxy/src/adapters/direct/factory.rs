//! Direct's domain decision is made by engine; this role opens its bound carrier.
use std::sync::Arc;
use zero_core::component::SessionId;
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
use zero_traits::DialPolicy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectOptions {
    policy: DialPolicy,
}
impl DirectOptions {
    pub fn validate(policy: DialPolicy) -> Result<Self, zero_traits::DialPolicyError> {
        policy.validate()?;
        Ok(Self { policy })
    }
    pub fn policy(&self) -> &DialPolicy {
        &self.policy
    }
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
struct Outbound {
    options: Arc<DirectOptions>,
    state: Arc<SessionAdmission>,
}
fn during_open(
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
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Stream],
            dependencies: vec![],
            // The neutral dial policy contains no credentials.
            static_identity: format!("{:?}", options.policy).into_bytes().into(),
            resources: vec![],
        }
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, state) = SessionOwner::new(context.component, &context.cancel);
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound { options, state })),
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
        let open = context.upstream.open_stream(request.destination, carrier);
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
