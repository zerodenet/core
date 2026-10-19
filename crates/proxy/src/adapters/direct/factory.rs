//! Direct's domain decision is made by engine; this role opens its bound carrier.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::time::{timeout_at, Instant};
use zero_core::component::{ComponentId, SessionId};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    context::{ConnectFact, OpenContext},
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    network::CarrierContext,
    stream::{
        erase_stream, BoxStream, ConnectRequest, Connected, PeerConfirmation, StreamOutbound,
    },
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
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
struct State {
    ready: AtomicBool,
    cancel: CancellationToken,
    component: ComponentId,
}
struct Owner(Arc<State>);
impl Drop for Owner {
    fn drop(&mut self) {
        // Emergency admission closure; this is never a shutdown acknowledgement.
        self.0.ready.store(false, Ordering::Release);
        self.0.cancel.cancel();
    }
}
struct Outbound {
    options: Arc<DirectOptions>,
    state: Arc<State>,
}

fn failure(
    state: &State,
    session: Option<SessionId>,
    stage: Stage,
    reason: Reason,
) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Session);
    error.component = Some(state.component);
    error.session = session;
    error.delivery = DeliveryState::NotSubmitted;
    error
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
        let state = Arc::new(State {
            ready: AtomicBool::new(false),
            cancel: context.cancel.child_token(),
            component: context.component,
        });
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner(state.clone())),
            roles: ComponentRoles {
                stream: Some(erase_stream(Outbound { options, state })),
                ..Default::default()
            },
        })
    }
}

impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.0.cancel.is_cancelled() {
            let mut error = failure(&self.0, None, Stage::Prepare, Reason::Revoked);
            error.impact = Impact::Component;
            return Err(error);
        }
        if Instant::now() >= deadline {
            let mut error = failure(&self.0, None, Stage::Prepare, Reason::Timeout);
            error.impact = Impact::Component;
            return Err(error);
        }
        self.0.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.ready.store(false, Ordering::Release);
        self.0.cancel.cancel();
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        // Each connected stream is transferred to the caller's session scope.
        // Direct owns no shared socket, device, pool or internal driver.
        Ok(CloseReport::default())
    }
}

impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<Self::Stream>, ChannelError> {
        let session = context.session_identity()?;
        if !self.state.ready.load(Ordering::Acquire) || self.state.cancel.is_cancelled() {
            return Err(failure(&self.state, session, Stage::Dial, Reason::Revoked));
        }
        if context.cancel.is_cancelled() {
            return Err(failure(
                &self.state,
                session,
                Stage::Dial,
                Reason::Cancelled,
            ));
        }
        if Instant::now() >= context.deadline {
            return Err(failure(&self.state, session, Stage::Dial, Reason::Timeout));
        }
        if context.upstream.binding().dial != self.options.policy {
            return Err(failure(
                &self.state,
                session,
                Stage::Prepare,
                Reason::StalePlan,
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
            _ = self.state.cancel.cancelled() => return Err(failure(&self.state, session, Stage::Dial, Reason::Revoked)),
            _ = context.cancel.cancelled() => return Err(failure(&self.state, session, Stage::Dial, Reason::Cancelled)),
            result = timeout_at(context.deadline, open) => result.map_err(|source| failure(&self.state, session, Stage::Dial, Reason::Timeout).with_source(source))?,
        };
        let io = io.map_err(|mut error| {
            error.component.get_or_insert(self.state.component);
            if error.session.is_none() {
                error.session = session;
            }
            error
        })?;
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
