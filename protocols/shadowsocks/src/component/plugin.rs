//! Explicit ownership of the adapter's actual SIP003 native child.
use crate::transport::plugin::{PluginTermination, ShadowsocksPluginResource};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::time::{timeout_at, Instant};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceAccess, ResourceDeclaration, ResourceOwner,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, TaskClass},
    scope::{SpawnError, TaskRegistrar},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};

#[derive(Clone, Debug)]
pub struct PluginOptions {
    resource: ShadowsocksPluginResource,
    identity: Arc<str>,
    static_identity: Arc<[u8]>,
    legacy_budget: Option<crate::shared::LegacyReplayBudgetProposal>,
    #[cfg(feature = "blake3")]
    udp_budget: Option<crate::udp::UdpBudgetProposal>,
}
impl PluginOptions {
    #[cfg(feature = "blake3")]
    pub fn with_udp_budget_proposal(mut self, proposal: crate::udp::UdpBudgetProposal) -> Self {
        self.udp_budget = Some(proposal);
        self.rebuild_identity();
        self
    }
    pub fn with_legacy_replay_budget_proposal(
        mut self,
        proposal: crate::shared::LegacyReplayBudgetProposal,
    ) -> Self {
        self.legacy_budget = Some(proposal);
        self.rebuild_identity();
        self
    }
    fn rebuild_identity(&mut self) {
        use std::{
            collections::hash_map::RandomState,
            hash::{BuildHasher, Hash, Hasher},
            sync::OnceLock,
        };
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = self.identity.as_bytes().to_vec();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            #[cfg(feature = "blake3")]
            self.udp_budget
                .as_ref()
                .map(|proposal| proposal.identity())
                .hash(&mut hash);
            self.legacy_budget
                .as_ref()
                .map(|proposal| proposal.identity())
                .hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        // Same original Child/plan ResourceOwner; static replacement identity
        // depends on all actual budget views, independently of setter order.
        self.static_identity = identity.into();
    }
    pub(crate) fn new(resource: ShadowsocksPluginResource) -> Self {
        Self {
            identity: resource.identity().into(),
            static_identity: resource.identity().as_bytes().into(),
            legacy_budget: None,
            #[cfg(feature = "blake3")]
            udp_budget: None,
            resource,
        }
    }
}
pub struct PluginFactory;
impl ComponentFactory for PluginFactory {
    type Options = PluginOptions;
    fn describe(&self, options: &PluginOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint(options.identity.clone()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.static_identity.clone(),
            resources: vec![ResourceDeclaration::Process {
                program: options.resource.program(),
                // One plan has one actual Child; it never respawns that child.
                max_starts: NonZeroUsize::new(1).unwrap(),
            }],
            task_limit: NonZeroUsize::new(1),
        }
    }
    fn build(
        &self,
        options: Arc<PluginOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(Owner {
                resource: options.resource.clone(),
                legacy_budget: options.legacy_budget.clone(),
                #[cfg(feature = "blake3")]
                udp_budget: options.udp_budget.clone(),
                resources: context.resources,
                component: context.component,
                tasks: context.tasks,
                cancel: context.cancel.child_token(),
                attempted: false,
                started: false,
                claimed: Arc::new(AtomicBool::new(false)),
            }),
            roles: ComponentRoles::default(),
        })
    }
}
struct Owner {
    legacy_budget: Option<crate::shared::LegacyReplayBudgetProposal>,
    #[cfg(feature = "blake3")]
    udp_budget: Option<crate::udp::UdpBudgetProposal>,
    resource: ShadowsocksPluginResource,
    resources: Arc<dyn ResourceAccess>,
    component: ComponentId,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
    attempted: bool,
    started: bool,
    claimed: Arc<AtomicBool>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.claimed.load(Ordering::Acquire) {
            self.resource.revoke();
        }
        // A persistent plan still holds the Child; Drop is not a close report.
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.started && !self.cancel.is_cancelled() {
            return Ok(());
        }
        if self.attempted || self.cancel.is_cancelled() {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Revoked,
                "plugin start is not admissible",
            ));
        }
        if Instant::now() >= deadline {
            return Err(failure(
                self.component,
                Stage::Prepare,
                Reason::Timeout,
                "plugin start deadline elapsed",
            ));
        }
        self.attempted = true;
        if let Some(proposal) = &self.legacy_budget {
            proposal.activate().map_err(|source| {
                failure(
                    self.component,
                    Stage::Prepare,
                    Reason::Capacity,
                    "legacy Bloom bitmap budget proposal cannot start",
                )
                .with_source(source)
            })?;
        }
        #[cfg(feature = "blake3")]
        if let Some(proposal) = &self.udp_budget {
            proposal.activate().map_err(|source| {
                failure(
                    self.component,
                    Stage::Prepare,
                    Reason::Capacity,
                    "SS UDP budget proposal cannot start",
                )
                .with_source(source)
            })?;
        }
        timeout_at(
            deadline,
            self.resource
                .start_authorized(self.resources.clone(), self.claimed.clone()),
        )
        .await
        .map_err(|source| {
            failure(
                self.component,
                Stage::Prepare,
                Reason::Timeout,
                "plugin readiness deadline elapsed",
            )
            .with_source(source)
        })?
        .map_err(|source| startup_failure(self.component, source))?;
        let resource = self.resource.clone();
        let component = self.component;
        let cancel = self.cancel.clone();
        let scope_cancel = self.tasks.cancellation();
        self.tasks.spawn(TaskClass::Critical, async move {
            tokio::select! { biased;
                _ = cancel.cancelled() => Ok(()),
                _ = scope_cancel.cancelled() => Ok(()),
                result = resource.wait_exit() => match result {
                    Ok(status) => Err(failure(component, Stage::Receive, Reason::Io, "native plugin child exited").with_source(std::io::Error::other(format!("native plugin child exited: {status}")))),
                    Err(source) => Err(failure(component, Stage::Receive, Reason::Io, "native plugin child status failed").with_source(source)),
                },
            }
        }).map_err(|source| failure(self.component, Stage::Prepare, match source {
            SpawnError::NotStarted => Reason::InvalidConfig,
            SpawnError::Capacity => Reason::Capacity,
            SpawnError::Closing => Reason::Revoked,
            SpawnError::IdentityExhausted => Reason::Internal,
        }, "plugin monitor registration failed").with_source(source))?;
        self.started = true;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.cancel.cancel();
        if self.claimed.load(Ordering::Acquire) {
            self.resource.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        if !self.claimed.load(Ordering::Acquire) {
            return Ok(CloseReport::default());
        }
        let termination = self.resource.shutdown(deadline).await.map_err(|source| {
            failure(
                self.component,
                Stage::Close,
                if source.kind() == std::io::ErrorKind::TimedOut {
                    Reason::Timeout
                } else {
                    Reason::Io
                },
                "plugin direct-child exit is not confirmed",
            )
            .with_source(source)
        })?;
        Ok(CloseReport {
            released_resources: match termination {
                PluginTermination::NotStarted => vec![],
                PluginTermination::Exited(_) => vec![format!(
                    "native direct child of component {}",
                    self.component.0
                )],
            },
            ..Default::default()
        })
    }
}
fn failure(
    component: ComponentId,
    stage: Stage,
    reason: Reason,
    message: &'static str,
) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component)
        .with_source(std::io::Error::other(message));
    error.component = Some(component);
    error
}

fn startup_failure(component: ComponentId, source: std::io::Error) -> ChannelError {
    if let Some(grant) = source
        .get_ref()
        .and_then(|error| error.downcast_ref::<ChannelError>())
    {
        let mut error = ChannelError::new(grant.stage, grant.reason, grant.impact);
        error.component = Some(component);
        error.delivery = grant.delivery;
        error.retry = grant.retry;
        return error.with_source(source);
    }
    let reason = match source.kind() {
        std::io::ErrorKind::AlreadyExists => Reason::StalePlan,
        std::io::ErrorKind::TimedOut => Reason::Timeout,
        _ => Reason::Io,
    };
    failure(
        component,
        Stage::Prepare,
        reason,
        "plugin resource start failed",
    )
    .with_source(source)
}
