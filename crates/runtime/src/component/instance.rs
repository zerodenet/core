//! Ownership of one cold recipe, installed before its first start.
use super::{ComponentRecipe, ValidatedRecipe};
mod business;
mod cleanup;
mod resources;
mod roles;
mod start;
mod tasks;
pub(in crate::component) mod users;
#[cfg(any(
    feature = "datagram-route-runtime",
    feature = "managed-datagram-runtime"
))]
pub use resources::HostDatagramListenerOwner;
pub use resources::HostOutboundDatagramResources;
pub use resources::HostResources;
#[cfg(any(test, feature = "test-support"))]
pub use resources::NoResources;
#[cfg(feature = "raw-ip-runtime")]
pub use resources::{HostIcmpProbeGrant, HostIcmpProbeOwner};
use std::sync::{Arc, OnceLock};
pub(in crate::component) use tasks::wake::WaitTarget;
pub use tasks::ComponentTaskFault;
use tasks::TaskMonitor;
use tokio::{
    sync::{Mutex, OnceCell},
    time::Instant,
};
use zero_core::component::*;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentInstance},
    datagram::DynDatagramOutbound,
    lifecycle::CloseReport,
    packet::DynPacketOutbound,
    scope::TaskScope,
    stream::DynStreamOutbound,
    CancellationToken, ChannelError, CloseReason, DeliveryState, Impact, Reason, Stage,
};

pub type SharedError = Arc<ChannelError>;
pub fn invalid(reason: Reason, message: impl Into<String>) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(std::io::Error::other(message.into()));
    error.delivery = DeliveryState::NotSubmitted;
    error
}
pub fn retained(error: SharedError) -> ChannelError {
    let mut retained = ChannelError::new(error.stage, error.reason, error.impact);
    retained.component = error.component;
    retained.session = error.session;
    retained.delivery = error.delivery;
    retained.retry = error.retry;
    retained.with_source(error)
}

pub struct ComponentSlot {
    pub id: ComponentId,
    pub generation: ResourceGeneration,
    pub recipe: ComponentRecipe,
    dependencies: Arc<[ComponentInstanceId]>,
    legacy_connect_stage: &'static str,
    pub dial: zero_traits::DialPolicy,
    cancel: CancellationToken,
    users: Arc<users::Users>,
    user_roots: OnceLock<Arc<[Arc<users::Users>]>>,
    admission: OnceLock<Arc<super::admission::HostAdmission>>,
    admission_owner: Arc<()>,
    live: OnceCell<Result<Live, SharedError>>,
    start_gate: Mutex<()>,
}
struct Live {
    packet: Option<Arc<dyn DynPacketOutbound>>,
    stream: Option<Arc<dyn DynStreamOutbound>>,
    datagram: Option<Arc<dyn DynDatagramOutbound>>,
    started: OnceLock<Result<(), SharedError>>,
    owner: Mutex<Owner>,
    tasks: TaskMonitor,
    resources: Arc<HostResources>,
}
struct Owner {
    instance: ComponentInstance,
    pending_close: Option<Box<PendingClose>>,
    protocol_closed: bool,
}
#[derive(Default)]
struct PendingClose {
    report: CloseReport,
    io: CloseReport,
}

// Cancels admission when the initiating future is dropped. The actual owner
// remains installed for explicit, confirmed cleanup by the host.
struct StartCancellation<'a> {
    slot: &'a ComponentSlot,
    armed: bool,
}
impl Drop for StartCancellation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.slot.revoke();
        }
    }
}

impl ComponentSlot {
    /// Observe the installed original for one declared ordinal. This weak
    /// identity grants no acquisition, cleanup or task authority; absence is
    /// unavailable evidence, never a successful close. Its opaque receipts
    /// remain independently matchable after the host retires this slot.
    pub fn original_carrier_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration> {
        self.live
            .get()?
            .as_ref()
            .ok()?
            .resources
            .original_carrier_owner(ordinal)
    }
    pub fn original_metadata_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolRegistration>
    {
        self.live
            .get()?
            .as_ref()
            .ok()?
            .resources
            .original_metadata_owner(ordinal)
    }
    pub fn new(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ValidatedRecipe,
        dial: zero_traits::DialPolicy,
    ) -> Self {
        Self::from_recipe(
            id,
            generation,
            recipe.component_recipe(),
            dial,
            recipe.legacy_connect_stage(),
        )
    }
    pub fn from_recipe(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ComponentRecipe,
        dial: zero_traits::DialPolicy,
        legacy_connect_stage: &'static str,
    ) -> Self {
        Self::from_bound_recipe(
            id,
            generation,
            recipe,
            dial,
            legacy_connect_stage,
            Arc::from([]),
        )
    }
    pub fn from_bound_recipe(
        id: ComponentId,
        generation: ResourceGeneration,
        recipe: ComponentRecipe,
        dial: zero_traits::DialPolicy,
        legacy_connect_stage: &'static str,
        dependencies: Arc<[ComponentInstanceId]>,
    ) -> Self {
        Self {
            id,
            generation,
            recipe,
            dependencies,
            legacy_connect_stage,
            dial,
            cancel: CancellationToken::new(),
            users: Arc::new(users::Users::new(id)),
            user_roots: OnceLock::new(),
            admission: OnceLock::new(),
            admission_owner: Arc::new(()),
            live: OnceCell::new(),
            start_gate: Mutex::new(()),
        }
    }
    /// Cold recipes remain reusable; revoked or failed runtime owners do not.
    /// This observes the actual persistent initializer, not a synthetic health flag.
    pub(in crate::component) fn can_reuse(&self) -> bool {
        !self.cancel.is_cancelled()
            && match self.live.get() {
                None => true,
                Some(Err(_)) => false,
                Some(Ok(live)) => !matches!(live.started.get(), Some(Err(_))),
            }
    }
    pub(in crate::component) fn matches_scope_identity(
        &self,
        expected: Option<&zero_platform_tokio::contract::scope::TaskScopeIdentity>,
    ) -> bool {
        match (self.live.get(), expected) {
            (None, None) => true,
            (Some(Ok(live)), Some(expected)) => live.tasks.scope_identity().same_scope(expected),
            _ => false,
        }
    }
    /// Actual successful lifecycle start, never a cold spec or a close proof.
    pub(in crate::component) fn has_started(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.task_fault().is_none()
            && matches!(self.live.get(), Some(Ok(live)) if matches!(live.started.get(), Some(Ok(()))))
    }
    pub fn instance_id(&self) -> ComponentInstanceId {
        ComponentInstanceId {
            component: self.id,
            generation: self.generation,
        }
    }
    pub(in crate::component) fn own_user_root(&self) -> Arc<users::Users> {
        self.users.clone()
    }
    pub(in crate::component) fn bind_user_retirement(
        &self,
        own: bool,
        roots: Arc<[Arc<users::Users>]>,
    ) {
        self.users.bind(own);
        if let Err(roots) = self.user_roots.set(roots) {
            let installed = self.user_roots.get().unwrap();
            assert_eq!(
                installed.len(),
                roots.len(),
                "one resource census per generation"
            );
            assert!(
                installed
                    .iter()
                    .zip(roots.iter())
                    .all(|(a, b)| Arc::ptr_eq(a, b)),
                "one resource census per generation"
            );
        }
    }
    pub fn dependency_instances(&self) -> &[ComponentInstanceId] {
        &self.dependencies
    }
    pub fn role_id(&self) -> RoleInstanceId {
        RoleInstanceId {
            component: self.id,
            role: RoleId::Outbound(OutboundId(self.id.0)),
            generation: self.generation,
        }
    }
    fn failure(&self, reason: Reason, message: &'static str) -> ChannelError {
        let mut error = invalid(reason, message);
        error.component = Some(self.id);
        error
    }
    pub async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let _starting = tokio::time::timeout_at(deadline, self.start_gate.lock())
            .await
            .map_err(|_| {
                let mut error = self.failure(Reason::Timeout, "component cleanup deadline elapsed");
                error.stage = Stage::Close;
                error
            })?;
        match self.live.get() {
            Some(Ok(live)) => {
                let mut owner = live.owner.lock().await;
                owner
                    .shutdown(
                        self.id,
                        deadline,
                        &live.resources,
                        &live.tasks,
                        Some(&self.users),
                    )
                    .await
            }
            Some(Err(_)) | None => Ok(CloseReport::default()),
        }
    }
    pub fn revoke(&self) {
        self.cancel.cancel();
        self.users.revoke();
        if let Some(Ok(live)) = self.live.get() {
            live.tasks.begin_shutdown();
        }
        if let Some(host) = self.admission.get() {
            host.revoke(self.instance_id(), &self.admission_owner);
        }
    }
}

#[cfg(test)]
#[path = "tests/lifecycle_scope.rs"]
mod lifecycle_scope_tests;

fn started(component: ComponentId, live: &Result<Live, SharedError>) -> Result<(), ChannelError> {
    let result = match live {
        Ok(live) => match live.started.get() {
            Some(Err(error)) => Err(retained(error.clone())),
            Some(Ok(())) => Ok(()),
            None => Err(invalid(Reason::Revoked, "component start is not complete")),
        },
        Err(error) => Err(retained(error.clone())),
    };
    result.map_err(|mut error| {
        error.component.get_or_insert(component);
        error
    })
}
