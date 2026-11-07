//! Ownership of one cold recipe, installed before its first start.
use super::{ComponentRecipe, ValidatedRecipe};
mod business;
mod cleanup;
mod construction;
mod health;
mod node_dns;
mod observations;
mod original_budgets;
mod owned_start;
mod packet_source;
mod probe;
#[cfg(any(target_os = "linux", windows))]
mod process_endpoint;
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
pub use resources::{
    packet_datagram_control_spec, packet_datagram_read_spec, HostPacketReadReservation,
};
#[cfg(feature = "raw-ip-runtime")]
pub use resources::{HostIcmpProbeGrant, HostIcmpProbeOwner};
use std::sync::{Arc, OnceLock};
pub(crate) use tasks::wake::{MonitorWake, WaitTarget};
pub use tasks::ComponentTaskFault;
use tasks::TaskMonitor;
use tokio::{
    sync::{Mutex, OnceCell},
    time::Instant,
};
pub(in crate::component) use users::PausedUsers;
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
    ChannelError::retained(error)
}

pub struct ComponentSlot {
    pub id: ComponentId,
    pub generation: ResourceGeneration,
    pub recipe: ComponentRecipe,
    dependencies: Arc<[ComponentInstanceId]>,
    dependency_slots: OnceLock<Arc<[std::sync::Weak<ComponentSlot>]>>,
    legacy_connect_stage: &'static str,
    pub dial: zero_traits::DialPolicy,
    cancel: CancellationToken,
    users: Arc<users::Users>,
    user_roots: OnceLock<Arc<[Arc<users::Users>]>>,
    admission: OnceLock<Arc<super::admission::HostAdmission>>,
    admission_owner: Arc<()>,
    live: OnceCell<Result<Live, SharedError>>,
    start_gate: Mutex<()>,
    owned_start_gate: std::sync::Mutex<()>,
    #[cfg(any(target_os = "linux", windows))]
    prepaid_process: OnceLock<Arc<zero_platform_tokio::owned_process::PrepaidProcessJob>>,
    owned_start: OnceLock<owned_start::OwnedStart>,
    owned_start_host: OnceLock<std::sync::Mutex<Option<owned_start::OwnedStart>>>,
    start_wake: Arc<MonitorWake>,
    declared_failures: Arc<resources::DeclaredReceiver>,
    originals: OnceLock<Arc<crate::HostOriginalBudgets>>,
    node_dns_source:
        OnceLock<Arc<OnceLock<Arc<dyn zero_platform_tokio::contract::dns::DnsService>>>>,
    packet_source: Arc<OnceLock<zero_platform_tokio::controlled_carrier::CarrierOrigin>>,
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
    resource_failures:
        [std::sync::OnceLock<Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>>; 2],
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
        self.finish_owned_start(deadline).await?;
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
                        self.instance_id(),
                        deadline,
                        &live.resources,
                        &live.tasks,
                        Some(&self.users),
                    )
                    .await
            }
            Some(Err(_)) | None => {
                let mut report = CloseReport::default();
                if let Some(fault) = self.observe_resource_fault() {
                    report.resource_failures.push(fault);
                }
                if self
                    .originals
                    .get()
                    .is_some_and(|root| root.carrier.retained_for_instance(self.instance_id()) != 0)
                {
                    report.unfinished.push(
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "actual root still retains originals for this unbuilt/failed Slot"
                                .into(),
                        ),
                    );
                }
                Ok(report)
            }
        }
    }
    pub fn revoke(&self) {
        self.cancel.cancel();
        #[cfg(any(target_os = "linux", windows))]
        if let Some(paid) = self.prepaid_process.get() {
            paid.request_stop();
        }
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

impl ComponentSlot {
    /// Exact live original Scope observation for prepared resource reuse. This
    /// grants neither I/O nor closure and never builds a candidate instance.
    pub fn matches_live_scope(
        &self,
        actual: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) -> bool {
        self.has_started()
            && !self.cancel.is_cancelled()
            && actual.resource_open()
            && self
                .live
                .get()
                .and_then(|live| live.as_ref().ok())
                .is_some_and(|live| live.tasks.scope_identity().same_scope(actual))
    }
}

/// The existing concrete one-job/derived-endpoint owner, not a generic lazy
/// Process policy or native authority. Other descriptions keep their gate.
pub(in crate::component) fn demand_process_endpoint_ordinal(
    spec: &zero_platform_tokio::contract::component::ComponentSpec,
) -> Option<usize> {
    use zero_platform_tokio::contract::component::ResourceDeclaration;
    if !spec.dependencies.is_empty()
        || !spec.roles.is_empty()
        || spec.task_limit.map(|limit| limit.get()) != Some(1)
    {
        return None;
    }
    // Only the existing finite Process + its matching Endpoint may defer
    // activation. Other resourceful roleless components retain their
    // normal start order; this is not a generic lazy-resource policy.
    match spec.resources.as_slice() {
        [ResourceDeclaration::Process { max_starts, .. }, ResourceDeclaration::ProcessEndpoint {
            process_ordinal: 0, ..
        }] if max_starts.get() == 1 => Some(0),
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/resource_fault_supervision.rs"]
mod resource_fault_supervision_tests;
