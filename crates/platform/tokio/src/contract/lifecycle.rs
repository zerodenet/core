use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::time::Instant;
use zero_core::component::TaskId;

use super::{BoxFuture, ChannelError, CloseReason};

pub type BuildError = ChannelError;
pub type ComponentError = ChannelError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskClass {
    Critical,
    Auxiliary,
}

#[derive(Debug)]
pub struct TaskFailure {
    pub id: TaskId,
    pub class: TaskClass,
    pub error: ComponentError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unfinished {
    Task(TaskId),
    Io(String),
    Resource(String),
    ReliableState(String),
    Recovery(String),
}

/// The two actual concrete controllers driven by one component Owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourceFailurePhase {
    ProtocolLifecycle,
    DeclaredResources,
}
/// Typed resource failure only, never a fabricated task/physical-close proof.
#[derive(Debug)]
pub struct ResourceFailure {
    pub instance: zero_core::component::ComponentInstanceId,
    pub controller: ResourceFailurePhase,
    pub source: super::RetainedError,
    original_sources: std::sync::OnceLock<Arc<OriginalProtocolFailures>>,
    reported: AtomicBool,
    ownership: std::sync::OnceLock<Arc<ChannelError>>,
}
impl ResourceFailure {
    pub fn new(
        instance: zero_core::component::ComponentInstanceId,
        controller: ResourceFailurePhase,
        source: impl Into<super::RetainedError>,
    ) -> Self {
        Self {
            instance,
            controller,
            source: source.into(),
            original_sources: std::sync::OnceLock::new(),
            reported: false.into(),
            ownership: std::sync::OnceLock::new(),
        }
    }
    pub(crate) fn with_original_sources(self, sources: Arc<OriginalProtocolFailures>) -> Self {
        assert!(self.install_original_sources(sources));
        self
    }
    /// Exact owning row only: a later Phase adds its same original ledger,
    /// never a replacement source or another resource-failure identity.
    pub(crate) fn install_original_sources(&self, sources: Arc<OriginalProtocolFailures>) -> bool {
        let actual = self.original_sources.get_or_init(|| sources.clone());
        if !Arc::ptr_eq(actual, &sources) {
            return false;
        }
        if self.ownership_received() {
            actual.accept();
        }
        true
    }
    pub fn original_sources(&self) -> Option<&Arc<OriginalProtocolFailures>> {
        self.original_sources.get()
    }
    /// Exact local ownership delivery, independent of publication, physical
    /// close, actual task join and Host registration ACK.
    pub fn ownership_received(&self) -> bool {
        self.ownership.get().is_some_and(|notice| {
            notice
                .cleanup_failure_receipt()
                .is_some_and(|receipt| receipt.is_acknowledged())
        })
    }
    /// Called only after the actual receiver durably retains this same Arc.
    /// The receipt payload retains the source, never this ResourceFailure or IO.
    pub fn acknowledge_retained(&self, retained: &Arc<Self>) -> bool {
        if !std::ptr::eq(self, retained.as_ref()) {
            return false;
        }
        if self
            .source
            .cleanup_failure_receipt()
            .is_some_and(|receipt| !receipt.acknowledge_retained(&self.source))
        {
            return false;
        }
        let notice = self.ownership.get_or_init(|| {
            let receipt = zero_error::channel::CleanupFailureReceipt::new();
            assert!(
                receipt.bind_payload(Arc::new(original_failures::SourcePayload {
                    first: self.source.clone(),
                    _original: self.original_sources.get().cloned(),
                }))
            );
            Arc::new(
                ChannelError::retained(self.source.clone()).with_cleanup_failure_receipt(receipt),
            )
        });
        let accepted = notice
            .cleanup_failure_receipt()
            .unwrap()
            .acknowledge_retained(notice);
        if accepted {
            if let Some(sources) = self.original_sources.get() {
                sources.accept();
            }
        }
        accepted
    }
    pub fn reported(&self) -> bool {
        self.reported.load(Ordering::Acquire)
    }
    pub fn confirm_reported(&self) {
        self.reported.store(true, Ordering::Release);
    }
}
/// Evidence from the owner. Cancellation, abort and Drop are not completion.
#[derive(Debug, Default)]
pub struct CloseReport {
    pub exited_tasks: Vec<TaskId>,
    pub failed_tasks: Vec<TaskFailure>,
    pub resource_failures: Vec<Arc<ResourceFailure>>,
    pub closed_io: Vec<String>,
    pub released_resources: Vec<String>,
    pub recovered_resources: Vec<String>,
    pub unfinished: Vec<Unfinished>,
}

impl CloseReport {
    pub fn is_complete(&self) -> bool {
        self.unfinished.is_empty()
    }
    pub fn append(&mut self, mut other: Self) {
        self.exited_tasks.append(&mut other.exited_tasks);
        self.failed_tasks.append(&mut other.failed_tasks);
        for failure in other.resource_failures {
            if !self
                .resource_failures
                .iter()
                .any(|same| Arc::ptr_eq(same, &failure))
            {
                self.resource_failures.push(failure);
            }
        }
        self.closed_io.append(&mut other.closed_io);
        self.released_resources
            .append(&mut other.released_resources);
        self.recovered_resources
            .append(&mut other.recovered_resources);
        self.unfinished.append(&mut other.unfinished);
    }
}

pub trait ChannelControl: Send {
    /// Observe original logical retirement only. Default controllers declare no
    /// such event; this is never native close, driver join or a release receipt.
    fn revoked(&self) -> BoxFuture<'static, ()> {
        Box::pin(std::future::pending())
    }

    /// Attempt only immediately available cleanup on this same controller.
    /// Pending work remains owned; no timer or completion evidence is invented.
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        Ok(CloseReport {
            unfinished: vec![Unfinished::Resource(
                "channel cleanup remains pending".into(),
            )],
            ..Default::default()
        })
    }

    /// Idempotent, nonblocking logical-session cancellation. Shared endpoints
    /// remain owned by ResourceLifecycle; this must not close another session.
    fn cancel(&self, reason: CloseReason);
    /// Report actual completion. A deadline cannot turn unfinished I/O into a
    /// successful close, nor release the owner's cleanup responsibility.
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> impl Future<Output = Result<CloseReport, ChannelError>> + Send;
}

pub trait DynChannelControl: Send {
    /// Observe original logical retirement only. Default controllers declare no
    /// such event; this is never native close, driver join or a release receipt.
    fn revoked(&self) -> BoxFuture<'static, ()> {
        Box::pin(std::future::pending())
    }

    /// Attempt only immediately available cleanup on this same controller.
    /// Pending work remains owned; no timer or completion evidence is invented.
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        Ok(CloseReport {
            unfinished: vec![Unfinished::Resource(
                "channel cleanup remains pending".into(),
            )],
            ..Default::default()
        })
    }

    fn cancel(&self, reason: CloseReason);
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>>;
}

mod control_owner;
pub use control_owner::BoxChannelControl;

struct ErasedControl<T>(T);
impl<T: ChannelControl> DynChannelControl for ErasedControl<T> {
    fn revoked(&self) -> BoxFuture<'static, ()> {
        self.0.revoked()
    }

    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.0.try_close()
    }

    fn cancel(&self, reason: CloseReason) {
        self.0.cancel(reason);
    }
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.wait_closed(deadline))
    }
}
pub fn erase_control(control: impl ChannelControl + 'static) -> BoxChannelControl {
    Box::new(ErasedControl(control)).into()
}

pub trait ResourceLifecycle: Send {
    fn start(&mut self, deadline: Instant) -> impl Future<Output = Result<(), BuildError>> + Send;
    /// Synchronously reject new admissions; shutdown still has to confirm exit.
    fn revoke(&self, reason: CloseReason);
    fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> impl Future<Output = Result<CloseReport, BuildError>> + Send;
}

pub trait DynResourceLifecycle: Send {
    fn start(&mut self, deadline: Instant) -> BoxFuture<'_, Result<(), BuildError>>;
    fn revoke(&self, reason: CloseReason);
    fn shutdown(&mut self, deadline: Instant) -> BoxFuture<'_, Result<CloseReport, BuildError>>;
}

struct ErasedLifecycle<T>(T);
impl<T: ResourceLifecycle> DynResourceLifecycle for ErasedLifecycle<T> {
    fn start(&mut self, deadline: Instant) -> BoxFuture<'_, Result<(), BuildError>> {
        Box::pin(self.0.start(deadline))
    }
    fn revoke(&self, reason: CloseReason) {
        self.0.revoke(reason);
    }
    fn shutdown(&mut self, deadline: Instant) -> BoxFuture<'_, Result<CloseReport, BuildError>> {
        Box::pin(self.0.shutdown(deadline))
    }
}
pub fn erase_lifecycle(owner: impl ResourceLifecycle + 'static) -> Box<dyn DynResourceLifecycle> {
    Box::new(ErasedLifecycle(owner))
}

mod cleanup_failures;
pub use cleanup_failures::{CleanupFailurePhase, ResourceCleanupFailures};

mod original_failures;
pub use original_failures::OriginalProtocolFailures;

mod declared_sources;
pub use declared_sources::DeclaredResourceSources;
