use std::future::Future;
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

/// Evidence from the owner. Cancellation, abort and Drop are not completion.
#[derive(Debug, Default)]
pub struct CloseReport {
    pub exited_tasks: Vec<TaskId>,
    pub failed_tasks: Vec<TaskFailure>,
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
        self.closed_io.append(&mut other.closed_io);
        self.released_resources
            .append(&mut other.released_resources);
        self.recovered_resources
            .append(&mut other.recovered_resources);
        self.unfinished.append(&mut other.unfinished);
    }
}

pub trait ChannelControl: Send {
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
    fn cancel(&self, reason: CloseReason);
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>>;
}

struct ErasedControl<T>(T);
impl<T: ChannelControl> DynChannelControl for ErasedControl<T> {
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
pub fn erase_control(control: impl ChannelControl + 'static) -> Box<dyn DynChannelControl> {
    Box::new(ErasedControl(control))
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
