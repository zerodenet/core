//! The actual Source's paid pure client-fault receiver. It owns no native IO,
//! task, registrar, Source/controller, policy setter or background worker.
use crate::contract::{
    component::{PacketDriverMetadataView, PacketMetadataVec},
    scope::{TaskJoinWitness, TaskJoined, TaskRegistrar, TaskScopeIdentity},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};
use std::sync::Mutex;
use zero_core::component::ComponentInstanceId;
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataArc, PacketMetadataLayout};
mod cursor;
mod liability;
mod plan;
mod receive;
mod rows;
mod success;
mod wait;
pub use cursor::{AcceptedTaskFailureCursor, AcceptedTaskFailureView};
pub use rows::{AcceptedTaskFailureRegistration, AcceptedTaskFailureReservation};
#[derive(Clone)]
pub struct AcceptedTaskFailures(PacketMetadataArc<Original>);
struct Original {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    memory: PacketDriverMetadataView,
    rows: Mutex<rows::Rows>,
    changed: tokio::sync::Notify,
}
impl AcceptedTaskFailures {
    pub fn prepare(
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
        memory: &PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        let _notifications = memory.notifications();
        let book_layout = PacketMetadataArc::<Original>::layout().map_err(capacity)?;
        let header_layout = liability::HeaderLiability::layout().map_err(capacity)?;
        let table_layout = PacketMetadataLayout::array::<rows::Cell>(1).map_err(capacity)?;
        let total = book_layout
            .checked_add(header_layout)
            .and_then(|layout| layout.checked_add(table_layout))
            .map_err(capacity)?;
        let mut credit = memory.reserve(total).map_err(capacity)?;
        let table_credit = credit.split(table_layout).map_err(capacity)?;
        let headers = liability::HeaderLiability::from_prepaid(
            credit.split(header_layout).map_err(capacity)?,
        )?;
        let mut rows = rows::Rows::default();
        rows.values
            .grow_prepaid(table_credit, usize::MAX)
            .map_err(|(error, _)| capacity(error))?;
        rows.values
            .push_reserved(rows::Cell::free(Some(headers)))
            .map_err(|_| capacity(PacketMemoryError::Capacity))?;
        let actual = PacketMetadataArc::from_prepaid(
            Original {
                instance,
                scope: tasks.scope_identity(),
                memory: memory.clone(),
                rows: Mutex::new(rows),
                changed: tokio::sync::Notify::new(),
            },
            credit,
        )
        .map_err(|(error, _, _)| capacity(error))?;
        Ok(Self(actual))
    }
    pub fn matches(
        &self,
        instance: ComponentInstanceId,
        scope: &TaskScopeIdentity,
        memory: &PacketDriverMetadataView,
    ) -> bool {
        self.0.instance == instance
            && self.0.scope.same_scope(scope)
            && self.0.memory.same_root(memory)
    }
    pub fn scope_identity(&self) -> TaskScopeIdentity {
        self.0.scope.clone()
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.instance
    }
    pub fn same_receiver(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
    pub fn iter(&self) -> AcceptedTaskFailureCursor {
        let end = self.0.rows.lock().unwrap().values.len();
        AcceptedTaskFailureCursor {
            receiver: self.clone(),
            next: 0,
            end,
        }
    }
    pub fn len(&self) -> usize {
        self.iter().count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl std::fmt::Debug for AcceptedTaskFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcceptedTaskFailures")
            .field("instance", &self.0.instance)
            .field("received", &self.len())
            .finish_non_exhaustive()
    }
}
fn capacity(source: PacketMemoryError) -> ChannelError {
    let reason = match source {
        PacketMemoryError::Stopped => Reason::Revoked,
        PacketMemoryError::Overflow => Reason::Internal,
        _ => Reason::Capacity,
    };
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
fn foreign() -> ChannelError {
    let mut error = ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component);
    error.delivery = DeliveryState::Unknown;
    error
}

#[cfg(test)]
mod tests;
