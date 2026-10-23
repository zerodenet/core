//! A unique claim binds the existing actual ledgers, not another manager.
use super::{
    drivers::{Admission, DriverExitReceiver, DriverKind, DriverOwner},
    LogicalUsers,
};
use crate::{
    contract::scope::TaskRegistrar,
    controlled_datagram::metadata::{MetadataViewClaim, MetadataViewsClosedReceipt},
};
use std::{fmt, num::NonZeroUsize, sync::Arc};

struct Graph<O: DriverKind, B: Send + Sync + 'static> {
    views: MetadataViewClaim,
    drivers: DriverOwner<O, B>,
    users: Arc<LogicalUsers<Admission<O, B>>>,
}
/// Owns only the original ledgers used by actual hooks and logical permits.
/// There is no second count, task map, runtime, queue, or native owner here.
pub struct MetadataUserGraph<O: DriverKind, B: Send + Sync + 'static>(Arc<Graph<O, B>>);
/// Pending observation over those same original ledgers; cloning it creates no
/// permission to edit a count, receive a task, or manufacture an exit.
pub struct MetadataUsersCompletion<O: DriverKind, B: Send + Sync + 'static>(Arc<Graph<O, B>>);
/// Opaque historical proof of released logical guards, exact actual joins and
/// explicit reception, and release of every original native I/O view. This is
/// neither an FD-close receipt nor acknowledgement of host publication credit.
#[derive(Clone)]
pub struct MetadataUsersClosedReceipt {
    pub(in crate::controlled_datagram::metadata) views: MetadataViewsClosedReceipt,
}
impl fmt::Debug for MetadataUsersClosedReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataUsersClosedReceipt")
            .finish_non_exhaustive()
    }
}
/// The original graph and its unique actual driver-outcome receiver.
pub type BoundMetadataUsers<O, B> = (MetadataUserGraph<O, B>, DriverExitReceiver<O, B>);

impl MetadataViewClaim {
    /// Consume the original unique claim before any endpoint/driver is built.
    /// A foreign/closed scope returns the unconsumed original capability.
    pub fn bind_users<O: DriverKind, B: Send + Sync + 'static>(
        self,
        registrar: TaskRegistrar,
        max_drivers: NonZeroUsize,
        max_users: NonZeroUsize,
    ) -> Result<BoundMetadataUsers<O, B>, Self> {
        if !self.0.scope.matches(&registrar) || !self.0.scope.registration_open() {
            return Err(self);
        }
        let cancel = registrar.cancellation();
        let (drivers, receiver) = DriverOwner::new(registrar, max_drivers);
        let users = LogicalUsers::new(max_users, cancel, drivers.admission());
        Ok((
            MetadataUserGraph(Arc::new(Graph {
                views: self,
                drivers,
                users,
            })),
            receiver,
        ))
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> MetadataUserGraph<O, B> {
    pub fn drivers(&self) -> &DriverOwner<O, B> {
        &self.0.drivers
    }
    pub fn users(&self) -> &Arc<LogicalUsers<Admission<O, B>>> {
        &self.0.users
    }
    pub fn completion(&self) -> MetadataUsersCompletion<O, B> {
        MetadataUsersCompletion(self.0.clone())
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> Clone for MetadataUsersCompletion<O, B> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> MetadataUsersCompletion<O, B> {
    pub fn try_closed(&self) -> Option<MetadataUsersClosedReceipt> {
        // Cancelled admission prevents a successful acquire after this zero
        // check. Transient cancelled CAS attempts cannot return a real user.
        if !self.0.users.cancellation().is_cancelled() || self.0.users.active() != 0 {
            return None;
        }
        if !self.0.drivers.completion_matches(&self.0.views.0.scope) {
            return None;
        }
        Some(MetadataUsersClosedReceipt {
            views: self.0.views.try_closed()?,
        })
    }
}

#[cfg(test)]
mod tests;
