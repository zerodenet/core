//! A unique claim binds the existing actual ledgers, not another manager.
use super::{
    drivers::{
        Admission, DriverExitReceiver, DriverKind, DriverOwner, RegistrationIter,
        RegistrationSequence,
    },
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
    registrations: RegistrationSequence,
    related_registrations: Option<RegistrationSequence>,
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
impl MetadataViewClaim {
    /// One unique native claim creates its actual wire graph and actual endpoint
    /// graph together. No arbitrary same-Scope ledger can be attached later.
    pub fn bind_packet_endpoint<O: DriverKind, B: Send + Sync + 'static>(
        self,
        registrar: TaskRegistrar,
        endpoint_drivers: NonZeroUsize,
        endpoint_users: NonZeroUsize,
    ) -> Result<(BoundMetadataUsers<O, B>, BoundMetadataUsers<O, B>), Self> {
        if !self.0.scope.matches(&registrar) || !self.0.scope.registration_open() {
            return Err(self);
        }
        let make = |claim: MetadataViewClaim, drivers: NonZeroUsize, users: NonZeroUsize| {
            let (drivers, receiver) = DriverOwner::new(registrar.clone(), drivers);
            let users = LogicalUsers::new(users, registrar.cancellation(), drivers.admission());
            (
                MetadataUserGraph(Arc::new(Graph {
                    views: claim,
                    drivers,
                    users,
                })),
                receiver,
            )
        };
        let endpoint = make(
            MetadataViewClaim(self.0.clone()),
            endpoint_drivers,
            endpoint_users,
        );
        let wire = make(
            self,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        );
        Ok((wire, endpoint))
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> MetadataUserGraph<O, B> {
    pub fn retained_views(
        &self,
        driver: zero_core::component::TaskId,
        endpoint: &Self,
        protocol: zero_core::component::TaskId,
    ) -> Option<crate::controlled_datagram::metadata::MetadataViewsRetained> {
        if !Arc::ptr_eq(&self.0.views.0, &endpoint.0.views.0) {
            return None;
        }
        let driver = self.0.drivers.live_registration(driver)?;
        let protocol = endpoint.0.drivers.live_registration(protocol)?;
        Some(
            crate::controlled_datagram::metadata::MetadataViewsRetained {
                original: self.0.views.0.clone(),
                driver,
                protocol,
            },
        )
    }
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
    /// Independent library completion keeps its original snapshot semantics.
    pub fn try_closed(&self) -> Option<MetadataUsersClosedReceipt> {
        // Cancelled admission prevents a successful acquire after this zero
        // check. Transient cancelled CAS attempts cannot return a real user.
        if !self.0.users.cancellation().is_cancelled() || self.0.users.active() != 0 {
            return None;
        }
        let registrations = self
            .0
            .drivers
            .completion_registrations(&self.0.views.0.scope)?;
        Some(MetadataUsersClosedReceipt {
            views: self.0.views.try_closed()?,
            registrations: RegistrationSequence::Library(registrations),
            related_registrations: None,
        })
    }
    /// Kernel completion references the same sealed original driver map after
    /// the real receiver transferred all output bodies. It allocates no copy
    /// of registrations and cannot retain their IO/Scope through a body.
    pub fn try_owned_closed(&self) -> Option<MetadataUsersClosedReceipt> {
        if !self.0.users.cancellation().is_cancelled() || self.0.users.active() != 0 {
            return None;
        }
        let registrations = self.0.drivers.owned_registration_completion()?;
        Some(MetadataUsersClosedReceipt {
            views: self.0.views.try_closed()?,
            registrations: RegistrationSequence::Original(registrations),
            related_registrations: None,
        })
    }
}

/// Completion of a unique, unused grant returned before native consumers exist.
/// The constructor consumes the original non-Clone RX and matching TX/claim.
/// This is distinct from retiring an endpoint/user graph and proves no FD close.
pub struct UnstartedMetadataCompletion(MetadataViewClaim);
impl UnstartedMetadataCompletion {
    pub fn try_closed(&self) -> Option<MetadataUsersClosedReceipt> {
        Some(MetadataUsersClosedReceipt {
            views: self.0.try_closed()?,
            registrations: RegistrationSequence::Empty,
            related_registrations: None,
        })
    }
}
impl crate::contract::component::GrantedMetadataListener {
    /// Return the original unique RX before it is moved into an endpoint/socket.
    /// Any outstanding TX clones/pollers still block completion. Rejection keeps
    /// the entire grant and never claims a user/task/native outcome.
    pub fn return_unstarted(mut self) -> Result<UnstartedMetadataCompletion, Self> {
        let Some(claim) = self.users_claim.take() else {
            return Err(self);
        };
        if !claim.matches(&self.rx, &self.tx) {
            self.users_claim = Some(claim);
            return Err(self);
        }
        drop(self);
        Ok(UnstartedMetadataCompletion(claim))
    }
}

#[cfg(test)]
mod tests;

impl MetadataUsersClosedReceipt {
    /// Actual original registrations behind this users receipt. Native close,
    /// logical return and local failure reception precede Host publication ACK.
    /// Already reclaimed registrations were removed only after their real ACK.
    pub fn registrations(&self) -> RegistrationIter {
        self.registrations.iter()
    }
}

impl MetadataUsersClosedReceipt {
    /// Add the actual continued endpoint ledger only after its admission is
    /// sealed and all real drivers have joined and been received. Native view
    /// closure remains this original unique claim's independent fact.
    pub fn with_related_driver_receipt(
        mut self,
        closed: super::drivers::DriverRegistrationsClosed,
    ) -> Result<Self, Self> {
        if self.related_registrations.is_some() || !self.views.0.scope.same_scope(&closed.scope) {
            return Err(self);
        }
        self.related_registrations = Some(RegistrationSequence::Original(closed));
        Ok(self)
    }
    /// Cursor over the independent original endpoint ledger. Kernel producers
    /// attach the same sealed record storage, never a copied registration slice.
    pub fn related_registrations(&self) -> RegistrationIter {
        self.related_registrations
            .as_ref()
            .unwrap_or(&RegistrationSequence::Empty)
            .iter()
    }
}

impl MetadataUsersClosedReceipt {
    /// Same original unique native view claim; read-only, not IO/ACK authority.
    pub fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.views.0, &other.views.0)
    }
}
