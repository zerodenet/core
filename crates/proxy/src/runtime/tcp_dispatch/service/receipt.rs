//! The completed chain reports exactly the nodes that created its real I/O.
use super::*;
use crate::component::ServiceAdmission;
use tokio::sync::watch;
use zero_core::component::RoleInstanceId;

pub(crate) struct ServiceConnected {
    pub(crate) stream: TcpRelayStream,
    pub(crate) binding: ServiceBinding,
}
#[derive(Debug)]
struct Hop {
    tag: Option<String>,
    role: RoleInstanceId,
    endpoint: Option<Destination>,
    dial: zero_traits::DialPolicy,
    admission: ServiceAdmission,
}
pub(crate) struct ServiceBinding {
    hops: Vec<Hop>,
    destination: Destination,
    network: NetworkGeneration,
    updates: watch::Receiver<u64>,
}
impl std::fmt::Debug for ServiceBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceBinding")
            .field(
                "roles",
                &self.hops.iter().map(|hop| hop.role).collect::<Vec<_>>(),
            )
            .field(
                "configured_endpoints",
                &self
                    .hops
                    .iter()
                    .filter(|hop| hop.endpoint.is_some())
                    .count(),
            )
            .field(
                "nondefault_dials",
                &self
                    .hops
                    .iter()
                    .filter(|hop| hop.dial != Default::default())
                    .count(),
            )
            .field("destination_port", &self.destination.port)
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}
impl ServiceBinding {
    pub(super) fn new(
        nodes: &[ServiceNode],
        destination: Destination,
        network: NetworkGeneration,
        updates: watch::Receiver<u64>,
    ) -> Result<Self, ChannelError> {
        let hops = nodes
            .iter()
            .map(|node| {
                Ok(Hop {
                    tag: node.tag.clone(),
                    role: node.slot.role_id(),
                    endpoint: node.endpoint.clone(),
                    dial: node.dial.clone(),
                    admission: node
                        .slot
                        .service_admission()
                        .ok_or_else(|| failure(Reason::StalePlan))?,
                })
            })
            .collect::<Result<_, ChannelError>>()?;
        Ok(Self {
            hops,
            destination,
            network,
            updates,
        })
    }
    pub(crate) fn tags(&self) -> Vec<Option<&str>> {
        self.hops.iter().map(|hop| hop.tag.as_deref()).collect()
    }
    pub(crate) fn check_current(&self) -> Result<(), ChannelError> {
        if *self.updates.borrow() != self.network.0 {
            return Err(failure(Reason::StalePlan));
        }
        for hop in &self.hops {
            hop.admission.check_current()?;
        }
        Ok(())
    }
    pub(crate) async fn revoked(&self) -> ChannelError {
        use futures_util::{stream::FuturesUnordered, StreamExt};
        let mut revoked: FuturesUnordered<_> = self
            .hops
            .iter()
            .map(|hop| hop.admission.revoked())
            .collect();
        let mut updates = self.updates.clone();
        loop {
            if let Err(error) = self.check_current() {
                return error;
            }
            tokio::select! {
                Some(error) = revoked.next() => return error,
                changed = updates.changed() => if changed.is_err() {return failure(Reason::Revoked);},
            }
        }
    }
}
