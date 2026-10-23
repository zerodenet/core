//! Opaque users closure uses the very ledgers used by sealed Quinn handles.
use super::*;
use zero_platform_tokio::controlled_datagram::metadata::{
    users::{MetadataUserGraph, MetadataUsersCompletion as Completion},
    MetadataViewClaim,
};

pub(super) type MetadataGraph =
    MetadataUserGraph<super::super::OwnedDriverOrigin, super::super::DriverBody>;
pub type MetadataUsersCompletion =
    Completion<super::super::OwnedDriverOrigin, super::super::DriverBody>;
/// Refusal returns the exact unconsumed claim and original hosted views.
/// No native original or accepted task is dropped or reconstructed here.
pub struct RejectedClaimedEndpoint {
    pub native: super::super::HostedMetadataSocket,
    pub claim: MetadataViewClaim,
}
impl OwnedEndpointOwner {
    /// Cold explicit selection. The unique claim must match this exact native
    /// grant and existing host registrar before any actual Quinn task starts.
    /// The original unclaimed constructor remains diagnostic-only.
    pub fn new_hosted_claimed(
        native: super::super::HostedMetadataSocket,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
        claim: MetadataViewClaim,
    ) -> Result<(Self, DriverExitReceiver), Box<RejectedClaimedEndpoint>> {
        if !native.matches(&registrar) || !native.matches_claim(&claim) {
            return Err(Box::new(RejectedClaimedEndpoint { native, claim }));
        }
        let (graph, receiver) = match claim.bind_users(registrar, limits.drivers, limits.users) {
            Ok(value) => value,
            Err(claim) => return Err(Box::new(RejectedClaimedEndpoint { native, claim })),
        };
        let drivers = QuinnDriverOwner::from_ledger(graph.drivers().clone());
        let users = graph.users().clone();
        Ok(Self::construct_users(
            Native::Hosted(native),
            drivers,
            users,
            receiver,
            Some(graph),
        ))
    }
    /// Observe the same actual graph after shutdown and native-view release.
    /// This cannot mark a task received, edit user counts, or close native I/O.
    pub fn users_completion(&self) -> Option<MetadataUsersCompletion> {
        self.users_graph.as_ref().map(MetadataGraph::completion)
    }
}
