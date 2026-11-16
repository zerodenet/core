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

impl OwnedEndpointOwner {
    pub fn borrowed_users_completion(
        &self,
    ) -> Option<
        zero_platform_tokio::controlled_carrier::loan::CarrierEndpointUsersCompletion<
            super::super::OwnedDriverOrigin,
            super::super::DriverBody,
        >,
    > {
        self.carrier_graph.as_ref().map(|graph| graph.completion())
    }
}

impl OwnedEndpointOwner {
    /// Sealed native Source lineage from its actual endpoint driver and unique
    /// Metadata user graph, including the existing mask stack in that Scope.
    pub fn source_carrier_origin(
        &self,
    ) -> Result<Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>, ChannelError> {
        let Native::Hosted(_) = &self.native else {
            return Ok(None);
        };
        let graph = self
            .users_graph
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let driver = self
            .drivers
            .tasks()
            .into_iter()
            .find(|id| {
                matches!(
                    self.drivers.origin(*id),
                    Some(super::super::OwnedDriverOrigin::Quinn(
                        quinn::DriverOrigin::Endpoint
                    ))
                )
            })
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let retained = graph
            .retained_views(driver, graph, driver)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        retained
            .carrier_origin()
            .map_err(|source| error(Stage::Prepare, Reason::StalePlan).with_source(source))
    }
}
