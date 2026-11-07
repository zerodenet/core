//! The one actual Host root and original caller, before native acquisition.
use super::*;
impl HostBoundDatagramResources {
    pub(in crate::component) fn new_scoped_with_totals(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        declaration: &ResourceDeclaration,
        cancel: CancellationToken,
        totals: Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
    ) -> Result<Arc<Self>, ChannelError> {
        Self::construct(
            instance,
            None,
            Some(scope),
            declaration,
            cancel,
            Some(totals),
        )
    }
    /// The ordinary listener supplies its exact already-registered peer caller.
    /// This does not turn a shared inline peer into an internal wire driver.
    pub fn new_for_peer_with_totals(
        instance: ComponentInstanceId,
        peer: &TaskJoinWitness,
        declaration: &ResourceDeclaration,
        cancel: CancellationToken,
        totals: Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
    ) -> Result<Arc<Self>, ChannelError> {
        let task = peer.task_id().ok_or_else(|| {
            error(
                instance,
                Reason::InvalidConfig,
                "peer has no actual task registration",
            )
        })?;
        let provider = Self::construct(
            instance,
            Some(task),
            None,
            declaration,
            cancel,
            Some(totals),
        )?;
        let original =
            zero_platform_tokio::controlled_datagram::BoundDatagramTotalsBinding::prepare(
                provider
                    .totals
                    .as_ref()
                    .expect("installed actual Host root"),
                instance,
                peer,
                &provider.cancel,
            )
            .map_err(|source| {
                error(
                    instance,
                    Reason::StalePlan,
                    "peer original Scope was stopped",
                )
                .with_source(source)
            })?;
        provider
            .state()
            .owner
            .bind_bound_totals(original)
            .map_err(|source| {
                error(instance, Reason::StalePlan, "peer original root differs").with_source(source)
            })?;
        Ok(provider)
    }
}
