use super::*;
/// Facts of this installed owning factory. These values do not grant IO,
/// register a task or replace the actual carrier's sealed receiver.
pub struct OwnedXhttpContext<'a> {
    pub instance: zero_core::component::ComponentInstanceId,
    pub scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    pub binding: &'a zero_platform_tokio::contract::network::UpstreamBinding,
    pub memory: zero_platform_tokio::contract::component::PacketDriverMetadataView,
    pub buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
}
impl XhttpClientPool {
    /// Slot retirement removes only this actual factory's groups. Existing
    /// users/driver/Native/Host ACK still belong to their original Root rows.
    pub fn retire_owned(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) {
        loop {
            let removed = {
                let mut state = self.0.state.lock().unwrap();
                let index = state
                    .groups
                    .iter()
                    .position(|group| group.same_factory(instance, scope));
                index.map(|index| state.groups.remove(index))
            };
            match removed {
                Some(group) => drop(group),
                None => break,
            }
        }
    }
    async fn return_old_idle_originals(&self, owner: &OwnerKey) -> io::Result<()> {
        loop {
            let removed = {
                let mut state = self.0.state.lock().unwrap();
                let index = state.groups.iter().position(|group| {
                    group.same_factory(owner.instance, &owner.scope)
                        && !group.matches_owner(Some(owner))
                        && group.active() == 0
                        && !group.has_responses()
                });
                index.map(|index| state.groups.remove(index))
            };
            let Some(group) = removed else { return Ok(()) };
            // H1 may retain several real originals in this same group.
            while let Some(original) = group.connections.retire_owned().await {
                original
                    .wait_settled_original()
                    .await
                    .map_err(io::Error::other)?;
            }
            drop(group);
        }
    }
    /// Existing pure-H3 entry keeps the same public factory shape. Mixed
    /// callers select concrete halves first and install all missing actual
    /// Root originals before they finish this selected operation.
    pub async fn connect_owned<P: SplitHttpTransportProfile + ?Sized>(
        &self,
        factory: XhttpCarrierFactory,
        config: &P,
        download: Option<XhttpDownload<'_>>,
        context: OwnedXhttpContext<'_>,
    ) -> Result<
        (
            XhttpStream,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        crate::RuntimeError,
    > {
        self.prepare_owned(
            factory.into(),
            config,
            download.map(|download| OwnedXhttpDownload {
                pool: download.pool,
                factory: download.factory.into(),
                config: download.config,
            }),
            context,
            OwnedConnectionKinds::multiplexed(),
        )
        .await?
        .finish()
        .await
    }
}
mod selection;
pub use selection::{
    OwnedConnectionKind, OwnedConnectionKinds, OwnedXhttpDownload, PreparedOwnedXhttp,
};
