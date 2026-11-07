//! Publish the actual started linked graph into the existing endpoint owner.
use super::*;
impl WireguardAdapter {
    pub(in crate::adapters::wireguard::lifecycle) async fn install_component_link(
        &self,
        candidate: &Arc<PendingEntries>,
        tag: &str,
        outbound: &OutboundConfig,
        staged: &mut crate::runtime::raw_ip::StagedRawIpDevices,
        identity: [u8; 32],
        generation: u64,
    ) -> Result<Arc<inbound::LinkedEndpoint>, EngineError> {
        let pending = candidate
            .get(tag)
            .ok_or_else(|| invalid("actual deferred linked peer originals are missing"))?;
        let link = candidate
            .invocations
            .lock()
            .await
            .get_mut(tag)
            .ok_or_else(|| invalid("actual linked invocation was not retained"))?
            .install(pending)?;
        let devices = link
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .devices
            .clone();
        for (peer, device) in devices.into_iter().enumerate() {
            let previous = self
                .pool
                .reusable_cell(&outbound.tag, peer, identity, generation)
                .filter(|cell| {
                    cell.get()
                        .is_some_and(|original| Arc::ptr_eq(original, &device))
                });
            let fresh = previous.is_none();
            let cell = match previous {
                Some(cell) => cell,
                None => {
                    let cell = Arc::new(OnceCell::new());
                    cell.set(device)
                        .map_err(|_| invalid("duplicate canonical peer device"))?;
                    cell
                }
            };
            staged.insert(
                outbound.tag.clone(),
                peer,
                identity,
                generation,
                cell,
                fresh,
                false,
            )?;
        }
        Ok(link)
    }
}
