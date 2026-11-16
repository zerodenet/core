//! The actual registered Source wire constructs its declared logical paths.
//! This owns only software views; no native-ancestor close is inferred here.
use super::*;
use zero_platform_tokio::{
    contract::{
        component::ResourceAccess,
        network::CarrierContext,
        scope::{TaskJoinWitness, WeakTaskRegistrar},
    },
    controlled_carrier::CarrierOrigin,
};
impl Wire {
    pub(in crate::runtime::inbound_operation::raw_ip) async fn prepare_declared_outer(
        &mut self,
        resources: &Arc<dyn ResourceAccess>,
        network: &std::sync::Weak<crate::component::background::BackgroundNetwork>,
        original: CarrierOrigin,
        driver: &TaskJoinWitness,
        tasks: WeakTaskRegistrar,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Result<(), EngineError> {
        let peers = self
            .endpoint
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::other("declared Source outer path has no actual endpoint")
            })?
            .peers
            .borrow()
            .clone();
        if peers.prepared_outer.len() != peers.initial_endpoints.len() || !self.carriers.is_empty()
        {
            return Err(std::io::Error::other(
                "Source outer paths do not match their original peer declaration",
            )
            .into());
        }
        let network = network
            .upgrade()
            .ok_or_else(|| std::io::Error::other("original Source Host services retired"))?;
        // Payload/read/copy/codec workspaces are the same pre-bind originals.
        // This per-peer controller table is separate construction metadata;
        // its finite row count does not claim that byte accounting is complete.
        self.carriers = vec![None; peers.prepared_outer.len()];
        {
            let mut owners = self
                .outer_owners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !owners.is_empty() {
                return Err(
                    std::io::Error::other("same Source outer owners already installed").into(),
                );
            }
            owners
                .try_reserve_exact(peers.prepared_outer.len())
                .map_err(std::io::Error::other)?;
        }
        for (peer, path) in peers.prepared_outer.iter().enumerate() {
            let Some(path) = path else {
                if peers
                    .initial_endpoints
                    .get(peer)
                    .is_some_and(Option::is_some)
                {
                    return Err(std::io::Error::other(
                        "configured Source outer peer lacks its actual prepared carrier",
                    )
                    .into());
                }
                continue;
            };
            if self
                .proxy_reads
                .get(peer)
                .and_then(Option::as_ref)
                .is_none()
            {
                return Err(std::io::Error::other(
                    "Source outer peer lost its original read workspace",
                )
                .into());
            }
            let grant = resources
                .packet_driver_caller(
                    &original,
                    driver,
                    path.recipe.binding().clone(),
                    CarrierContext {
                        session: None,
                        deadline: None,
                        cancel: cancel.clone(),
                    },
                )
                .map_err(std::io::Error::other)?;
            let execution = network
                .packet_driver_execution(grant, Arc::downgrade(resources))
                .map_err(std::io::Error::other)?
                .with_receive_workspaces(self.proxy_codecs.get(peer).cloned().ok_or_else(|| {
                    std::io::Error::other("Source outer peer lost its actual codec workspaces")
                })?)
                .map_err(std::io::Error::other)?;
            let carrier = Arc::new(
                crate::runtime::raw_ip::RegisteredProxiedRawIpWireCarrier::prepare(
                    path.recipe.clone(),
                    execution,
                    tasks.clone(),
                    peers
                        .initial_endpoints
                        .get(peer)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            std::io::Error::other(
                                "declared outer peer has no actual initial endpoint",
                            )
                        })?,
                )?,
            );
            // Keep the controller before its FIRST acquisition can suspend.
            // Interrupted or failed initialization remains on this same wire.
            self.outer_owners
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(carrier.clone());
            self.carriers[peer] = Some(carrier.clone());
            carrier.initialize().await?;
        }
        Ok(())
    }
}
