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
        if peers.prepared_outer.len() != peers.carriers.len()
            || peers.carriers.iter().any(Option::is_some)
            || !self.carriers.is_empty()
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
                    path.root_binding.clone(),
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
            let carrier = crate::runtime::raw_ip::RegisteredProxiedRawIpWireCarrier::start(
                path.operation.clone(),
                execution,
                tasks.clone(),
            )
            .await?;
            // Strong-install this actual carrier, including its original views
            // and control, before another peer can start or the loop can poll IO.
            self.carriers[peer] = Some(Arc::new(carrier));
        }
        Ok(())
    }
}
