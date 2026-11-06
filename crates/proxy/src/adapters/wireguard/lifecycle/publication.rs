use super::*;
impl PreparedWireguardDevices {
    pub(crate) fn publish_sync(&mut self) {
        if self.published {
            return;
        }
        for prepared in self.traffic.drain(..) {
            prepared.publish();
        }
        let mut active = self
            .profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for update in self.linked_updates.drain(..) {
            update.publish();
        }
        for link in self.next_linked.values() {
            link.publish_endpoint_io();
        }
        self.stopped = self.pool.publish_pending(&mut self.staged);
        *active = std::mem::take(&mut self.next_profiles);
        *self
            .linked
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = std::mem::take(&mut self.next_linked);
        self.published = true;
        // Keep the original PendingEndpoints and fresh staging custody armed.
        // Failure after this synchronous switch must stop only fresh originals.
    }
    pub(crate) fn confirm_publication(&mut self) {
        assert!(
            self.published,
            "linked maps must precede commit confirmation"
        );
        self.staged.confirm_publication();
        if let Some(mut pending) = self.pending.take() {
            pending.published();
        }
    }
    pub(crate) async fn finish_publication(&mut self) {
        // Borrow originals in place. Cancelled waiters leave both this guard
        // and the pool's same retiring controllers/joins available for retry.
        for device in &self.stopped {
            if let Err(error) = device.wait_workers_stopped().await {
                tracing::warn!(%error, "compatibility device publication joined original failed workers");
            }
        }
        self.stopped.clear();
    }
    #[cfg(test)]
    pub(crate) async fn publish(mut self) {
        self.publish_sync();
        self.confirm_publication();
        self.finish_publication().await;
    }
}

impl WireguardAdapter {
    pub(crate) fn request_device_shutdown(&self) {
        self.pool.request_shutdown();
    }

    pub(crate) async fn close_devices(&self) -> Result<(), EngineError> {
        self.request_device_shutdown();
        self.pool.close_all().await?;
        self.profiles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.linked_endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        Ok(())
    }

    pub(crate) fn outbound_device_health(
        &self,
        outbounds: &[&OutboundConfig],
    ) -> Vec<zero_api::OutboundDeviceHealthSnapshot> {
        let mut snapshots = Vec::new();
        for outbound in outbounds {
            let OutboundProtocolConfig::Wireguard { peers, .. } = &outbound.protocol else {
                continue;
            };
            let Some(identity) = protocol_identity(&outbound.protocol) else {
                continue;
            };
            let identity = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map_or(identity, |cached| cached.device_identity);
            for peer_index in 0..peers.len() {
                snapshots.push(
                    self.pool
                        .health_snapshot(&outbound.tag, peer_index, identity)
                        .unwrap_or_else(|| zero_api::OutboundDeviceHealthSnapshot {
                            tag: outbound.tag.clone(),
                            peer_index,
                            ..Default::default()
                        }),
                );
            }
        }
        snapshots
    }
}
