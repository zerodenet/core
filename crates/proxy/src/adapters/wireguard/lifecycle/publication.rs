use super::*;
impl PreparedWireguardDevices {
    pub(crate) async fn publish(self) {
        let Self {
            pool,
            profiles,
            next_profiles,
            staged,
            linked,
            next_linked,
            linked_updates,
            mut pending,
            traffic,
        } = self;
        for prepared in traffic {
            prepared.publish();
        }
        let stopped = {
            let mut active = profiles.lock().unwrap_or_else(|error| error.into_inner());
            for update in linked_updates {
                update.publish();
            }
            let stopped = pool.publish(staged);
            *active = next_profiles;
            *linked.lock().unwrap_or_else(|error| error.into_inner()) = next_linked;
            pending.published();
            drop(pending);
            stopped
        };
        for device in stopped {
            if let Err(error) = device.wait_workers_stopped().await {
                tracing::warn!(%error, "compatibility device publication joined original failed workers");
            }
        }
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
