//! Install or retain actual prepared peer originals after deferred startup.
use super::*;
impl WireguardAdapter {
    pub(super) async fn prepare_devices_inner(
        &self,
        outbounds: &[&OutboundConfig],
        mut context: OutboundDevicePreparationContext,
        cold: Option<Arc<pending_map::PendingEntries>>,
    ) -> Result<PreparedWireguardDevices, EngineError> {
        context.stack_options = context.stack_options.with_packet_wire_memory(
            zero_stack::packet_output::memory::PacketWireMemory::PaddedEnvelope {
                multiple: 16,
                overhead: 32,
            },
        );
        if cold.is_none() {
            PendingEndpoints::ensure_available(&self.pending_endpoints)?;
        }
        let mut profiles = HashMap::new();
        let mut linked = HashMap::new();
        let mut traffic = Vec::new();
        let generation = context.network_generation.0;
        let selected = self.select_devices(outbounds, &context)?;
        let desired = selected
            .iter()
            .map(|(outbound, _, identity, _)| {
                let candidate = cold.as_ref().ok_or_else(|| {
                    invalid("WireGuard requires its same actual cold component candidate")
                })?;
                let count = match &outbound.protocol {
                    OutboundProtocolConfig::Wireguard {
                        inbound_tag: Some(tag),
                        ..
                    } => candidate
                        .get(tag)
                        .ok_or_else(|| {
                            invalid("linked WireGuard has no actual cold component root")
                        })?
                        .client_peer_count(),
                    OutboundProtocolConfig::Wireguard {
                        inbound_tag: None, ..
                    } => candidate
                        .standalone
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&outbound.tag)
                        .ok_or_else(|| {
                            invalid("standalone WireGuard has no actual cold component root")
                        })?
                        .root
                        .peers
                        .len(),
                    _ => unreachable!(),
                };
                Ok((outbound.tag.clone(), (*identity, generation, count)))
            })
            .collect::<Result<HashMap<_, _>, EngineError>>()?;
        // No candidate device/task/socket may start before these actual
        // originals (including discarded candidates) have confirmed cleanup.
        self.pool.retire_changed(&desired).await?;
        let mut staged = self.pool.begin_stage();
        for (outbound, identity, device_identity, plan) in selected {
            let outer_udp_proxy = match &outbound.protocol {
                OutboundProtocolConfig::Wireguard {
                    outer_udp_proxy, ..
                } => outer_udp_proxy.as_deref(),
                _ => None,
            };
            if let OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(inbound_tag),
                ..
            } = &outbound.protocol
            {
                let link = self
                    .install_component_link(
                        cold.as_ref().expect("actual cold candidate"),
                        inbound_tag,
                        outbound,
                        &mut staged,
                        device_identity,
                        generation,
                        outer_udp_proxy
                            .and_then(|tag| context.packet_paths.get(tag))
                            .cloned(),
                    )
                    .await?;
                linked.insert(inbound_tag.clone(), link);
                profiles.insert(
                    outbound.tag.clone(),
                    CachedProfile {
                        graph: None,
                        identity,
                        device_identity,
                        plan,
                    },
                );
                continue;
            }
            {
                let (root, owned) = cold
                    .as_ref()
                    .and_then(|candidate| {
                        candidate
                            .standalone
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .get(&outbound.tag)
                            .map(|candidate| {
                                (candidate.root.clone(), candidate.owns_candidate_originals())
                            })
                    })
                    .ok_or_else(|| {
                        invalid(
                            "plain standalone WireGuard cannot start through its old UDP executor",
                        )
                    })?;
                if let Some(tag) = outer_udp_proxy {
                    root.install_outer(context.packet_paths.get(tag).ok_or_else(|| {
                        invalid("actual standalone outer operation was not prepared")
                    })?)
                    .await?;
                }
                for peer in 0..root.peers.len() {
                    let device = root.device(peer).map_err(std::io::Error::other)?;
                    let retained = self
                        .pool
                        .reusable_cell(&outbound.tag, peer, device_identity, generation)
                        .filter(|cell| {
                            cell.get()
                                .is_some_and(|current| Arc::ptr_eq(current, &device))
                        });
                    let (cell, fresh) = match retained {
                        Some(cell) => (cell, false),
                        None => {
                            let cell = Arc::new(OnceCell::new());
                            cell.set(device).map_err(|_| {
                                invalid("standalone original peer was already installed")
                            })?;
                            (cell, owned)
                        }
                    };
                    staged.insert(
                        outbound.tag.clone(),
                        peer,
                        device_identity,
                        generation,
                        cell,
                        fresh,
                    )?;
                }
                profiles.insert(
                    outbound.tag.clone(),
                    CachedProfile {
                        graph: Some(WeakStandaloneGraph::new(&root)),
                        identity,
                        device_identity,
                        plan,
                    },
                );
                continue;
            }
        }
        let pending = if let Some(candidate) = cold {
            traffic.extend(
                std::mem::take(&mut *candidate.traffic.lock().unwrap_or_else(|e| e.into_inner()))
                    .into_values(),
            );
            Some(PendingEndpoints::adopt(
                self.pending_endpoints.clone(),
                candidate,
            )?)
        } else {
            // With no WireGuard devices there is no new candidate to adopt.
            // Any previous originals still retire through the same host/pool.
            None
        };
        Ok(PreparedWireguardDevices {
            pool: self.pool.clone(),
            profiles: self.profiles.clone(),
            next_profiles: profiles,
            staged,
            linked: self.linked_endpoints.clone(),
            next_linked: linked,
            traffic,
            pending,
            published: false,
            stopped: Vec::new(),
        })
    }
}
