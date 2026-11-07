//! Install or retain actual prepared peer originals after deferred startup.
use super::*;
mod proxied;
impl WireguardAdapter {
    pub(super) async fn prepare_devices_inner(
        &self,
        outbounds: &[&OutboundConfig],
        inbounds: &[InboundConfig],
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
        let upstream = &context.upstream;
        let mut profiles = HashMap::new();
        let mut linked = HashMap::new();
        let mut linked_updates = Vec::new();
        let mut traffic = Vec::new();
        let generation = upstream.egress_generation();
        let selected = self.select_devices(outbounds, &context)?;
        let desired = selected
            .iter()
            .map(|(outbound, _, identity, plan)| {
                let count = if matches!(
                    outbound.protocol,
                    OutboundProtocolConfig::Wireguard {
                        inbound_tag: None,
                        outer_udp_proxy: None,
                        ..
                    }
                ) {
                    cold.as_ref()
                        .and_then(|candidate| {
                            candidate
                                .standalone
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .get(&outbound.tag)
                                .map(|candidate| candidate.root.peers.len())
                        })
                        .ok_or_else(|| {
                            invalid("plain standalone WireGuard has no actual cold component root")
                        })?
                } else {
                    self.prepared_peer_count(
                        outbound, inbounds, plan, *identity, generation, &context,
                    )?
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
            let canonical = match &outbound.protocol {
                OutboundProtocolConfig::Wireguard {
                    inbound_tag: Some(tag),
                    outer_udp_proxy: None,
                    ..
                } => cold
                    .as_ref()
                    .is_some_and(|candidate| candidate.contains_key(tag)),
                OutboundProtocolConfig::Wireguard {
                    inbound_tag: None,
                    outer_udp_proxy: None,
                    ..
                } => cold.as_ref().is_some_and(|candidate| {
                    candidate
                        .standalone
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .contains_key(&outbound.tag)
                }),
                _ => false,
            };
            let prepared_traffic = (!canonical)
                .then(|| {
                    context
                        .endpoint_bindings
                        .iter()
                        .find(|b| b.canonical && b.outbound_tags.contains(&outbound.tag))
                        .and_then(|binding| {
                            context
                                .traffic
                                .prepare_endpoint_traffic(binding, &plan.peer_ids())
                        })
                })
                .flatten();
            let meters = prepared_traffic.as_ref().map(|p| (p.endpoint(), p.peers()));
            if let Some(prepared) = prepared_traffic {
                traffic.push(prepared);
            }
            if let OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(inbound_tag),
                ..
            } = &outbound.protocol
            {
                let inbound = inbounds
                    .iter()
                    .find(|inbound| inbound.tag == *inbound_tag)
                    .ok_or_else(|| invalid("linked WireGuard inbound missing"))?;
                let (link, update) = if canonical {
                    let link = self
                        .install_component_link(
                            cold.as_ref().expect("actual cold candidate"),
                            inbound_tag,
                            outbound,
                            &mut staged,
                            device_identity,
                            generation,
                        )
                        .await?;
                    (link, None)
                } else {
                    if outer_udp_proxy.is_none() {
                        return Err(invalid(
                            "plain linked graph cannot restart through the legacy worker path",
                        ));
                    }
                    // TEMP-WG-OUTER-UDP-01 retains this original compatibility branch.
                    self.prepare_linked_endpoint(
                        outbound,
                        inbound,
                        &plan,
                        &context,
                        &mut staged,
                        device_identity,
                        generation,
                        desired[&outbound.tag].2,
                        meters.as_ref(),
                    )
                    .await?
                };
                if let Some(update) = update {
                    linked_updates.push(update);
                }
                linked.insert(inbound_tag.clone(), link);
                profiles.insert(
                    outbound.tag.clone(),
                    CachedProfile {
                        graph: None,
                        packet: self.packet_operations(
                            &outbound.tag,
                            device_identity,
                            plan.clone(),
                        ),
                        identity,
                        device_identity,
                        plan,
                    },
                );
                continue;
            }
            if outer_udp_proxy.is_none() {
                let root = cold
                    .as_ref()
                    .and_then(|candidate| {
                        candidate
                            .standalone
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .get(&outbound.tag)
                            .map(|candidate| candidate.root.clone())
                    })
                    .ok_or_else(|| {
                        invalid(
                            "plain standalone WireGuard cannot start through its old UDP executor",
                        )
                    })?;
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
                            (cell, true)
                        }
                    };
                    staged.insert(
                        outbound.tag.clone(),
                        peer,
                        device_identity,
                        generation,
                        cell,
                        fresh,
                        false,
                    )?;
                }
                profiles.insert(
                    outbound.tag.clone(),
                    CachedProfile {
                        graph: Some(Arc::downgrade(&root)),
                        packet: self.packet_operations(
                            &outbound.tag,
                            device_identity,
                            plan.clone(),
                        ),
                        identity,
                        device_identity,
                        plan,
                    },
                );
                continue;
            }
            // A standalone device has no retained listener role. Revocation
            // removes its stacks/carrier through normal pool publication.
            if context.disabled_outbounds.contains(&outbound.tag) {
                continue;
            }
            self.prepare_proxied_peers(
                proxied::ProxiedPreparation {
                    outbound,
                    plan: &plan,
                    context: &context,
                    device_identity,
                    generation,
                    meters: &meters,
                    outer_udp_proxy,
                },
                &mut staged,
            )
            .await?;
            profiles.insert(
                outbound.tag.clone(),
                CachedProfile {
                    graph: None,
                    packet: self.packet_operations(&outbound.tag, device_identity, plan.clone()),
                    identity,
                    device_identity,
                    plan,
                },
            );
        }
        let pending = if let Some(candidate) = cold {
            traffic.extend(
                std::mem::take(&mut *candidate.traffic.lock().unwrap_or_else(|e| e.into_inner()))
                    .into_values(),
            );
            PendingEndpoints::adopt(self.pending_endpoints.clone(), candidate)?
        } else {
            PendingEndpoints::install(
                self.pending_endpoints.clone(),
                linked
                    .iter()
                    .map(|(tag, link)| {
                        Ok((
                            tag.clone(),
                            Arc::new(PendingLinkedEndpoint::from_ready(link.clone())?),
                        ))
                    })
                    .collect::<Result<HashMap<_, _>, EngineError>>()?,
            )?
        };
        Ok(PreparedWireguardDevices {
            pool: self.pool.clone(),
            profiles: self.profiles.clone(),
            next_profiles: profiles,
            staged,
            linked: self.linked_endpoints.clone(),
            next_linked: linked,
            linked_updates,
            traffic,
            pending: Some(pending),
            published: false,
            stopped: Vec::new(),
        })
    }
}
