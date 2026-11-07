//! Actual registered wire takes the unique original before packet views escape.
use super::*;
impl PacketReply for Reply {
    fn packet_memory(&self) -> Option<PacketSourceMemory> {
        let memory = self.shared.state.lock().unwrap().memory.clone()?;
        memory.view.check_open().ok()?;
        Some(memory)
    }

    fn link_mtu(&self) -> Option<u16> {
        self.shared
            .state
            .lock()
            .unwrap()
            .wire
            .as_ref()
            .map(|wire| wire.device.mtu())
    }
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    fn facts(&self) -> &PacketFacts {
        &self.facts
    }
    async fn respond(self, _: ReplyOutcome) -> PacketReplyResult<Tx, Rx, Control> {
        Err(error(Reason::Unsupported))
    }
    async fn respond_observed(
        self,
        outcome: ReplyOutcome,
        observer: Arc<dyn PacketObserver>,
    ) -> PacketReplyResult<Tx, Rx, Control> {
        if outcome != ReplyOutcome::LocalChannelReady {
            self.shared.stop.send_replace(true);
            return Ok(None);
        }
        #[cfg(test)]
        eprintln!(
            "actual Packet Source respond before wire custody: {:?}",
            self.facts.inbound
        );
        let (mut wire, grant, reads, control, launch) = {
            let mut state = self.shared.state.lock().unwrap();
            (
                state.wire.take().ok_or_else(|| error(Reason::Revoked))?,
                state.grant.take().ok_or_else(|| error(Reason::Revoked))?,
                state.reads.take().ok_or_else(|| error(Reason::Revoked))?,
                state.control.take().ok_or_else(|| error(Reason::Revoked))?,
                state
                    .wire_launch
                    .take()
                    .ok_or_else(|| error(Reason::InvalidConfig))?,
            )
        };
        wire.take_declared_proxy_memory(self.shared.resources.as_ref())?;
        let original = grant.tx.identity();
        let (registered, original_registration) = tokio::sync::oneshot::channel::<()>();
        let (checkpoint_sender, actual_checkpoint) = tokio::sync::oneshot::channel();
        let mtu = wire.device.mtu();
        let (packets, received) = mpsc::channel(1);
        let (responses, returned) = mpsc::channel(1);
        let cancel = self.shared.cancel.clone();
        let shutdown = self.shared.stop.subscribe();
        let egress = observer.egress();
        let proxies = self.shared.proxies.clone();
        let outer_network = self.shared.outer_network.clone();
        let outer_resources = self.shared.resources.clone();
        let outer_tasks = self.shared.tasks.downgrade();
        let guard = cancel.clone().drop_guard();
        let waiting_cancel = cancel.clone();
        let source_facts = self.facts.clone();
        let facts = self.facts;
        let (_id, witness) = launch.spawn_tracked(async move {
                let _guard = guard;
                // This is the same real wire registration. It owns the unique
                // views while waiting; no IO begins before its retained native
                // controller has validated and saved this exact witness.
                tokio::select! {
                    biased;
                    _ = waiting_cancel.cancelled() => return Ok(()),
                    ready = original_registration => ready.map_err(|source| error(Reason::Revoked).with_source(source))?,
                }
                let (checkpoint, software)=tokio::select! {
                    biased;
                    _=waiting_cancel.cancelled()=>return Ok(()),
                    checkpoint=actual_checkpoint=>checkpoint.map_err(|source|error(Reason::Revoked).with_source(source))?,
                };
                if let Some((original, witness)) = software {
                    wire.prepare_declared_outer(
                        &outer_resources,
                        outer_network.as_ref().ok_or_else(||error(Reason::InvalidConfig))?,
                        original, &witness, outer_tasks, waiting_cancel.clone(),
                    ).await.map_err(|source|error(Reason::Io).with_source(source))?;
                }
                let result = wire::run(
                    wire, grant, reads, control, egress, packets, returned, facts, observer, shutdown, checkpoint,
                )
                .await;
                proxies.request_abort();
                result.map_err(|source| error(Reason::Io).with_source(source))
            })
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source wire registered: {:?}, task {:?}",
            source_facts.inbound,
            witness.task_id()
        );
        *self.shared.done.lock().unwrap() = Some(witness.clone());
        let registered_driver = self
            .shared
            .resources
            .register_packet_source_driver(&original, &witness)?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source native driver registration accepted: {:?}",
            source_facts.inbound
        );
        let checkpoint = registered_driver.checkpoint;
        let software = if self.shared.outer_network.is_some() {
            let original = self
                .shared
                .resources
                .register_software_packet_source_driver(&witness)?;
            Some((
                zero_platform_tokio::controlled_carrier::CarrierOrigin::from_logical_packet(
                    original,
                ),
                witness.clone(),
            ))
        } else {
            None
        };
        checkpoint_sender
            .send((checkpoint, software))
            .map_err(|_| error(Reason::Revoked))?;
        registered.send(()).map_err(|_| error(Reason::Revoked))?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source wire original gate released: {:?}",
            source_facts.inbound
        );
        Ok(Some(PacketSession {
            tx: Tx {
                sender: PollSender::new(responses),
                cancel: cancel.clone(),
                pending: self.shared.pending_tx.clone(),
                facts: source_facts,
            },
            rx: Rx { receiver: received },
            control: Control {
                cancel,
                stop: self.shared.stop.clone(),
                witness,
                pending: self.shared.pending_tx.clone(),
            },
            limits: PacketLimits {
                mtu: usize::from(mtu).max(65535),
                families: vec![
                    zero_core::packet::IpFamily::Ipv4,
                    zero_core::packet::IpFamily::Ipv6,
                ],
                source_adaptation: SourceAdaptation::Preserved,
                return_association: ReturnGuarantee::Peer,
            },
        }))
    }
}
