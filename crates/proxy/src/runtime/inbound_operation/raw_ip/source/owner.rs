//! The same Source lifecycle keeps original views/memory through actual joins.
use super::*;
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        #[cfg(test)]
        eprintln!(
            "actual Packet Source lifecycle before read-grant take: {:?}",
            self.destination
        );
        let launch = self.resources.take_packet_source_wire_launch(0)?;
        self.shared.state.lock().unwrap().wire_launch = Some(launch);
        let reads = self.resources.take_packet_datagram_reads(0)?;
        #[cfg(test)]
        eprintln!(
            "actual Packet Source lifecycle read-grants taken, before listener: {:?}",
            self.destination
        );
        let layout = self
            .shared
            .state
            .lock()
            .unwrap()
            .wire
            .as_ref()
            .and_then(|wire| wire.device.packet_memory_layout())
            .ok_or_else(|| error(Reason::InvalidConfig))?;
        let memory = PacketSourceMemory {
            view: reads[0]
                .memory_view()
                .map_err(|source| error(Reason::Revoked).with_source(source))?,
            layout,
        };
        self.resources
            .install_packet_source_memory(0, memory.clone())?;
        self.shared.state.lock().unwrap().memory = Some(memory);
        self.shared.state.lock().unwrap().reads = Some(reads);
        let control = self
            .resources
            .take_packet_datagram_control(0)?
            .ok_or_else(|| error(Reason::InvalidConfig))?;
        self.shared.state.lock().unwrap().control = Some(control);
        let grant = self
            .resources
            .listen_datagram(self.destination.clone())
            .await?;
        self.shared.state.lock().unwrap().grant = Some(grant);
        #[cfg(test)]
        eprintln!(
            "actual Packet Source lifecycle retained listener: {:?}",
            self.destination
        );
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.shared.cancel.cancel();
        self.shared.stop.send_replace(true);
        self.shared.proxies.request_abort();
        // Only an unconsumed launch is refundable here; a spawned wire keeps
        // its real registration credit through its actual join/Host ACK.
        let unspawned = self.shared.state.lock().unwrap().wire_launch.take();
        drop(unspawned);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let witness = self.shared.done.lock().unwrap().clone();
        if let Some(witness) = witness {
            tokio::time::timeout_at(deadline, witness.wait())
                .await
                .map_err(|source| error(Reason::Timeout).with_source(source))?;
        }
        self.shared.pending_tx.lock().unwrap().take();
        // The real Scope join above ends wire custody. Native close belongs to
        // the original listener provider, not this logical-session report.
        self.shared.state.lock().unwrap().grant.take();
        self.shared.state.lock().unwrap().wire.take();
        let reads = self.shared.state.lock().unwrap().reads.take();
        if let Some(reads) = reads {
            for read in reads {
                read.revoke();
            }
        }
        let control = self.shared.state.lock().unwrap().control.take();
        if let Some(control) = control {
            control.revoke();
        }
        self.shared.proxies.clone().shutdown(deadline).await
    }
}
