//! Owned native/decode storage may be inspected for actual stack admission
//! without prematurely accepting replay/counter/timer/session state.
use super::*;
/// A concrete authenticated original Session, never a caller-created ID. Holds
/// only the same key/session allocation through this one pending operation.
pub struct PreparedDataCommit {
    session: std::sync::Arc<session::Session>,
    slot: usize,
    counter: u64,
    plaintext_len: usize,
}
impl<R: rand::RngCore + Send> Tunn<R> {
    pub fn prepare_owned_data(
        &self,
        packet: Packet<WgData>,
    ) -> Result<(PreparedDataCommit, Packet), WireGuardError> {
        let receiving = packet.header.receiver_idx.get();
        let (slot, session) = self
            .sessions
            .iter()
            .enumerate()
            .filter_map(|(slot, session)| session.as_ref().map(|s| (slot, s)))
            .find(|(_, session)| session.receiving_index.value() == receiving)
            .ok_or(WireGuardError::NoCurrentSession)?;
        if !self.prepared_data_session_live(slot, self.timers.now()) {
            return Err(WireGuardError::ConnectionExpired);
        }
        let (packet, counter) = session.prepare_packet_data(packet)?;
        Ok((
            PreparedDataCommit {
                session: session.clone(),
                slot,
                counter,
                plaintext_len: packet.len(),
            },
            packet,
        ))
    }
    /// Only after concrete storage/actual consumer admission. Recheck both
    /// original Session identity and replay before any commit-side mutation.
    pub fn commit_owned_data(
        &mut self,
        prepared: PreparedDataCommit,
    ) -> Result<(), WireGuardError> {
        let now = self.timers.now();
        if !self.sessions[prepared.slot]
            .as_ref()
            .is_some_and(|same| std::sync::Arc::ptr_eq(same, &prepared.session))
            || !self.prepared_data_session_live(prepared.slot, now)
        {
            return Err(WireGuardError::NoCurrentSession);
        }
        prepared.session.receiving_counter_mark(prepared.counter)?;
        // All memory is admitted and replay is now accepted. Use that same
        // owner-clock observation for the existing expiry/timer behavior.
        self.refresh_expiration_at(now);
        self.set_current_session(prepared.slot);
        self.timer_tick(TimerName::TimeLastPacketReceived);
        self.rx_bytes += prepared.plaintext_len;
        if prepared.plaintext_len != 0 {
            self.timer_tick(TimerName::TimeLastDataPacketReceived);
        }
        Ok(())
    }
}
