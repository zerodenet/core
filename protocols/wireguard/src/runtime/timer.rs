use super::{PeerTunnel, TunnelAction, TunnelError};
use gotatun::{
    noise::{errors::WireGuardError, TunnResult},
    packet::WgKind,
};

impl PeerTunnel {
    /// Protocol-owned deadline, including sampled jitter and key destruction.
    pub fn next_timer_delay(&self) -> Option<std::time::Duration> {
        self.engine.next_timer_delay()
    }

    /// GoTATun has already cleared expired sessions/queued packets. Its timer
    /// stays inert until incoming authentication or outgoing demand revives it.
    pub fn timer_enabled(&self) -> bool {
        !self.engine.is_expired()
    }

    pub fn tick(&mut self) -> Result<Vec<TunnelAction>, TunnelError> {
        let result = self.engine.update_timers();
        self.collect_timer_result(result)
    }

    fn collect_timer_result(
        &mut self,
        result: Result<Option<WgKind>, WireGuardError>,
    ) -> Result<Vec<TunnelAction>, TunnelError> {
        match result {
            Ok(Some(packet)) => self.collect_result(TunnResult::WriteToNetwork(packet)),
            // Expiration persists until a new handshake. It is not a failed
            // carrier operation, and must not become four errors per second.
            // Keep this exception local to timers, as in GoTATun's device loop.
            Ok(None) | Err(WireGuardError::ConnectionExpired) => Ok(Vec::new()),
            Err(_) => Err(TunnelError::Engine),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/runtime/timer.rs"]
mod tests;
