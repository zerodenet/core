use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use zero_engine::EngineError;

pub(crate) enum RawIpAction {
    SendNetwork(zero_traits::PacketBuffer),
    ReceiveIp {
        packet: zero_traits::PacketBuffer,
        source: IpAddr,
    },
}

pub(crate) trait RawIpTunnel: Send {
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError>;
    fn send_ip_packet(&mut self, packet: &[u8]) -> Result<Vec<RawIpAction>, EngineError>;
    fn receive_datagram(
        &mut self,
        source: Option<SocketAddr>,
        datagram: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError>;
    fn receive_datagram_with_authentication(
        &mut self,
        source: Option<SocketAddr>,
        datagram: &[u8],
    ) -> Result<(Vec<RawIpAction>, bool), EngineError> {
        self.receive_datagram(source, datagram)
            .map(|actions| (actions, false))
    }
    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError>;
    /// A protocol may park its timer when only I/O can revive its state.
    /// This must not suppress pending keepalive, retry or key-expiry work.
    fn timer_enabled(&self) -> bool {
        true
    }
    /// Parking affects the protocol timer, not I/O or resource maintenance.
    /// Legacy implementations retain their polling cadence until they provide
    /// their own deadline. Recompute after every protocol state transition.
    fn timer_schedule(&self) -> super::timer::TimerSchedule {
        if self.timer_enabled() {
            super::timer::TimerSchedule::Polling
        } else {
            super::timer::TimerSchedule::Parked
        }
    }
    fn allows_source(&self, source: IpAddr) -> bool;
    fn time_since_last_handshake(&self) -> Option<Duration> {
        None
    }
}

pub(crate) struct RawIpPeerPlan {
    pub(crate) peer_index: usize,
    pub(crate) local_ip: IpAddr,
}

pub(crate) trait RawIpOutboundPlan: Send + Sync {
    fn peer_identity(&self, _peer: usize) -> Option<std::sync::Arc<str>> {
        None
    }
    fn mtu(&self) -> u16;
    fn is_local_address(&self, _address: IpAddr) -> bool {
        false
    }
    fn peer_for_target(&self, target: IpAddr) -> Result<RawIpPeerPlan, EngineError>;
    fn build_tunnel(&self, peer_index: usize) -> Result<Box<dyn RawIpTunnel>, EngineError>;
}
