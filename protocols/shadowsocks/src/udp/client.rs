//! One codec clone family owns one outbound UDP association.
#[cfg(feature = "blake3")]
use super::session::{ReceiveWindows, SenderSession};
use crate::shared::CipherKind;
use zero_core::{Address, Error};
use zero_traits::{DatagramCodec, UdpDatagramFraming};

#[cfg(feature = "blake3")]
#[derive(Debug, Default)]
struct Session {
    sender: SenderSession,
    received: ReceiveWindows,
}

#[derive(Clone)]
pub struct ShadowsocksDatagramCodec {
    cipher: CipherKind,
    password: Vec<u8>,
    limits: crate::validation::StateLimits,
    replay: crate::shared::legacy_replay::LegacyReplay,
    #[cfg(feature = "blake3")]
    state: std::sync::Arc<std::sync::Mutex<Session>>,
}
impl ShadowsocksDatagramCodec {
    pub fn with_state_limits(mut self, limits: crate::validation::StateLimits) -> Self {
        self.limits = limits;
        #[cfg(feature = "blake3")]
        {
            self.state = std::sync::Arc::new(std::sync::Mutex::new(Session {
                sender: Default::default(),
                received: ReceiveWindows::with_limits(limits),
            }));
        }
        self
    }

    pub fn with_replay_policy(self, policy: crate::validation::ReplayPolicy) -> Self {
        self.with_replay_guard(crate::shared::legacy_replay::LegacyReplay::new(
            policy, false,
        ))
    }
    pub(crate) fn with_replay_guard(
        mut self,
        replay: crate::shared::legacy_replay::LegacyReplay,
    ) -> Self {
        self.replay = replay;
        self
    }
    pub(crate) fn fresh_association(&self) -> Self {
        Self::new(self.cipher, &self.password)
            .with_replay_guard(self.replay.clone())
            .with_state_limits(self.limits)
    }

    pub fn new(cipher: CipherKind, password: impl AsRef<[u8]>) -> Self {
        Self {
            cipher,
            limits: Default::default(),
            password: password.as_ref().to_vec(),
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
            #[cfg(feature = "blake3")]
            state: Default::default(),
        }
    }
}
impl DatagramCodec<Address> for ShadowsocksDatagramCodec {
    type Error = Error;
    fn encode(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
        #[cfg(feature = "blake3")]
        if self.cipher.is_blake3() {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Protocol("ss: UDP session poisoned"))?;
            state.received.prune();
            let session = state.sender.next()?;
            return crate::shared::encode_udp_request_with_session(
                self.cipher,
                &self.password,
                target,
                port,
                payload,
                session,
            );
        }
        <crate::outbound::ShadowsocksOutbound as UdpDatagramFraming<
            crate::udp::ShadowsocksUdpPacketTarget<'_>,
            crate::udp::ShadowsocksUdpDecodeContext<'_>,
        >>::encode_udp_datagram(
            &crate::outbound::ShadowsocksOutbound,
            &crate::udp::ShadowsocksUdpPacketTarget {
                target,
                port,
                payload,
                cipher: self.cipher,
                password: &self.password,
            },
        )
    }
    fn decode(&self, data: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        #[cfg(feature = "blake3")]
        if self.cipher.is_blake3() {
            let packet =
                crate::shared::decode_udp_wire_2022(self.cipher, &self.password, data).ok()?;
            let mut state = self.state.lock().ok()?;
            if !state.sender.matches(packet.client_session_id?) {
                return None;
            }
            if !state
                .received
                .accept(packet.session_id, packet.packet_id)
                .ok()?
            {
                return None;
            }
            return Some((packet.target, packet.port, packet.payload));
        }
        let packet = <crate::outbound::ShadowsocksOutbound as UdpDatagramFraming<
            crate::udp::ShadowsocksUdpPacketTarget<'_>,
            crate::udp::ShadowsocksUdpDecodeContext<'_>,
        >>::decode_udp_datagram(
            &crate::outbound::ShadowsocksOutbound,
            &crate::udp::ShadowsocksUdpDecodeContext {
                cipher: self.cipher,
                password: &self.password,
            },
            data,
        )
        .ok()?;
        self.replay
            .check(data.get(..self.cipher.salt_len())?)
            .ok()?;
        Some(packet.into_parts())
    }
}

#[cfg(all(test, feature = "blake3"))]
#[path = "../../tests/udp_session/client.rs"]
mod tests;

impl core::fmt::Debug for ShadowsocksDatagramCodec {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ShadowsocksDatagramCodec")
            .field("cipher", &self.cipher)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
