use super::*;
/// Protocol-owned codec/state for Shadowsocks inbound UDP.
///
/// Runtime code owns socket I/O and routing, while this type owns
/// Shadowsocks UDP decoding, SIP022 replay protection, and response encoding.
#[cfg(feature = "crypto")]
pub struct ShadowsocksInboundUdpCodec {
    pub(super) limits: crate::validation::StateLimits,
    cipher: crate::shared::CipherKind,
    password: Vec<u8>,
    #[cfg(feature = "blake3")]
    identity_password: Option<Vec<u8>>,
    replay: crate::shared::legacy_replay::LegacyReplay,
    #[cfg(feature = "blake3")]
    replay_windows: state::ReplaySessions,
    #[cfg(feature = "blake3")]
    responses: std::sync::Mutex<crate::udp::session::ResponseSessions>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpCodec {
    pub fn with_state_limits(mut self, limits: crate::validation::StateLimits) -> Self {
        self.limits = limits;
        #[cfg(feature = "blake3")]
        {
            self.replay_windows = state::ReplaySessions::with_limits(limits);
            self.responses =
                std::sync::Mutex::new(crate::udp::session::ResponseSessions::with_limits(limits));
        }
        self
    }
    pub(super) fn touch_association(&mut self, id: Option<u64>) {
        #[cfg(not(feature = "blake3"))]
        let _ = id;
        #[cfg(feature = "blake3")]
        if let Some(id) = id {
            self.replay_windows.touch(id);
        }
    }

    pub fn with_replay_policy(self, policy: crate::validation::ReplayPolicy) -> Self {
        self.with_replay_guard(crate::shared::legacy_replay::LegacyReplay::new(
            policy, true,
        ))
    }
    pub(crate) fn with_replay_guard(
        mut self,
        replay: crate::shared::legacy_replay::LegacyReplay,
    ) -> Self {
        self.replay = replay;
        self
    }

    pub(super) fn maintain(&mut self) {
        #[cfg(feature = "blake3")]
        {
            self.replay_windows.prune();
            self.responses.get_mut().unwrap().prune();
        }
    }

    pub fn new(cipher: crate::shared::CipherKind, password: &[u8]) -> Self {
        Self {
            limits: Default::default(),
            cipher,
            password: password.to_vec(),
            #[cfg(feature = "blake3")]
            identity_password: None,
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), true),
            #[cfg(feature = "blake3")]
            replay_windows: state::ReplaySessions::default(),
            #[cfg(feature = "blake3")]
            responses: Default::default(),
        }
    }

    pub fn new_eih(
        cipher: crate::shared::CipherKind,
        identity_password: &[u8],
        user_password: &[u8],
    ) -> Self {
        #[cfg(not(feature = "blake3"))]
        let _ = identity_password;
        Self {
            limits: Default::default(),
            cipher,
            password: user_password.to_vec(),
            #[cfg(feature = "blake3")]
            identity_password: Some(identity_password.to_vec()),
            replay: crate::shared::legacy_replay::LegacyReplay::new(Default::default(), true),
            #[cfg(feature = "blake3")]
            replay_windows: state::ReplaySessions::default(),
            #[cfg(feature = "blake3")]
            responses: Default::default(),
        }
    }

    pub fn decode_request(
        &mut self,
        datagram: &[u8],
    ) -> Result<ShadowsocksInboundUdpPacket, Error> {
        if self.cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                let decoded = match self.identity_password.as_deref() {
                    Some(identity_password) => crate::shared::decode_udp_datagram_2022_eih_session(
                        self.cipher,
                        identity_password,
                        &self.password,
                        datagram,
                    ),
                    None => crate::shared::decode_udp_datagram_2022_session(
                        self.cipher,
                        &self.password,
                        datagram,
                    ),
                }?;
                let (target, port, payload, client_session_id, packet_id) = decoded;
                if !self.replay_windows.accept(client_session_id, packet_id)? {
                    return Err(Error::Protocol("ss: udp replay rejected"));
                }
                return Ok(ShadowsocksInboundUdpPacket {
                    target,
                    port,
                    payload,
                    client_session_id: Some(client_session_id),
                });
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: 2022 udp decode requires `blake3` feature",
            ));
        }

        let codec = crate::udp::ShadowsocksDatagramCodec::new(self.cipher, &self.password);
        let (target, port, payload) = codec
            .decode(datagram)
            .ok_or(Error::Protocol("ss: udp datagram decode failed"))?;
        self.replay.check(&datagram[..self.cipher.salt_len()])?;
        Ok(ShadowsocksInboundUdpPacket {
            target,
            port,
            payload,
            client_session_id: None,
        })
    }

    pub fn encode_response(
        &self,
        client_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        #[cfg(not(feature = "blake3"))]
        let _ = client_session_id;
        if self.cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                let client_session_id =
                    client_session_id.ok_or(Error::Protocol("ss: missing UDP client session"))?;
                let session = self
                    .responses
                    .lock()
                    .map_err(|_| Error::Protocol("ss: UDP response state poisoned"))?
                    .next(client_session_id)?;
                return crate::shared::encode_udp_response_2022(
                    self.cipher,
                    &self.password,
                    client_session_id,
                    target,
                    port,
                    payload,
                    session,
                );
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: 2022 udp encode requires `blake3` feature",
            ));
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

    pub fn encode_response_to_client(
        &self,
        client_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<ShadowsocksInboundUdpResponse, Error> {
        Ok(ShadowsocksInboundUdpResponse {
            datagram: self.encode_response(client_session_id, target, port, payload)?,
        })
    }

    pub fn response_frame(
        &self,
        target: &ShadowsocksInboundUdpResponseTarget,
        payload: &[u8],
    ) -> Result<ShadowsocksInboundUdpResponse, Error> {
        self.encode_response_to_client(
            target.client_session_id,
            &target.target,
            target.port,
            payload,
        )
    }
}
