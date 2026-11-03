//! Typed packet/return facts over the listener's actual shared codecs.
use super::*;
impl ShadowsocksInboundUdpResponder {
    pub fn decode_authenticated_packet(
        &mut self,
        packet: &[u8],
        sender: std::net::SocketAddr,
    ) -> Result<(InboundUdpDispatch, SessionAuth), Error> {
        let dispatch = self.decode_inbound_dispatch(packet)?;
        let user = match &self.mode {
            ShadowsocksInboundUdpResponderMode::Profile { current_user, .. } => {
                current_user.as_deref()
            }
            ShadowsocksInboundUdpResponderMode::Single(_) => None,
        };
        let id = self
            .associations
            .identify(user, dispatch.client_session_id(), sender)
            .ok_or(Error::Protocol("ss: UDP association capacity exhausted"))?;
        let auth = self
            .current_auth()
            .cloned()
            .ok_or(Error::Protocol("ss: missing packet authentication"))?;
        let (protocol, target, port, payload, _) = dispatch.into_parts();
        Ok((
            InboundUdpDispatch::new(protocol, target, port, payload, Some(id)),
            auth,
        ))
    }
    pub fn encode_association_response(
        &mut self,
        association: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(std::net::SocketAddr, Vec<u8>), Error> {
        self.maintain();
        let (user, wire, client) = self
            .associations
            .response(association)
            .ok_or(Error::Protocol("ss: UDP association retired"))?;
        let session = match &mut self.mode {
            ShadowsocksInboundUdpResponderMode::Single(session) => session,
            ShadowsocksInboundUdpResponderMode::Profile {
                profile, sessions, ..
            } => {
                let user = user.ok_or(Error::Protocol("ss: missing authenticated UDP user"))?;
                if !profile
                    .users_snapshot()
                    .iter()
                    .any(|active| active.cache_key() == user)
                {
                    return Err(Error::Protocol("ss: UDP user retired"));
                }
                sessions
                    .get_mut(&user)
                    .ok_or(Error::Protocol("ss: UDP codec retired"))?
            }
        };
        session.codec.touch_association(wire);
        let packet = session.encode_response_to_client(wire, target, port, payload)?;
        Ok((client, packet.into_datagram()))
    }
}
