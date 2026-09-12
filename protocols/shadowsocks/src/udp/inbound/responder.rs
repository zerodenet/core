use super::*;
#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpResponder {
    pub fn new(session: ShadowsocksInboundUdpSession) -> Self {
        let associations = association::Associations::with_limits(session.codec.limits);
        Self {
            mode: ShadowsocksInboundUdpResponderMode::Single(session),
            pending_client: None,
            pending_wire_session: None,
            associations,
            read_buf: vec![0_u8; 64 * 1024],
            maintenance_at: tokio::time::Instant::now() + state::MAINTENANCE,
        }
    }

    pub fn from_profile(profile: crate::inbound::ShadowsocksInboundProfile) -> Self {
        let associations = association::Associations::with_limits(profile.state_limits());
        Self {
            mode: ShadowsocksInboundUdpResponderMode::Profile {
                profile,
                sessions: std::collections::HashMap::new(),
                proxy_users: std::collections::HashMap::new(),
                current_user: None,
                current_auth: None,
            },
            pending_client: None,
            pending_wire_session: None,
            associations,
            read_buf: vec![0_u8; 64 * 1024],
            maintenance_at: tokio::time::Instant::now() + state::MAINTENANCE,
        }
    }

    pub fn decode_inbound_dispatch(
        &mut self,
        datagram: &[u8],
    ) -> Result<InboundUdpDispatch, Error> {
        self.maintain();
        match &mut self.mode {
            ShadowsocksInboundUdpResponderMode::Single(session) => {
                session.decode_inbound_dispatch(datagram)
            }
            ShadowsocksInboundUdpResponderMode::Profile {
                profile,
                sessions,
                current_user,
                current_auth,
                ..
            } => {
                let users = profile.users_snapshot();
                let active = users
                    .iter()
                    .map(crate::inbound::ShadowsocksUser::cache_key)
                    .collect::<std::collections::HashSet<_>>();
                sessions.retain(|key, _| active.contains(key));
                if profile.uses_eih() {
                    #[cfg(feature = "blake3")]
                    {
                        let user = profile.identify_udp_user(datagram)?;
                        let key = user.cache_key();
                        let identity_password = profile
                            .identity_password()
                            .ok_or(Error::Protocol("ss: SIP023 identity key is not configured"))?;
                        let session = sessions.entry(key.clone()).or_insert_with(|| {
                            ShadowsocksInboundUdpSession::new(
                                ShadowsocksInboundUdpCodec::new_eih(
                                    profile.cipher(),
                                    identity_password,
                                    user.password(),
                                )
                                .with_state_limits(profile.state_limits()),
                            )
                        });
                        let dispatch = session.decode_inbound_dispatch(datagram)?;
                        *current_user = Some(key);
                        *current_auth = Some(user.auth());
                        return Ok(dispatch);
                    }
                    #[cfg(not(feature = "blake3"))]
                    return Err(Error::Protocol(
                        "ss: SIP023 udp accept requires `blake3` feature",
                    ));
                }
                for user in users.iter() {
                    let key = user.cache_key();
                    let session = sessions.entry(key.clone()).or_insert_with(|| {
                        ShadowsocksInboundUdpSession::new(
                            ShadowsocksInboundUdpCodec::new(profile.cipher(), user.password())
                                .with_replay_guard(profile.legacy_replay())
                                .with_state_limits(profile.state_limits()),
                        )
                    });
                    if let Ok(dispatch) = session.decode_inbound_dispatch(datagram) {
                        *current_user = Some(key);
                        *current_auth = Some(user.auth());
                        return Ok(dispatch);
                    }
                }
                *current_user = None;
                *current_auth = None;
                Err(Error::Protocol("ss: udp user authentication failed"))
            }
        }
    }

    pub async fn read_inbound_dispatch_from_socket_tokio(
        &mut self,
        socket: &tokio::net::UdpSocket,
    ) -> Result<InboundUdpDispatch, Error> {
        loop {
            let (n, client) = tokio::select! {
                result = socket.recv_from(&mut self.read_buf) => {
                    result.map_err(|_| Error::Io("ss udp recv error"))?
                }
                _ = tokio::time::sleep_until(self.maintenance_at) => {
                    self.maintain();
                    continue;
                }
            };
            let datagram = self.read_buf[..n].to_vec();
            let dispatch = self.decode_inbound_dispatch(&datagram);
            match dispatch {
                Ok(dispatch) => {
                    let wire_session = dispatch.client_session_id();
                    let user = match &self.mode {
                        ShadowsocksInboundUdpResponderMode::Single(_) => None,
                        ShadowsocksInboundUdpResponderMode::Profile { current_user, .. } => {
                            current_user.as_deref()
                        }
                    };
                    let Some(id) = self.associations.identify(user, wire_session, client) else {
                        continue;
                    };
                    self.pending_client = Some(client);
                    self.pending_wire_session = wire_session;
                    let (protocol, target, port, payload, _) = dispatch.into_parts();
                    return Ok(InboundUdpDispatch::new(
                        protocol,
                        target,
                        port,
                        payload,
                        Some(id),
                    ));
                }
                Err(_) => continue,
            }
        }
    }

    pub fn record_dispatch_success(
        &mut self,
        proxy_session_id: u64,
        client_session_id: Option<u64>,
        client: std::net::SocketAddr,
    ) {
        match &mut self.mode {
            ShadowsocksInboundUdpResponderMode::Single(session) => {
                session.record_dispatch_success(proxy_session_id, client_session_id, client);
            }
            ShadowsocksInboundUdpResponderMode::Profile {
                sessions,
                proxy_users,
                current_user,
                ..
            } => {
                let Some(user_key) = current_user.clone() else {
                    return;
                };
                let Some(session) = sessions.get_mut(&user_key) else {
                    return;
                };
                session.record_dispatch_success(proxy_session_id, client_session_id, client);
                if session.bindings.contains(proxy_session_id) {
                    proxy_users.insert(proxy_session_id, user_key);
                }
            }
        }
    }

    pub fn record_pending_dispatch_success(
        &mut self,
        proxy_session_id: u64,
        _client_session_id: Option<u64>,
    ) {
        if let Some(client) = self.pending_client.take() {
            let wire_session = self.pending_wire_session.take();
            self.record_dispatch_success(proxy_session_id, wire_session, client);
        }
    }

    pub async fn send_response_for_target_proxy_session_to_client_tokio(
        &self,
        socket: &tokio::net::UdpSocket,
        proxy_session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        match &self.mode {
            ShadowsocksInboundUdpResponderMode::Single(session) => {
                session
                    .send_response_for_target_proxy_session_to_client_tokio(
                        socket,
                        proxy_session_id,
                        target,
                        port,
                        payload,
                    )
                    .await
            }
            ShadowsocksInboundUdpResponderMode::Profile {
                sessions,
                proxy_users,
                ..
            } => {
                let Some(proxy_session_id) = proxy_session_id else {
                    return Ok(None);
                };
                let Some(user_key) = proxy_users.get(&proxy_session_id) else {
                    return Ok(None);
                };
                let Some(session) = sessions.get(user_key) else {
                    return Ok(None);
                };
                session
                    .send_proxy_session_response_to_client_tokio(
                        socket,
                        proxy_session_id,
                        target,
                        port,
                        payload,
                    )
                    .await
            }
        }
    }

    fn current_auth(&self) -> Option<&SessionAuth> {
        match &self.mode {
            ShadowsocksInboundUdpResponderMode::Single(_) => None,
            ShadowsocksInboundUdpResponderMode::Profile { current_auth, .. } => {
                current_auth.as_ref()
            }
        }
    }
}

#[cfg(feature = "crypto")]
#[async_trait::async_trait]
impl DatagramUdpResponder<std::sync::Arc<tokio::net::UdpSocket>>
    for ShadowsocksInboundUdpResponder
{
    fn auth(&self) -> Option<&SessionAuth> {
        self.current_auth()
    }

    fn shared_listener(&self) -> bool {
        true
    }

    fn client_addr(&self) -> Option<std::net::SocketAddr> {
        self.pending_client
    }

    async fn read_inbound_dispatch(
        &mut self,
        socket: &std::sync::Arc<tokio::net::UdpSocket>,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        self.read_inbound_dispatch_from_socket_tokio(socket.as_ref())
            .await
            .map(Some)
    }

    fn on_dispatch_success(&mut self, session_id: u64, dispatch: &InboundUdpDispatch) {
        self.record_pending_dispatch_success(session_id, dispatch.client_session_id());
    }

    async fn write_response_for_session(
        &mut self,
        socket: &std::sync::Arc<tokio::net::UdpSocket>,
        session_id: Option<u64>,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        self.maintain();
        self.touch_response(session_id);
        self.send_response_for_target_proxy_session_to_client_tokio(
            socket.as_ref(),
            session_id,
            target,
            port,
            payload,
        )
        .await
    }
}
