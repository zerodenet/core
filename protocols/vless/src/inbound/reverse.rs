use super::*;

impl VlessInbound {
    pub(super) async fn accept_routed_request<S: AsyncSocket, A: VlessUserStore>(
        &self,
        stream: &mut S,
        users: &A,
    ) -> Result<VlessAcceptedSession, Error> {
        let (mut session, id, flow, reverse) = read_routed_request(stream).await?;
        let user = users
            .find_user(&id)
            .ok_or(Error::Unsupported("VLESS user is not authorized"))?;
        // Match the command, never a user-controlled target domain.
        if reverse != user.reverse_tag.is_some() {
            return Err(Error::Unsupported(
                "VLESS reverse user requires the Rvs command",
            ));
        }
        validate_user_flow(user.flow, flow)?;
        if crate::flow::is_vision_flow(flow) && stream.transport_bypass_control().is_none() {
            return Err(Error::Unsupported(
                "Vision requires TLS 1.3, REALITY or VLESS Encryption",
            ));
        }
        let testseed = user.testseed;
        let mut auth = SessionAuth::new("vless");
        auth.principal_key = user.principal_key;
        auth.up_bps = user.up_bps;
        auth.down_bps = user.down_bps;
        auth.device_limit = user.device_limit;
        auth.quota_remaining_bytes = user.quota_remaining_bytes;
        auth.policy_revision = user.policy_revision;
        session.apply_auth(auth);
        let mut accepted = VlessAcceptedSession::new(session, id, flow, testseed);
        accepted.reverse_tag = user.reverse_tag;
        Ok(accepted)
    }
}

impl VlessInboundProfile {
    pub(crate) fn with_portals(mut self, portals: crate::reverse::PortalRegistry) -> Self {
        self.portals = portals;
        self
    }
    // Err returns ownership for the ordinary route; keep that fast path allocation-free.
    #[allow(clippy::result_large_err)]
    pub(crate) fn prepare_reverse<S>(
        &self,
        accepted: VlessAcceptedClient<S>,
        backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<Box<dyn zero_core::inbound::InboundControlSession>, VlessAcceptedClient<S>>
    where
        S: AsyncSocket
            + zero_core::InboundFallbackCapture
            + tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + Send
            + Unpin
            + 'static,
        <S as zero_core::InboundFallbackCapture>::Stream:
            zero_platform_tokio::ClientStream + 'static,
    {
        let Some(tag) = accepted.accepted.reverse_tag.as_deref() else {
            return Err(accepted);
        };
        let portal = self.portals.portal(tag);
        let VlessAcceptedClient { accepted, stream } = accepted;
        // Authentication is complete. The control carrier can live for days;
        // fallback recording must not retain its subsequent MUX traffic.
        let (stream, recorded) = stream.into_fallback_replay_parts();
        drop(recorded);
        let accepted = VlessAcceptedClient::new(accepted, stream);
        Ok(Box::new(ReverseControl {
            accepted,
            portal,
            backlog,
        }))
    }
}

struct ReverseControl<S> {
    accepted: VlessAcceptedClient<S>,
    portal: crate::reverse::Portal,
    backlog: crate::mux::MuxResponseBacklogPolicy,
}
impl<S> zero_core::inbound::InboundControlSession for ReverseControl<S>
where
    S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    fn auth(&self) -> Option<&SessionAuth> {
        self.accepted.accepted.session.auth.as_ref()
    }
    fn run(
        self: Box<Self>,
    ) -> core::pin::Pin<Box<dyn core::future::Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async move {
            let Self {
                accepted,
                portal,
                backlog,
            } = *self;
            let (_, uuid, flow, testseed, mut stream) = accepted.into_parts();
            // Runtime grants principal admission before a response or any MUX task.
            zero_traits::AsyncSocket::write_all(&mut stream, &[crate::shared::VLESS_VERSION, 0])
                .await
                .map_err(|_| Error::Io("Rvs response write failed"))?;
            let stream = if crate::flow::is_vision_flow(flow) {
                VlessInboundTcpStream::vision_after_response(stream, uuid, testseed)
            } else {
                VlessInboundTcpStream::plain(stream)
            };
            portal.attach(stream, backlog)?.run().await
        })
    }
}

impl<S> VlessAcceptedClient<S> {
    pub(crate) fn with_inbound_endpoints(
        mut self,
        source: Option<std::net::SocketAddr>,
        local: Option<std::net::SocketAddr>,
    ) -> Self {
        let address = |ip: std::net::IpAddr| match ip {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        if let Some(source) = source {
            self.accepted.session.source_ip = Some(address(source.ip()));
            self.accepted.session.source_port = Some(source.port());
        }
        self.accepted.session.inbound_local =
            local.map(|local| (address(local.ip()), local.port()));
        self
    }
}
