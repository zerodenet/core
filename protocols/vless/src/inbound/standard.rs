//! Shared authenticated facts precede either host TCP admission or legacy routes.
use super::*;
use zero_core::request::Destination;

pub(crate) struct RoutedRequest {
    pub(crate) command: u8,
    pub(crate) destination: Destination,
    pub(crate) id: [u8; 16],
    pub(crate) flow: Option<&'static str>,
    reverse: bool,
}
impl RoutedRequest {
    pub(super) fn new(
        command: u8,
        target: Address,
        port: u16,
        id: [u8; 16],
        flow: Option<&'static str>,
        reverse: bool,
    ) -> Result<Self, Error> {
        match command {
            CMD_TCP | CMD_UDP | CMD_MUX => {}
            crate::reverse::COMMAND if reverse => {}
            _ => return Err(Error::Unsupported("VLESS command is not supported")),
        }
        let destination = if command == CMD_MUX {
            Destination::new(Address::Domain(String::new()), 0)
        } else {
            Destination::new(target, port)
        };
        Ok(Self {
            command,
            destination,
            id,
            flow,
            reverse,
        })
    }
    pub(super) fn into_legacy_parts(self) -> (Session, [u8; 16], Option<&'static str>, bool) {
        let session = Session::new(
            0,
            self.destination.address,
            self.destination.port,
            if self.command == CMD_UDP {
                Network::Udp
            } else {
                Network::Tcp
            },
            ProtocolType::new("vless"),
        );
        (session, self.id, self.flow, self.reverse)
    }
}
pub(crate) struct AuthenticatedRequest {
    pub(crate) request: RoutedRequest,
    pub(crate) auth: SessionAuth,
    pub(crate) testseed: [u32; 4],
    pub(crate) reverse_tag: Option<String>,
}
impl AuthenticatedRequest {
    pub(super) fn into_legacy(self) -> VlessAcceptedSession {
        let (mut session, id, flow, _) = self.request.into_legacy_parts();
        session.apply_auth(self.auth);
        let mut accepted = VlessAcceptedSession::new(session, id, flow, self.testseed);
        accepted.reverse_tag = self.reverse_tag;
        accepted
    }
}
impl VlessInbound {
    pub(super) async fn accept_authenticated_facts<S: AsyncSocket, A: VlessUserStore>(
        &self,
        stream: &mut S,
        users: &A,
    ) -> Result<AuthenticatedRequest, Error> {
        let request = read_routed_facts(stream).await?;
        let user = users
            .find_user(&request.id)
            .ok_or(Error::Unsupported("VLESS user is not authorized"))?;
        if request.reverse != user.reverse_tag.is_some() {
            return Err(Error::Unsupported(
                "VLESS reverse user requires the Rvs command",
            ));
        }
        validate_user_flow(user.flow, request.flow)?;
        if crate::flow::is_vision_flow(request.flow) && stream.transport_bypass_control().is_none()
        {
            return Err(Error::Unsupported(
                "Vision requires TLS 1.3, REALITY or VLESS Encryption",
            ));
        }
        let mut auth = SessionAuth::new("vless");
        auth.principal_key = user.principal_key;
        auth.up_bps = user.up_bps;
        auth.down_bps = user.down_bps;
        auth.device_limit = user.device_limit;
        auth.quota_remaining_bytes = user.quota_remaining_bytes;
        auth.policy_revision = user.policy_revision;
        Ok(AuthenticatedRequest {
            request,
            auth,
            testseed: user.testseed,
            reverse_tag: user.reverse_tag,
        })
    }
}
impl VlessInboundProfile {
    pub(crate) async fn accept_standard_facts<S: AsyncSocket>(
        &self,
        stream: &mut S,
    ) -> Result<AuthenticatedRequest, Error> {
        let users = self.users.read().unwrap_or_else(|e| e.into_inner()).clone();
        VlessInbound
            .accept_authenticated_facts(stream, &VlessConfiguredUsers::new(users.as_ref()))
            .await
    }
}
