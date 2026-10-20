//! Shared authenticated facts precede either host TCP admission or legacy routes.
use super::*;
use zero_core::request::Destination;

pub(crate) struct RoutedRequest {
    command: u8,
    destination: Destination,
    id: [u8; 16],
    flow: Option<&'static str>,
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
pub(super) struct AuthenticatedRequest {
    request: RoutedRequest,
    auth: SessionAuth,
    testseed: [u32; 4],
    reverse_tag: Option<String>,
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
pub(crate) struct AuthenticatedTcp<S> {
    pub(crate) destination: Destination,
    pub(crate) auth: SessionAuth,
    pub(crate) stream: S,
}
pub(crate) enum AuthenticatedClient<S> {
    Tcp(Box<AuthenticatedTcp<S>>),
    Legacy(Box<VlessAcceptedClient<S>>),
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
    pub(crate) async fn accept_standard_client_owned<S: AsyncSocket>(
        &self,
        mut stream: S,
    ) -> Result<AuthenticatedClient<S>, VlessClientAcceptError<S>> {
        let users = self
            .users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let accepted = match VlessInbound
            .accept_authenticated_facts(&mut stream, &VlessConfiguredUsers::new(users.as_ref()))
            .await
        {
            Ok(accepted) => accepted,
            Err(error) => return Err(VlessClientAcceptError::new(error, stream)),
        };
        // Vision, Zero AEAD, MUX, UDP and reverse control keep their original
        // codec/response/executor path. Plain TCP does not construct a Session.
        if accepted.request.command == CMD_TCP
            && accepted.request.flow.is_none()
            && accepted.reverse_tag.is_none()
        {
            Ok(AuthenticatedClient::Tcp(Box::new(AuthenticatedTcp {
                destination: accepted.request.destination,
                auth: accepted.auth,
                stream,
            })))
        } else {
            Ok(AuthenticatedClient::Legacy(Box::new(
                VlessAcceptedClient::new(accepted.into_legacy(), stream),
            )))
        }
    }
}
