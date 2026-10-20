//! Transitional ingress adapter; admission, routing and relay retain one owner.
mod io;
use super::TcpIngressRuntime;
use zero_core::{
    request::{Destination, RequestFacts},
    Network, ProtocolType, Session, SessionAuth,
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{inbound::AcceptedStream, ChannelError};

impl TcpIngressRuntime {
    pub(crate) async fn serve_standard_stream(
        &self,
        protocol: ProtocolType,
        destination: Destination,
        auth: Option<SessionAuth>,
        reply: impl FnOnce(RequestFacts) -> Result<Box<dyn AcceptedStream>, ChannelError> + Send,
    ) -> Result<(), EngineError> {
        // Mint only for a recognized business request. The sealed ticket stays
        // here; protocol code sees only its immutable actual request identity.
        let identity = self.services.engine().mint_session_identity()?;
        let mut session = Session::new(
            identity.id().0,
            destination.address,
            destination.port,
            Network::Tcp,
            protocol,
        );
        if let Some(auth) = auth {
            session.apply_auth(auth);
        }
        let ingress = self.services.execution().ingress.ok_or_else(|| {
            std::io::Error::other("standard inbound has no actual host role identity")
        })?;
        let mut facts = crate::component::request::facts(&session, &ingress);
        facts.effective_source = self.source_addr();
        facts.local_endpoint = self.local_addr();
        let expected = facts.clone();
        let accepted = reply(facts).map_err(std::io::Error::other)?;
        if accepted.facts() != &expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "accepted stream facts differ from the host request",
            )
            .into());
        }
        // Preserve the full authentication result, including scheme and
        // anonymous per-session rates, in the existing kernel policy path.
        // RequestFacts.subject contains only an actual configured principal.
        super::super::lifecycle::serve::serve_with_identity(
            self,
            session,
            io::PendingStream::new(accepted),
            &io::Protocol,
            Some(&identity),
        )
        .await
    }
}
