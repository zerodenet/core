//! Transitional ingress adapter; admission, routing and relay retain one owner.
mod io;
mod message;
use super::TcpIngressRuntime;
use zero_core::{
    request::{Destination, RequestFacts},
    Network, ProtocolType, Session, SessionAuth,
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{inbound::AcceptedStream, ChannelError};

impl TcpIngressRuntime {
    pub(crate) fn accepted_session(
        &self,
        facts: &RequestFacts,
        authentication: Option<SessionAuth>,
        identity: &zero_engine::SessionIdentity,
    ) -> Result<Session, EngineError> {
        let ingress = self
            .services
            .execution()
            .ingress
            .ok_or_else(|| std::io::Error::other("standard inbound has no actual role"))?;
        if facts.session != identity.id()
            || facts.inbound != ingress.role.role
            || facts.effective_source != self.source_addr()
            || facts.local_endpoint != self.local_addr()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "accepted request does not belong to its actual host identity",
            )
            .into());
        }
        let target = facts
            .target
            .as_ref()
            .ok_or_else(|| std::io::Error::other("stream/message request has no destination"))?;
        let mut session = Session::new(
            identity.id().0,
            target.address.clone(),
            target.port,
            Network::Tcp,
            facts.protocol,
        );
        session.sni = facts.sni.clone();
        session.inbound_peer_identity = facts.peer.as_ref().map(|peer| peer.0.clone());
        if let Some(auth) = authentication {
            session.apply_auth(auth);
        }
        if crate::component::request::facts(&session, &ingress).subject != facts.subject {
            return Err(std::io::Error::other(
                "authenticated request subject differs from its host-owned facts",
            )
            .into());
        }
        Ok(session)
    }

    pub(crate) async fn serve_accepted_stream(
        &self,
        accepted: Box<dyn AcceptedStream>,
        authentication: Option<SessionAuth>,
        identity: zero_engine::SessionIdentity,
    ) -> Result<(), EngineError> {
        let session = self.accepted_session(accepted.facts(), authentication, &identity)?;
        super::super::lifecycle::serve::serve_with_identity(
            self,
            session,
            io::PendingStream::new(accepted),
            &io::Protocol,
            Some(&identity),
        )
        .await
    }

    pub(crate) async fn serve_standard_stream(
        &self,
        protocol: ProtocolType,
        destination: Destination,
        auth: Option<SessionAuth>,
        sni: Option<String>,
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
        // Carrier metadata is captured before protocol handoff and becomes
        // part of the host-issued facts used by the existing routing path.
        session.sni = sni;
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
