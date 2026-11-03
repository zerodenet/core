//! Transitional ingress adapter; admission, routing and relay retain one owner.
mod io;
mod message;
use super::TcpIngressRuntime;
use zero_core::{request::RequestFacts, Network, Session, SessionAuth};
use zero_engine::EngineError;
use zero_platform_tokio::contract::inbound::AcceptedStream;

impl TcpIngressRuntime {
    pub(crate) fn accepted_session(
        &self,
        facts: &RequestFacts,
        authentication: Option<SessionAuth>,
        identity: &zero_engine::SessionIdentity,
    ) -> Result<Session, EngineError> {
        self.services.ensure_configuration_committed()?;
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
}
