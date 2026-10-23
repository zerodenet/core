use zero_core::{request::Destination, Address, Network, ProtocolType, Session, SessionAuth};

/// Authenticated command facts before the host assigns a business identity.
#[derive(Clone)]
pub struct VmessRequestFacts {
    pub destination: Destination,
    pub network: Network,
    pub auth: SessionAuth,
}
impl VmessRequestFacts {
    pub(crate) fn is_mux(&self) -> bool {
        self.network == Network::Tcp
            && matches!(&self.destination.address, Address::Domain(domain) if domain == crate::shared::MUX_COOL_DOMAIN)
            && self.destination.port == crate::shared::MUX_COOL_PORT
    }

    /// Explicit compatibility with the existing UDP/Mux and public Session API.
    pub(crate) fn into_legacy_session(self) -> Session {
        let mut session = Session::new(
            0,
            self.destination.address,
            self.destination.port,
            self.network,
            ProtocolType::new("vmess"),
        );
        session.apply_auth(self.auth);
        session
    }
}
