#[cfg(feature = "runtime")]
use zero_core::Address;
use zero_core::{request::Destination, Network, ProtocolType, Session, SessionAuth};

/// Protocol facts after complete authentication and command/address parsing.
/// No business identity exists until the host accepts an ordinary TCP request.
pub struct TrojanRequestFacts {
    pub destination: Destination,
    pub network: Network,
    pub auth: SessionAuth,
}
impl TrojanRequestFacts {
    #[cfg(feature = "runtime")]
    pub(crate) fn is_mux(&self) -> bool {
        self.network == Network::Tcp
            && matches!(&self.destination.address, Address::Domain(domain) if domain == crate::shared::MUX_COOL_DOMAIN)
            && self.destination.port == crate::shared::MUX_COOL_PORT
    }

    /// Compatibility for the existing UDP/Mux executors and legacy public API.
    /// The ordinary production TCP listener does not enter this conversion.
    pub(crate) fn into_legacy_session(self) -> Session {
        let mut session = Session::new(
            0,
            self.destination.address,
            self.destination.port,
            self.network,
            ProtocolType::new("trojan"),
        );
        session.apply_auth(self.auth);
        session
    }
}
