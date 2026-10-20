//! Protocol-authenticated command facts, before any business identity allocation.
use alloc::boxed::Box;
use zero_core::{request::Destination, Network, ProtocolType, Session, SessionAuth};

use super::{Socks5Request, Socks5UdpAssociateRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Socks5Command {
    Connect(Destination),
    /// This is a client endpoint hint, never a fabricated business destination.
    UdpAssociate(Socks5UdpAssociateRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5CommandFacts {
    pub command: Socks5Command,
    /// The actual verification result, including configured limits and an absent
    /// principal key. No username is substituted for an absent configured key.
    pub auth: Option<SessionAuth>,
}

impl Socks5CommandFacts {
    pub(super) fn into_legacy(self) -> Socks5Request {
        match self.command {
            Socks5Command::Connect(destination) => {
                let mut session = Session::new(
                    0,
                    destination.address,
                    destination.port,
                    Network::Tcp,
                    ProtocolType::new("socks5"),
                );
                if let Some(auth) = self.auth {
                    session.apply_auth(auth);
                }
                Socks5Request::Connect(Box::new(session))
            }
            // Preserve the existing legacy association shape. The new facts
            // retain authentication for a future native association consumer.
            Socks5Command::UdpAssociate(request) => Socks5Request::UdpAssociate(request),
        }
    }
}

pub(super) fn authentication(
    auth: &impl super::Socks5PasswordAuth,
    username: Option<&str>,
) -> Option<SessionAuth> {
    username.map(|name| {
        let mut facts = SessionAuth::new("socks5");
        facts.principal_key = auth.principal_key_for(name);
        (facts.up_bps, facts.down_bps) = auth.rate_limit_for(name);
        facts
    })
}
