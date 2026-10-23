//! Original handshake state and destination, before host business admission.
use super::*;
use zero_core::request::Destination;

/// Internal authenticated/decrypted TCP request. Public legacy APIs convert at
/// their edge; the production facts path never creates a placeholder Session.
pub(crate) struct ShadowsocksRequest {
    pub(crate) destination: Destination,
    pub(crate) legacy: Option<crate::shared::legacy::LegacyCipherState>,
    pub(crate) remaining_payload: Vec<u8>,
    pub(crate) session_key: Vec<u8>,
    pub(crate) cipher: crate::shared::CipherKind,
    pub(crate) next_upload_nonce: u128,
    pub(crate) request_salt: Vec<u8>,
}
impl ShadowsocksRequest {
    pub(super) fn into_legacy_accept(self) -> ShadowsocksAccept {
        ShadowsocksAccept {
            session: Session::new(
                0,
                self.destination.address,
                self.destination.port,
                Network::Tcp,
                ProtocolType::new("shadowsocks"),
            ),
            legacy: self.legacy,
            remaining_payload: self.remaining_payload,
            session_key: self.session_key,
            cipher: self.cipher,
            next_upload_nonce: self.next_upload_nonce,
            request_salt: self.request_salt,
        }
    }
}
impl From<ShadowsocksAccept> for ShadowsocksRequest {
    fn from(accept: ShadowsocksAccept) -> Self {
        Self {
            destination: Destination::new(accept.session.target, accept.session.port),
            legacy: accept.legacy,
            remaining_payload: accept.remaining_payload,
            session_key: accept.session_key,
            cipher: accept.cipher,
            next_upload_nonce: accept.next_upload_nonce,
            request_salt: accept.request_salt,
        }
    }
}
