use crate::{outbound::TrojanTcpConnectRequest, transport::TrojanOutboundBuildOptionsRef};
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    sync::{Arc, OnceLock},
};
use zero_core::{request::Destination, Address, Error};
use zero_traits::{DialPolicy, GrpcTransportProfile, WebSocketTransportProfile};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};

/// One validated non-MUX Stream configuration; credential material is private.
pub struct Options {
    pub(super) endpoint: Destination,
    pub(super) server: String,
    pub(super) password: [u8; crate::shared::PASSWORD_HASH_LEN],
    pub(super) tls: OwnedClientTlsProfile,
    pub(super) ws: Option<OwnedWebSocketProfile>,
    pub(super) grpc: Option<OwnedGrpcProfile>,
    pub(super) dial: DialPolicy,
    pub(super) identity: Arc<[u8]>,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrojanOptions")
            .field("endpoint", &self.endpoint)
            .field("dial", &self.dial)
            .field("ws", &self.ws.is_some())
            .field("grpc", &self.grpc.is_some())
            .finish_non_exhaustive()
    }
}
impl Options {
    pub fn from_options_refs<TWs, TGrpc>(
        input: TrojanOutboundBuildOptionsRef<'_, TWs, TGrpc>,
        dial: DialPolicy,
    ) -> Result<Self, Error>
    where
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        if input.protocol.mux_concurrency.is_some() {
            return Err(Error::Unsupported(
                "Trojan MUX requires its shared resource owner",
            ));
        }
        if input.ws.is_some_and(|ws| ws.browser_dialer().is_some()) {
            return Err(Error::Unsupported(
                "Trojan browser dialer requires its resource owner",
            ));
        }
        if input.server.is_empty() || input.port == 0 {
            return Err(Error::Config("Trojan server and port are required"));
        }
        if input.protocol.password.is_empty() {
            return Err(Error::Config("Trojan password is required"));
        }
        if input.ws.is_some() && input.grpc.is_some() {
            return Err(Error::Config("trojan: ws and grpc are mutually exclusive"));
        }
        dial.validate()
            .map_err(|_| Error::Config("invalid Trojan dial policy"))?;
        let address = match zero_core::address::parse_ip_literal(input.server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(input.server.to_owned()),
        };
        let endpoint = Destination::new(address, input.port);
        let request = TrojanTcpConnectRequest::from_config(
            input.protocol.password,
            input.protocol.sni,
            input.protocol.insecure,
            Some(input.protocol.client_fingerprint.unwrap_or("chrome")),
        );
        let mut tls = OwnedClientTlsProfile::from_profile(&request.owned_tls_profile());
        if input.grpc.is_some() {
            tls.alpn = vec!["h2".into()];
        }
        let ws = input.ws.map(OwnedWebSocketProfile::from_profile);
        let grpc = input.grpc.map(OwnedGrpcProfile::from_profile);
        // Keyed process-local equality hints do not expose a password dictionary
        // hash. The host still compares complete source configuration on reuse.
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::with_capacity(16);
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            endpoint.hash(&mut hash);
            input.protocol.password.hash(&mut hash);
            format!("{tls:?}{ws:?}{grpc:?}").hash(&mut hash);
            dial.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            endpoint,
            server: input.server.to_owned(),
            password: crate::shared::password_hash(input.protocol.password),
            tls,
            ws,
            grpc,
            dial,
            identity: identity.into(),
        })
    }
}
