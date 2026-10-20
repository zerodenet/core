use crate::{parse_uuid, transport::VmessOutboundBuildOptionsRef, VmessCipher};
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    sync::{Arc, OnceLock},
};
use zero_core::{request::Destination, Address, Error};
use zero_traits::{
    ClientTlsProfile, DialPolicy, EchConfigSource, GrpcTransportProfile, WebSocketTransportProfile,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};

pub struct Options {
    pub(super) endpoint: Destination,
    pub(super) server: String,
    pub(super) uuid: [u8; 16],
    pub(super) cipher: VmessCipher,
    pub(super) tls: Option<OwnedClientTlsProfile>,
    pub(super) ws: Option<OwnedWebSocketProfile>,
    pub(super) grpc: Option<OwnedGrpcProfile>,
    pub(super) dial: DialPolicy,
    pub(super) identity: Arc<[u8]>,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VmessOptions")
            .field("endpoint", &self.endpoint)
            .field("cipher", &self.cipher)
            .field("dial", &self.dial)
            .finish_non_exhaustive()
    }
}
impl Options {
    pub fn from_options_refs<TTls, TWs, TGrpc>(
        input: VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        if input.protocol.mux_concurrency.is_some() {
            return Err(Error::Unsupported(
                "VMess MUX requires its shared resource owner",
            ));
        }
        if input.server.is_empty() || input.port == 0 {
            return Err(Error::Config("VMess server and port are required"));
        }
        if input.ws.is_some() && input.grpc.is_some() {
            return Err(Error::Config("vmess: ws and grpc are mutually exclusive"));
        }
        if input.ws.is_some_and(|ws| ws.browser_dialer().is_some()) {
            return Err(Error::Unsupported(
                "VMess browser dialer requires its resource owner",
            ));
        }
        let uuid = parse_uuid(input.protocol.id)?;
        let cipher = VmessCipher::from_name(input.protocol.cipher)
            .ok_or(Error::Config("vmess unknown cipher"))?;
        let tls = input.tls.map(OwnedClientTlsProfile::from_profile);
        if let Some(tls) = &tls {
            if tls.ca_cert_path.is_some() {
                return Err(Error::Unsupported(
                    "VMess CA file requires an explicit File grant",
                ));
            }
            if matches!(
                tls.options.ech.source().map_err(Error::Config)?,
                Some(EchConfigSource::Dns { .. })
            ) {
                return Err(Error::Unsupported(
                    "VMess ECH DNS requires a native DNS service",
                ));
            }
        }
        dial.validate()
            .map_err(|_| Error::Config("invalid VMess dial policy"))?;
        let address = match zero_core::address::parse_ip_literal(input.server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(input.server.into()),
        };
        let endpoint = Destination::new(address, input.port);
        let ws = input.ws.map(OwnedWebSocketProfile::from_profile);
        let grpc = input.grpc.map(OwnedGrpcProfile::from_profile);
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::with_capacity(16);
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            endpoint.hash(&mut hash);
            uuid.hash(&mut hash);
            cipher.hash(&mut hash);
            format!("{tls:?}{ws:?}{grpc:?}").hash(&mut hash);
            dial.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            endpoint,
            server: input.server.into(),
            uuid,
            cipher,
            tls,
            ws,
            grpc,
            dial,
            identity: identity.into(),
        })
    }
}
