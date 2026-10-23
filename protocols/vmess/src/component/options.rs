use crate::{parse_uuid, transport::VmessOutboundBuildOptionsRef, VmessCipher};
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};
use zero_core::{request::Destination, Address, Error};
use zero_traits::{
    ClientTlsProfile, DialPolicy, EchConfigSource, GrpcTransportProfile, WebSocketTransportProfile,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};

#[derive(Debug, Clone, Hash)]
pub(super) struct CaFile {
    pub(super) path: PathBuf,
    pub(super) source_dir: Option<PathBuf>,
    pub(super) max_bytes: usize,
}
pub struct Options {
    pub(super) endpoint: Destination,
    pub(super) server: String,
    pub(super) uuid: [u8; 16],
    pub(super) cipher: VmessCipher,
    pub(super) ca_file: Option<CaFile>,
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
        Self::from_options_refs_at(input, dial, None, None)
    }
    /// Library-only file material policy. No default file size limit or implicit
    /// grant is invented: callers must explicitly supply the finite bound.
    /// source_dir retains the ordinary lexical relative-path base semantics.
    pub fn from_options_refs_at<TTls, TWs, TGrpc>(
        input: VmessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc>,
        dial: DialPolicy,
        source_dir: Option<&Path>,
        ca_max_bytes: Option<NonZeroUsize>,
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
        if input.grpc.is_some() {
            return Err(Error::Unsupported(
                "VMess gRPC requires its carrier and registered driver owner",
            ));
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
        let ca_file = if let Some(path) = tls.as_ref().and_then(|tls| tls.ca_cert_path.as_deref()) {
            let max_bytes = ca_max_bytes
                .ok_or(Error::Unsupported(
                    "VMess CA file requires an explicit bounded File grant",
                ))?
                .get();
            if max_bytes.checked_add(1).is_none() {
                return Err(Error::Config("VMess CA File bound overflows its reader"));
            }
            if path.is_empty() {
                return Err(Error::Config("VMess CA File path must not be empty"));
            }
            zero_transport::tls::PreparedClient::validate_profile(
                tls.as_ref().expect("CA TLS profile"),
            )
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::Unsupported {
                    Error::Unsupported("VMess prepared CA does not support OpenSSL TLS")
                } else {
                    Error::Config("invalid VMess prepared TLS profile")
                }
            })?;
            let path = PathBuf::from(path);
            let path = if path.is_absolute() {
                path
            } else {
                source_dir.map(|base| base.join(&path)).unwrap_or(path)
            };
            Some(CaFile {
                path,
                source_dir: source_dir.map(Path::to_path_buf),
                max_bytes,
            })
        } else {
            None
        };
        if let Some(tls) = &tls {
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
            ca_file.hash(&mut hash);
            dial.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            endpoint,
            server: input.server.into(),
            uuid,
            cipher,
            ca_file,
            tls,
            ws,
            grpc,
            dial,
            identity: identity.into(),
        })
    }
}

#[cfg(test)]
#[path = "../../tests/component/options.rs"]
mod tests;
