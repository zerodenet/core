use crate::{parse_uuid, transport::VlessOutboundBuildOptionsRef};
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
    pub(super) flow: Option<&'static str>,
    pub(super) testseed: [u32; 4],
    pub(super) tls: Option<OwnedClientTlsProfile>,
    pub(super) ws: Option<OwnedWebSocketProfile>,
    pub(super) grpc: Option<OwnedGrpcProfile>,
    pub(super) dial: DialPolicy,
    pub(super) identity: Arc<[u8]>,
}
impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VlessOptions")
            .field("endpoint", &self.endpoint)
            .field("dial", &self.dial)
            .finish_non_exhaustive()
    }
}
impl Options {
    pub fn from_options_refs<TTls, TWs, TGrpc, TH2, THttp, TSplit>(
        input: VlessOutboundBuildOptionsRef<'_, TTls, TWs, TGrpc, TH2, THttp, TSplit>,
        dial: DialPolicy,
    ) -> Result<Self, Error>
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
        TH2: zero_traits::H2TransportProfile + ?Sized,
        THttp: zero_traits::HttpUpgradeTransportProfile + ?Sized,
        TSplit: zero_traits::SplitHttpTransportProfile + ?Sized,
    {
        if input.protocol.mux_concurrency.is_some() || input.protocol.xudp_concurrency.is_some() {
            return Err(Error::Unsupported(
                "VLESS MUX/XUDP requires its shared resource owner",
            ));
        }
        let flow = input
            .protocol
            .flow
            .map(crate::flow::parse_flow)
            .transpose()?;
        if flow.is_some() && !crate::flow::is_vision_flow(flow) {
            return Err(Error::Unsupported(
                "VLESS flow requires its protocol stream integration",
            ));
        }
        if crate::flow::is_vision_flow(flow)
            && (input.tls.is_none() || input.ws.is_some() || input.grpc.is_some())
        {
            return Err(Error::Unsupported(
                "Vision requires a real switchable TLS carrier",
            ));
        }
        if input.protocol.testpre != 0 || (flow.is_none() && !input.protocol.testseed.is_empty()) {
            return Err(Error::Unsupported(
                "VLESS preconnect/padding requires its owning lifecycle",
            ));
        }
        let testseed = crate::validation::normalize_vision_testseed(input.protocol.testseed)
            .map_err(Error::Config)?;
        if let Some(encryption) = input.protocol.encryption.filter(|value| *value != "none") {
            crate::encryption::config::EncryptionConfig::client(encryption)
                .map_err(Error::Config)?;
            return Err(Error::Unsupported(
                "VLESS Encryption requires its protocol state owner",
            ));
        }
        if input.protocol.reality.is_some()
            || input.protocol.quic.is_some()
            || input.final_mask.is_some()
            || input.mkcp.is_some()
            || input.hysteria.is_some()
            || input.download.is_some()
            || input.h2.is_some()
            || input.http_upgrade.is_some()
            || input.split_http.is_some()
        {
            return Err(Error::Unsupported(
                "VLESS carrier requires a dedicated standard role integration",
            ));
        }
        if input.server.is_empty() || input.port == 0 {
            return Err(Error::Config("VLESS server and port are required"));
        }
        if input.ws.is_some() && input.grpc.is_some() {
            return Err(Error::Config("vless: ws and grpc are mutually exclusive"));
        }
        if input.ws.is_some_and(|ws| ws.browser_dialer().is_some()) {
            return Err(Error::Unsupported(
                "VLESS browser dialer requires its resource owner",
            ));
        }
        let uuid = parse_uuid(input.protocol.id)?;
        let tls = input.tls.map(|profile| {
            crate::transport::client_tls_for_stream(
                profile,
                input.ws.is_some(),
                input.grpc.is_some(),
            )
        });
        if let Some(tls) = &tls {
            if tls.ca_cert_path.is_some() {
                return Err(Error::Unsupported(
                    "VLESS CA file requires an explicit File grant",
                ));
            }
            if matches!(
                tls.options.ech.source().map_err(Error::Config)?,
                Some(EchConfigSource::Dns { .. })
            ) {
                return Err(Error::Unsupported(
                    "VLESS ECH DNS requires a native DNS service",
                ));
            }
        }
        dial.validate()
            .map_err(|_| Error::Config("invalid VLESS dial policy"))?;
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
            flow.hash(&mut hash);
            testseed.hash(&mut hash);
            format!("{tls:?}{ws:?}{grpc:?}").hash(&mut hash);
            dial.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            endpoint,
            server: input.server.into(),
            uuid,
            flow,
            testseed,
            tls,
            ws,
            grpc,
            dial,
            identity: identity.into(),
        })
    }
}

#[cfg(test)]
#[path = "options/tests.rs"]
mod tests;
