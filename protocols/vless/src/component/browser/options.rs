//! One accepted value and its actual shared Browser Source dependency.
use super::*;
mod carrier;
use zero_transport::browser_dialer::component::{self as shared_browser, CarrierProfile};

pub struct Prepared {
    pub provider: Arc<crate::transport::BrowserProviderOptions>,
    protocol: crate::component::Options,
    plan: crate::transport::OwnedVlessOutboundTransportPlan,
    carrier_profile: CarrierProfile,
    secure: bool,
    origins: std::num::NonZeroUsize,
    identity: Vec<u8>,
}

pub struct Options {
    pub(super) protocol: crate::component::Options,
    pub(super) plan: crate::transport::OwnedVlessOutboundTransportPlan,
    pub(super) browser: Arc<shared_browser::Options>,
    pub(super) identity: Arc<[u8]>,
    pub(super) carrier: Option<crate::component::quic::TransportCarrier>,
}

impl Prepared {
    pub fn bind_provider(mut self, dependency: zero_core::component::ComponentId) -> Options {
        self.identity.extend_from_slice(&dependency.0.to_le_bytes());
        let identity: Arc<[u8]> = self.identity.into();
        let browser = Arc::new(shared_browser::Options {
            endpoint: self.protocol.endpoint.clone(),
            dial: self.protocol.dial.clone(),
            server: self.protocol.server.clone(),
            secure: self.secure,
            carrier: self.carrier_profile,
            provider: self.provider,
            dependency: Some(dependency),
            origins: self.origins,
        });
        Options {
            protocol: self.protocol,
            plan: self.plan,
            browser,
            identity,
            carrier: None,
        }
    }
}

impl Options {
    pub fn from_accepted_leaf(
        leaf: &crate::transport::VlessOutboundLeaf,
        dial: zero_traits::DialPolicy,
        origins: std::num::NonZeroUsize,
    ) -> Result<Option<Prepared>, zero_transport::RuntimeError> {
        let Some(parts) = leaf.owned_browser_parts()? else {
            return Ok(None);
        };
        dial.validate().map_err(std::io::Error::other)?;
        origins
            .get()
            .checked_mul(2)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let (carrier_profile, server, secure) =
            if let Some((ws, server, _, secure)) = parts.plan.browser_websocket() {
                (
                    CarrierProfile::WebSocket(ws.clone()),
                    server.to_owned(),
                    secure,
                )
            } else if let Some((http, server, _, secure)) = parts.plan.browser_http() {
                (
                    CarrierProfile::SplitHttp(Box::new(http.clone())),
                    server.to_owned(),
                    secure,
                )
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Browser Dialer has no supported carrier",
                )
                .into());
            };
        let (uuid, flow, testseed) = parts.protocol;
        let mut identity = parts.identity.to_vec();
        identity.extend_from_slice(b"vless.actual.browser.source.v1");
        identity.extend_from_slice(&origins.get().to_le_bytes());
        struct DialIdentity(blake3::Hasher);
        impl std::fmt::Write for DialIdentity {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                self.0.update(text.as_bytes());
                Ok(())
            }
        }
        use std::fmt::Write;
        let mut digest = DialIdentity(blake3::Hasher::new());
        write!(&mut digest, "{dial:?}").unwrap();
        identity.extend_from_slice(digest.0.finalize().as_bytes());
        let protocol = crate::component::Options {
            material: None,
            masks: None,
            endpoint: parts.endpoint,
            server,
            uuid,
            flow,
            testseed,
            reality: None,
            // The enclosing owned transport plan applies Encryption once.
            encryption: None,
            tls: None,
            ws: None,
            grpc: None,
            h2: None,
            http_upgrade: None,
            dial,
            identity: identity.clone().into(),
        };
        Ok(Some(Prepared {
            provider: parts.provider,
            protocol,
            plan: parts.plan,
            carrier_profile,
            secure,
            origins,
            identity,
        }))
    }
    pub fn endpoint(&self) -> &zero_core::request::Destination {
        &self.protocol.endpoint
    }
    pub fn dial(&self) -> &zero_traits::DialPolicy {
        &self.protocol.dial
    }
}
