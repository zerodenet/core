//! One accepted value and its actual shared Source dependency.
use super::*;
pub struct Prepared {
    pub provider: Arc<crate::transport::BrowserProviderOptions>,
    options: Options,
}
pub struct Options {
    pub(super) protocol: crate::component::Options,
    pub(super) plan: crate::transport::OwnedVlessOutboundTransportPlan,
    pub(super) provider: Arc<crate::transport::BrowserProviderOptions>,
    pub(super) dependency: Option<zero_core::component::ComponentId>,
    pub(super) origins: std::num::NonZeroUsize,
    pub(super) identity: Arc<[u8]>,
}
impl Prepared {
    pub fn bind_provider(mut self, dependency: zero_core::component::ComponentId) -> Options {
        self.options.dependency = Some(dependency);
        let mut identity = self.options.identity.to_vec();
        identity.extend_from_slice(&dependency.0.to_le_bytes());
        self.options.identity = identity.into();
        self.options
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
        let (uuid, flow, testseed) = parts.protocol;
        let server = match &parts.endpoint.address {
            zero_core::Address::Domain(name) => name.clone(),
            zero_core::Address::Ipv4(address) => std::net::Ipv4Addr::from(*address).to_string(),
            zero_core::Address::Ipv6(address) => std::net::Ipv6Addr::from(*address).to_string(),
        };
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
            endpoint: parts.endpoint,
            server,
            uuid,
            flow,
            testseed,
            reality: None,
            tls: None,
            ws: None,
            grpc: None,
            h2: None,
            http_upgrade: None,
            dial,
            identity: identity.clone().into(),
        };
        Ok(Some(Prepared {
            provider: parts.provider.clone(),
            options: Options {
                protocol,
                plan: parts.plan,
                provider: parts.provider,
                dependency: None,
                origins,
                identity: identity.into(),
            },
        }))
    }
    pub fn endpoint(&self) -> &zero_core::request::Destination {
        &self.protocol.endpoint
    }
    pub fn dial(&self) -> &zero_traits::DialPolicy {
        &self.protocol.dial
    }
}
