//! Neutral Browser ownership; protocol adapters still parse credentials/framing.
use crate::component::{
    background::{BackgroundCarrier, BackgroundNetwork},
    RegistrationError, ResourcePreparation,
};
use std::{num::NonZeroUsize, sync::Arc};
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
use zero_platform_tokio::contract::{
    component::*, lifecycle::BuildError, network::UpstreamBinding,
};
use zero_transport::browser_dialer::{component as carrier, registry};
pub(crate) mod mux;
pub(crate) fn mux_eligible(input: &OutboundConfig) -> bool {
    input.protocol.browser_mux_ws_listener().is_some()
}
/// Preserve the preexisting native TLS/WS semantics for profiles the browser
/// channel cannot express. Their complete profile stays on the standard native
/// owner; selecting this branch never retries a failed Browser acquisition.
pub(crate) fn native_compatibility(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vmess { ws: Some(ws), .. } | OutboundProtocolConfig::Trojan { ws: Some(ws), .. } if ws.browser_dialer.is_some())
        && !eligible(input)
        && !mux_eligible(input)
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    input.protocol.ordinary_browser_ws_listener().is_some()
}
pub(crate) struct Options<T> {
    inner: Arc<T>,
    carrier: Arc<carrier::Options>,
    tag: String,
    network: Arc<BackgroundNetwork>,
}
impl<T> Options<T> {
    pub(crate) fn new(
        inner: T,
        carrier: Arc<carrier::Options>,
        tag: String,
        network: Arc<BackgroundNetwork>,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            carrier,
            tag,
            network,
        }
    }
}
pub(crate) struct Factory<F>(pub F);
impl<F: ComponentFactory> ComponentFactory for Factory<F> {
    type Options = Options<F::Options>;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        self.0.describe(&options.inner)
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        context.background_carrier = Some(
            BackgroundCarrier {
                instance: context.instance(),
                network: options.network.clone(),
                tag: options.tag.clone(),
                initial: UpstreamBinding {
                    prefix: vec![],
                    dial: options.carrier.dial.clone(),
                    network_generation: zero_core::component::NetworkGeneration(
                        options.network.egress.generation(),
                    ),
                },
                endpoint: options.carrier.endpoint.clone(),
                wire_endpoint: options.carrier.endpoint.clone(),
            }
            .transport(),
        );
        self.0.build(options.inner.clone(), context)
    }
}
pub(crate) fn prepare(
    input: &OutboundConfig,
    resources: &mut ResourcePreparation<'_>,
    registry: &registry::Registry,
) -> Result<Arc<carrier::Options>, RegistrationError> {
    let (server, port, ws, secure) = match &input.protocol {
        OutboundProtocolConfig::Trojan {
            server, port, ws, ..
        } => (server, *port, ws.as_deref(), true),
        OutboundProtocolConfig::Vmess {
            server,
            port,
            ws,
            tls,
            ..
        } => (server, *port, ws.as_deref(), tls.is_some()),
        _ => unreachable!("Browser owning protocol projection"),
    };
    let ws = ws.expect("selected Browser WS");
    let settings = ws
        .browser_dialer
        .as_ref()
        .expect("selected Browser settings");
    settings
        .validate()
        .map_err(|e| ConfigError::InvalidOutbound(e.into()))?;
    let provider = Arc::new(
        registry::ProviderOptions::new(registry.access(settings.settings()))
            .map_err(|e| ConfigError::InvalidOutbound(e.to_string()))?,
    );
    let dependency = resources.share(
        "transport.browser.actual-source.v1",
        Arc::new(registry::ProviderFactory),
        provider.clone(),
    )?;
    let origins = NonZeroUsize::new(
        resources
            .candidate()
            .map(|c| c.runtime.effective_native_stream_max_originals())
            .unwrap_or(256),
    )
    .filter(|v| v.get() <= 4096)
    .ok_or_else(|| ConfigError::InvalidRuntime("invalid Browser original limit".into()))?;
    Ok(Arc::new(carrier::Options {
        endpoint: zero_core::request::Destination::new(
            match zero_core::address::parse_ip_literal(server) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(server.clone()),
            },
            port,
        ),
        server: server.clone(),
        secure,
        carrier: carrier::CarrierProfile::WebSocket(
            zero_transport::profile::OwnedWebSocketProfile::from_profile(ws),
        ),
        provider,
        dependency: Some(dependency),
        origins,
        dial: input.dial.to_policy(),
    }))
}

#[cfg(test)]
#[path = "tests/browser.rs"]
mod tests;
