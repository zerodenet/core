//! Actual accepted Browser settings select their one canonical Source owner.
use super::*;
use crate::component::background::{BackgroundCarrier, BackgroundNetwork};
use crate::component::{RegistrationError, ResourcePreparation};
use std::{num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};
pub(crate) struct Options {
    pub inner: Arc<::vless::component::browser::Options>,
    tag: String,
    network: Arc<BackgroundNetwork>,
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ::vless::component::browser::Factory.describe(&options.inner)
    }
    fn build(
        &self,
        options: Arc<Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        // The exact current Host plan supplies readonly original validation.
        // Browser acquisition itself uses only its sealed Accepted dependency;
        // this opener is not used to dial a replacement native socket.
        context.background_carrier = Some(
            BackgroundCarrier {
                instance: context.instance(),
                network: options.network.clone(),
                tag: options.tag.clone(),
                initial: UpstreamBinding {
                    prefix: vec![],
                    dial: options.inner.dial().clone(),
                    network_generation: zero_core::component::NetworkGeneration(
                        options.network.egress.generation(),
                    ),
                },
                endpoint: options.inner.endpoint().clone(),
                wire_endpoint: options.inner.endpoint().clone(),
            }
            .transport(),
        );
        ::vless::component::browser::Factory.build(options.inner.clone(), context)
    }
}
pub(super) fn eligible(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vless {
        mux_concurrency: None, xudp_concurrency: None, testpre: 0,
        reverse_tag: None, reverse_sniffing: None, ws, split_http, ..
    } if ws.as_ref().is_some_and(|ws| ws.browser_dialer.is_some())
        || split_http.as_ref().is_some_and(|http| http.browser_dialer.is_some()))
}
impl super::super::VlessAdapter {
    pub(super) fn browser_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
    ) -> Result<super::mux::Options, RegistrationError> {
        let OutboundProtocolConfig::Vless { server, port, .. } = &input.protocol else {
            unreachable!("actual Browser projection")
        };
        let refs = super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
            .expect("same accepted VLESS value");
        // The original leaf validates the entire Browser grammar before any
        // registry/host selection. No CA, settings or material is decoded twice.
        let leaf = ::vless::transport::VlessOutboundLeaf::from_options_refs(
            resources.candidate().and_then(|config| config.source_dir()),
            refs,
            &self.runtime,
        )
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        let limit = resources
            .candidate()
            .map(|config| config.runtime.effective_native_stream_max_originals())
            .unwrap_or(4);
        let limit = NonZeroUsize::new(limit).ok_or_else(|| {
            ConfigError::InvalidRuntime("zero actual Browser original limit".into())
        })?;
        let prepared = ::vless::component::browser::Options::from_accepted_leaf(
            &leaf,
            input.dial.to_policy(),
            limit,
        )
        .map_err(|source| {
            ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?
        .ok_or_else(|| {
            ConfigError::InvalidOutbound(format!(
                "outbound `{}` Browser carrier differs from its accepted leaf",
                input.tag
            ))
        })?;
        let provider = resources.share(
            "vless.actual.browser-source.v1",
            Arc::new(::vless::transport::BrowserProviderFactory),
            prepared.provider.clone(),
        )?;
        Ok(super::mux::Options::Browser(Arc::new(Options {
            inner: Arc::new(prepared.bind_provider(provider)),
            tag: input.tag.clone(),
            network,
        })))
    }
}
