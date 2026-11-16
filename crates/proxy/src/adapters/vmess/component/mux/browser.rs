//! Protocol-owned VMess MUX framing over its real neutral Browser Source.
use super::*;
impl VmessAdapter {
    pub(super) fn browser_mux_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        background: Arc<background_pool::Factory>,
        limit: NonZeroUsize,
    ) -> Result<Options, RegistrationError> {
        let (source, dependency) = crate::adapters::browser::mux::prepare(
            input,
            resources,
            &self.browser,
            background.network.clone(),
            limit,
        )?;
        let OutboundProtocolConfig::Vmess { server, port, .. } = &input.protocol else {
            unreachable!("owning protocol projection")
        };
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("Vmess Browser MUX projection");
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: input.dial.to_policy(),
            network_generation: background.generation,
        };
        let prepared = ::vmess::component::mux::PreparedMux::from_browser_options_refs(
            refs,
            &self.runtime,
            binding,
            limit,
            Duration::from_secs(5),
        )
        .map_err(|source| zero_config::ConfigError::InvalidOutbound(source.to_string()))?
        .with_carrier_lender(dependency);
        let options = Arc::new(pool::Options {
            pool: Arc::new(
                (*prepared.owned_pool_options())
                    .clone()
                    .with_scoped_preparation(),
            ),
            browser: Some((dependency, source)),
            tag: input.tag.clone(),
            wire_endpoint: zero_core::request::Destination::new(
                zero_core::Address::Domain(server.clone()),
                *port,
            ),
        });
        let owner = resources.share(
            "vmess.mux.actual_pool",
            Arc::new(pool::Factory { background }),
            options,
        )?;
        Ok(Options::Mux(Arc::new(prepared.bind(owner))))
    }
}
