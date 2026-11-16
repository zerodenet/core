//! Protocol-owned Trojan MUX framing over its real neutral Browser Source.
use super::*;
impl TrojanAdapter {
    pub(super) fn browser_mux_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        background: Arc<pool::Factory>,
        limit: NonZeroUsize,
    ) -> Result<Options, RegistrationError> {
        let (source, dependency) = crate::adapters::browser::mux::prepare(
            input,
            resources,
            &self.browser,
            background.network.clone(),
            limit,
        )?;
        let OutboundProtocolConfig::Trojan { server, port, .. } = &input.protocol else {
            unreachable!("owning protocol projection")
        };
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("Trojan Browser MUX projection");
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: input.dial.to_policy(),
            network_generation: background.generation,
        };
        let prepared = ::trojan::component::mux::PreparedMux::from_browser_options_refs(
            refs,
            &self.mux_pool,
            binding,
            limit,
            Duration::from_secs(5),
        )
        .map_err(|source| zero_config::ConfigError::InvalidOutbound(source.to_string()))?
        .with_carrier_lender(dependency);
        let options = Arc::new(pool::Options {
            pool: prepared.pool_options().clone(),
            tag: input.tag.clone(),
            wire_endpoint: zero_core::request::Destination::new(
                zero_core::Address::Domain(server.clone()),
                *port,
            ),
        });
        let owner = resources.share(
            "trojan.mux.actual_pool",
            Arc::new(crate::adapters::browser::mux::pool::Factory {
                background,
                dependency,
                source,
            }),
            options,
        )?;
        Ok(Options::Mux(Arc::new(prepared.bind(owner))))
    }
}
