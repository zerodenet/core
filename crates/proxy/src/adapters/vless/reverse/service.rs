//! Active ingress always consumes the configured canonical Stream provider.
//! There is no private transport runtime or alternate MUX executor here.
use super::super::*;
use crate::protocol_registry::InboundServiceCapability;
use crate::runtime::inbound_service::NativeServiceOperation;
impl InboundServiceCapability for VlessAdapter {
    fn prepare_inbound_service(
        &self,
        outbound: &zero_config::OutboundConfig,
    ) -> Result<Option<NativeServiceOperation>, EngineError> {
        let OutboundProtocolConfig::Vless {
            reverse_tag: Some(tag),
            reverse_sniffing,
            server,
            port,
            ..
        } = &outbound.protocol
        else {
            return Ok(None);
        };
        let options =
            outbound_options(outbound.tag(), (server.as_str(), *port), &outbound.protocol)
                .expect("VLESS outbound projection");
        let sniffing = reverse_sniffing
            .as_deref()
            .map(|config| {
                crate::runtime::sniff::SniffingPolicy::new(
                    config.enabled,
                    &config.destination_override,
                    &config.domains_excluded,
                    config.metadata_only,
                    config.route_only,
                )
            })
            .transpose()
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        let source = ::vless::reverse::source::Options::from_options(
            options.protocol,
            crate::inventory::service::endpoint(server, *port),
        )?;
        Ok(Some(NativeServiceOperation {
            recipe: crate::component::ComponentRecipe::from_validated(
                std::sync::Arc::new(::vless::reverse::source::Factory),
                std::sync::Arc::new(source),
            ),
            outbound: outbound.tag.clone(),
            tag: tag.clone(),
            sniffing,
        }))
    }
}
