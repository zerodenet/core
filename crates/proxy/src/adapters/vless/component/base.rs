//! Runtime-neutral assembly still selects the same actual virtual factory.
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::component::*;
pub(crate) enum Options {
    Stream(Arc<::vless::component::Options>),
    Reverse(Arc<::vless::reverse::component::Options>),
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Stream(options) => ::vless::component::Factory.describe(options),
            Options::Reverse(options) => ::vless::reverse::component::Factory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, zero_platform_tokio::contract::ChannelError> {
        match options.as_ref() {
            Options::Stream(options) => ::vless::component::Factory.build(options.clone(), context),
            Options::Reverse(options) => {
                ::vless::reverse::component::Factory.build(options.clone(), context)
            }
        }
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    matches!(input.protocol, OutboundProtocolConfig::VlessReverse) || super::native_input(input)
}
impl super::super::VlessAdapter {
    pub(crate) fn base_options(
        &self,
        input: &OutboundConfig,
    ) -> Result<Options, zero_config::ConfigError> {
        if matches!(input.protocol, OutboundProtocolConfig::VlessReverse) {
            return Ok(Options::Reverse(Arc::new(
                self.runtime.reverse_component_options(input.tag()),
            )));
        }
        super::component_options(input).map(|options| Options::Stream(Arc::new(options)))
    }
}
