//! Recovery rebinds only concrete device aliases to this actual cold pass.
//! Other protocols retain their original immutable prepared recipes/materials.
use super::*;
use crate::component::{ComponentRecipe, ResourcePreparation};
use std::collections::BTreeMap;
impl Devices {
    /// None names the existing builtin Direct, never a configured tag "direct".
    /// The host validates these finite overrides against its original deferred
    /// aliases and rebuilds their generations in the normal dependency order.
    pub(crate) fn device_alias_overrides(
        &self,
        config: &RuntimeConfig,
        resources: &mut ResourcePreparation<'_>,
    ) -> Result<BTreeMap<Option<String>, ComponentRecipe>, ChannelError> {
        let mut aliases = BTreeMap::new();
        if let Some(direct) = &self.direct {
            if config.runtime.network.direct_packet_device.is_some() {
                let builtin = zero_config::OutboundConfig {
                    tag: "direct".into(),
                    protocol: zero_config::OutboundProtocolConfig::Direct,
                    udp: Default::default(),
                    dial: Default::default(),
                };
                for (tag, input) in std::iter::once((None, &builtin)).chain(
                    config
                        .outbounds
                        .iter()
                        .filter(|input| {
                            matches!(input.protocol, zero_config::OutboundProtocolConfig::Direct)
                        })
                        .map(|input| (Some(input.tag.clone()), input)),
                ) {
                    if let Some(recipe) = direct.packet_alias_recipe(input, resources)? {
                        aliases.insert(tag, recipe);
                    }
                }
            }
        }
        #[cfg(feature = "wireguard")]
        if let Some(wireguard) = &self.wireguard {
            let admission = EndpointAdmission::from_config(config);
            for input in config.outbounds.iter().filter(|input| {
                matches!(
                    &input.protocol,
                    zero_config::OutboundProtocolConfig::Wireguard {
                        inbound_tag: None,
                        outer_udp_proxy: None,
                        ..
                    }
                ) || admission.device_enabled(&input.tag)
                    && matches!(
                        &input.protocol,
                        zero_config::OutboundProtocolConfig::Wireguard {
                            inbound_tag: Some(_),
                            outer_udp_proxy: None,
                            ..
                        }
                    )
            }) {
                aliases.insert(
                    Some(input.tag.clone()),
                    wireguard.device_alias_recipe(input, resources)?,
                );
            }
        }
        Ok(aliases)
    }
}
