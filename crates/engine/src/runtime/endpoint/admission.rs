use zero_config::RuntimeConfig;

/// Engine-owned admission over validated, protocol-neutral resource intent.
/// Preparation and packet hot paths borrow the configuration without cloning
/// keys, constructing bindings, or allocating when admission succeeds.
pub struct EndpointAdmission<'a> {
    config: &'a RuntimeConfig,
    intents: Option<&'a super::intent::EndpointIntents>,
}

impl<'a> EndpointAdmission<'a> {
    pub fn from_config(config: &'a RuntimeConfig) -> Self {
        Self {
            config,
            intents: None,
        }
    }

    pub fn from_snapshot(snapshot: &'a crate::EngineRuntimeSnapshot) -> Self {
        Self {
            config: snapshot.config(),
            intents: Some(&snapshot.endpoint_intents),
        }
    }

    pub fn listener_enabled(&self, tag: &str) -> bool {
        if let Some(intents) = self.intents {
            return intents.inbound(tag).is_none_or(|entry| entry.enabled());
        }
        self.config
            .endpoints
            .iter()
            .find(|endpoint| {
                endpoint.listen.is_some()
                    && tag.strip_prefix("endpoint/") == Some(endpoint.tag.as_str())
            })
            .is_none_or(|endpoint| endpoint.enabled)
    }

    pub fn device_enabled(&self, tag: &str) -> bool {
        if let Some(intents) = self.intents {
            return intents.outbound(tag).is_none_or(|entry| entry.enabled());
        }
        self.config
            .endpoints
            .iter()
            .find(|endpoint| endpoint.tag == tag)
            .is_none_or(|endpoint| endpoint.enabled)
    }

    pub fn inbound_allowed(&self, tag: &str) -> bool {
        if let Some(intents) = self.intents {
            return intents
                .inbound(tag)
                .is_none_or(|entry| entry.enabled() && entry.directions().inbound);
        }
        self.config
            .endpoints
            .iter()
            .find(|endpoint| {
                endpoint.listen.is_some()
                    && tag.strip_prefix("endpoint/") == Some(endpoint.tag.as_str())
            })
            .is_none_or(|endpoint| endpoint.enabled && endpoint.directions.inbound)
    }

    pub fn outbound_denial(&self, tag: &str) -> Option<(&'static str, String)> {
        if let Some(intents) = self.intents {
            let entry = intents.outbound(tag)?;
            let reason = if !entry.enabled() {
                "endpoint_disabled"
            } else if !entry.directions().outbound {
                "endpoint_outbound_disabled"
            } else {
                return None;
            };
            return Some((reason, entry.binding.endpoint_id.clone()));
        }
        let endpoint = self
            .config
            .endpoints
            .iter()
            .find(|endpoint| endpoint.tag == tag)?;
        if !endpoint.enabled {
            Some(("endpoint_disabled", endpoint.endpoint_id()))
        } else if !endpoint.directions.outbound {
            Some(("endpoint_outbound_disabled", endpoint.endpoint_id()))
        } else {
            None
        }
    }

    /// An outbound-only resource must identify replies without admitting new
    /// remote business through a coarse destination-address return route.
    pub fn requires_correlated_packet_returns(&self, tag: &str) -> bool {
        if let Some(intents) = self.intents {
            return intents
                .outbound(tag)
                .is_some_and(|entry| entry.correlated_returns());
        }
        self.config
            .endpoints
            .iter()
            .find(|endpoint| endpoint.tag == tag)
            .is_some_and(|endpoint| !endpoint.directions.inbound)
    }
}
