//! Static selected-outbound dependencies, independent of rule generations.
//! No token is a resource grant, admission, task join or physical-close proof.
use super::EnginePlan;
use std::sync::{Arc, Weak};
use zero_config::RuntimeConfig;

/// Weak proof of the exact static configuration of one concrete outbound.
/// It retains neither Engine, a historical config/plan, nor protocol resources.
#[derive(Clone, Debug)]
pub struct OutboundStaticBinding {
    tag: String,
    token: Weak<()>,
}
impl OutboundStaticBinding {
    pub fn tag(&self) -> &str {
        &self.tag
    }
    pub fn matches(&self, current: &EnginePlan) -> bool {
        current
            .outbound_static
            .get(&self.tag)
            .is_some_and(|token| Weak::ptr_eq(&self.token, &Arc::downgrade(token)))
    }
}
impl EnginePlan {
    pub fn outbound_static_binding(&self, tag: &str) -> Option<OutboundStaticBinding> {
        self.outbound_static
            .get(tag)
            .map(|token| OutboundStaticBinding {
                tag: tag.to_owned(),
                token: Arc::downgrade(token),
            })
    }
    /// Cold comparison uses the complete actual OutboundConfig Eq, including
    /// protocol-private options, dial and UDP options. The source directory is
    /// a construction input to prepared protocol options. No hash, serialized
    /// copy, global history, or protocol-name projection decides equivalence.
    pub(crate) fn reconcile_outbound_static(
        &mut self,
        config: &RuntimeConfig,
        previous: &Self,
        old_config: &RuntimeConfig,
    ) {
        if config.source_dir() != old_config.source_dir() {
            return;
        }
        let old_by_tag: std::collections::HashMap<_, _> = old_config
            .outbounds
            .iter()
            .map(|outbound| (outbound.tag.as_str(), outbound))
            .collect();
        for outbound in &config.outbounds {
            if old_by_tag
                .get(outbound.tag.as_str())
                .is_some_and(|old| **old == *outbound)
            {
                if let Some(token) = previous.outbound_static.get(&outbound.tag) {
                    self.outbound_static
                        .insert(outbound.tag.clone(), token.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
