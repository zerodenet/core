use super::ProtocolRegistry;
use zero_config::RuntimeConfig;

impl ProtocolRegistry {
    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        for entry in &self.entries {
            entry.support.on_config_reloaded(config);
        }
    }
}
