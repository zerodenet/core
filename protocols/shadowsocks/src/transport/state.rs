//! Adapter-scoped client nonce history shared by all prepared leaves.
use super::ShadowsocksTransportLeaf;
use crate::{shared::legacy_replay::LegacyReplay, validation::ReplayPolicy};

#[derive(Debug, Default)]
pub struct ShadowsocksTransportState {
    plugins: super::plugin::outbound::PluginPool,
    guards:
        std::sync::Mutex<std::collections::HashMap<(ReplayPolicy, Option<usize>), LegacyReplay>>,
}
impl ShadowsocksTransportState {
    pub fn clear_plugins(&self) {
        self.plugins.clear();
    }
    pub fn with_plugin(
        &self,
        mut leaf: ShadowsocksTransportLeaf,
        config: Option<crate::validation::PluginConfig>,
    ) -> ShadowsocksTransportLeaf {
        leaf.plugin = config.map(|config| self.plugins.plan(&leaf.server, leaf.port, config));
        leaf
    }

    pub fn apply(
        &self,
        mut leaf: ShadowsocksTransportLeaf,
        policy: ReplayPolicy,
    ) -> ShadowsocksTransportLeaf {
        let mut guards = self.guards.lock().unwrap();
        leaf.replay = guards
            .entry((policy, leaf.limits.tcp_replay_capacity))
            .or_insert_with(|| {
                LegacyReplay::new(policy, false).with_tcp_capacity(leaf.limits.tcp_replay_capacity)
            })
            .clone();
        leaf
    }
}
