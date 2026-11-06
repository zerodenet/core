//! Adapter-scoped client nonce history shared by all prepared leaves.
use super::ShadowsocksTransportLeaf;
use crate::{shared::legacy_replay::LegacyReplay, validation::ReplayPolicy};

#[derive(Debug, Default)]
pub struct ShadowsocksTransportState {
    #[cfg(feature = "blake3")]
    modern: std::sync::Arc<crate::shared::ReplaySaltPool>,
    plugins: super::plugin::outbound::PluginPool,
    guards: std::sync::Mutex<std::collections::HashMap<ReplayPolicy, LegacyReplay>>,
}
impl ShadowsocksTransportState {
    #[cfg(feature = "blake3")]
    pub fn with_replay_budget(budget: std::sync::Arc<crate::shared::ReplayBudget>) -> Self {
        Self {
            plugins: Default::default(),
            guards: Default::default(),
            modern: std::sync::Arc::new(crate::shared::ReplaySaltPool::configured_with_budget(
                crate::shared::ReplaySaltPool::DEFAULT_TTL,
                None,
                budget,
            )),
        }
    }
    /// Prepare protocol framing with this actual adapter's nonce history.
    /// No proxy Session, socket, child or maintenance task is created here.
    #[cfg(feature = "blake3")]
    pub fn prepare_tcp(
        &self,
        input: crate::component::TcpPreparation<'_>,
    ) -> Result<crate::component::PreparedTcp, zero_core::Error> {
        crate::component::PreparedTcp::new(input, |policy, capacity| {
            self.replay_guard(policy, capacity)
        })
    }
    /// A resource handle sharing the plan used by `with_plugin` leaves.
    pub fn plugin_resource(
        &self,
        host: &str,
        port: u16,
        config: crate::validation::PluginConfig,
    ) -> super::plugin::ShadowsocksPluginResource {
        super::plugin::ShadowsocksPluginResource::new(self.plugins.plan(host, port, config))
    }
    pub fn plugin_component_options(
        &self,
        host: &str,
        port: u16,
        config: crate::validation::PluginConfig,
    ) -> crate::component::PluginOptions {
        crate::component::PluginOptions::new(self.plugin_resource(host, port, config))
    }
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
        leaf.replay = self.replay_guard(policy, leaf.limits.tcp_replay_capacity);
        leaf
    }
    fn replay_guard(&self, policy: ReplayPolicy, capacity: Option<usize>) -> LegacyReplay {
        self.guards
            .lock()
            .unwrap()
            .entry(policy)
            .or_insert_with(|| {
                let guard = LegacyReplay::new(policy, false);
                #[cfg(feature = "blake3")]
                let guard = guard.with_modern_history(&self.modern);
                guard
            })
            .clone()
            .with_tcp_capacity(capacity)
    }

    #[cfg(feature = "blake3")]
    pub fn replay_component_options(
        &self,
        policy: ReplayPolicy,
        capacity: Option<usize>,
    ) -> crate::component::ReplayOptions {
        crate::component::ReplayOptions::new(
            self.replay_guard(policy, capacity).modern_pool(),
            policy,
            capacity,
        )
    }
}
