//! Adapter-scoped client nonce history shared by all prepared leaves.
use super::ShadowsocksTransportLeaf;
use crate::{shared::legacy_replay::LegacyReplay, validation::ReplayPolicy};

#[derive(Debug, Default)]
pub struct ShadowsocksTransportState {
    #[cfg(feature = "blake3")]
    modern: std::sync::Arc<crate::shared::ReplaySaltPool>,
    #[cfg(feature = "blake3")]
    udp_replay: std::sync::Arc<crate::udp::UdpReplayBudget>,
    bloom_budget: std::sync::Arc<crate::shared::LegacyReplayBudget>,
    plugins: super::plugin::outbound::PluginPool,
    guards: std::sync::Mutex<std::collections::HashMap<ReplayPolicy, LegacyReplay>>,
}
impl ShadowsocksTransportState {
    #[cfg(feature = "blake3")]
    pub fn with_replay_budget(budget: std::sync::Arc<crate::shared::ReplayBudget>) -> Self {
        Self::with_replay_budgets(budget, Default::default())
    }
    #[cfg(feature = "blake3")]
    pub fn with_replay_budgets(
        budget: std::sync::Arc<crate::shared::ReplayBudget>,
        udp_replay: std::sync::Arc<crate::udp::UdpReplayBudget>,
    ) -> Self {
        Self::with_security_budgets(budget, udp_replay, Default::default())
    }
    #[cfg(feature = "blake3")]
    pub fn with_security_budgets(
        budget: std::sync::Arc<crate::shared::ReplayBudget>,
        udp_replay: std::sync::Arc<crate::udp::UdpReplayBudget>,
        bloom_budget: std::sync::Arc<crate::shared::LegacyReplayBudget>,
    ) -> Self {
        Self {
            udp_replay,
            bloom_budget,
            plugins: Default::default(),
            guards: Default::default(),
            modern: std::sync::Arc::new(crate::shared::ReplaySaltPool::configured_with_budget(
                crate::shared::ReplaySaltPool::DEFAULT_TTL,
                None,
                budget,
            )),
        }
    }
    /// Cold validation of this actual adapter's retained security history,
    /// including when the candidate removes every SS consumer. No live cap swap.
    #[cfg(feature = "blake3")]
    pub fn validate_tcp_replay_budget(&self, total: usize) -> Result<(), zero_core::Error> {
        self.modern.budget().validate(Some(total))
    }
    #[cfg(feature = "blake3")]
    pub fn validate_udp_budget(
        &self,
        limits: crate::udp::UdpBudgetLimits,
    ) -> Result<(), zero_core::Error> {
        self.udp_replay.validate(limits)
    }

    /// A weak cold proposal for the existing process owner. No cap is installed
    /// and no child, socket, worker or second protocol pool is created here.
    #[cfg(feature = "blake3")]
    pub fn udp_budget_proposal(
        &self,
        limits: crate::udp::UdpBudgetLimits,
    ) -> Result<crate::udp::UdpBudgetProposal, zero_core::Error> {
        crate::udp::UdpBudgetProposal::new(&self.udp_replay, limits)
    }

    pub fn validate_legacy_replay_budget(&self, max_bytes: usize) -> Result<(), zero_core::Error> {
        self.bloom_budget.validate(Some(max_bytes))
    }
    pub fn legacy_replay_budget_proposal(
        &self,
        max_bytes: usize,
    ) -> Result<crate::shared::LegacyReplayBudgetProposal, zero_core::Error> {
        crate::shared::LegacyReplayBudgetProposal::new(&self.bloom_budget, max_bytes)
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
                let guard = guard
                    .with_modern_history(&self.modern)
                    .with_udp_replay_budget(self.udp_replay.clone())
                    .expect("a fresh transport guard has no UDP security book");
                guard
                    .with_bloom_budget(self.bloom_budget.clone())
                    .expect("a fresh transport guard has no Bloom history")
            })
            .clone()
            .with_tcp_capacity(capacity)
    }

    /// Canonical existing TCP2022 maintenance options for an owning plugin
    /// framing role; callers share ReplayFactory in the same actual host.
    #[cfg(feature = "blake3")]
    pub fn replay_component_options_with_total(
        &self,
        policy: ReplayPolicy,
        capacity: Option<usize>,
        total: usize,
    ) -> Result<crate::component::ReplayOptions, zero_core::Error> {
        let pool = self.replay_guard(policy, capacity).modern_pool();
        let view = std::sync::Arc::new(pool.with_total(Some(total))?);
        Ok(crate::component::ReplayOptions::new(view, policy, capacity))
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
