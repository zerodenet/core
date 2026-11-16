//! Concrete accepted Source custody, not a socket grant or an erased provider SPI.
use super::*;
use zero_platform_tokio::controlled_carrier::AcceptedCarrierProvider;
pub(super) struct AcceptedSources {
    declaration: Option<(usize, std::num::NonZeroUsize)>,
    actual: std::sync::OnceLock<Arc<AcceptedCarrierProvider>>,
}
impl AcceptedSources {
    pub(super) fn new(declarations: &[ResourceDeclaration]) -> Result<Self, ChannelError> {
        let found = declarations
            .iter()
            .enumerate()
            .filter_map(|(ordinal, declaration)| match declaration {
                ResourceDeclaration::AcceptedCarrierSources { max_sources } => {
                    Some((ordinal, *max_sources))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        if found.len() > 1 {
            return Err(crate::component::instance::invalid(
                Reason::InvalidConfig,
                "duplicate accepted Source declaration",
            ));
        }
        Ok(Self {
            declaration: found.first().copied(),
            actual: std::sync::OnceLock::new(),
        })
    }
    pub(super) fn bind(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        root: &Arc<crate::HostOriginalBudgets>,
        provider: Arc<AcceptedCarrierProvider>,
    ) -> Result<(), ChannelError> {
        if self
            .declaration
            .is_none_or(|(_, limit)| provider.limit() != limit)
            || provider.instance() != instance
        {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "accepted Source provider differs from actual declaration",
            ));
        }
        provider.bind_host(instance, &root.carrier)?;
        self.actual.set(provider).map_err(|_| {
            crate::component::instance::invalid(
                Reason::StalePlan,
                "accepted Source provider already installed",
            )
        })
    }
    pub(super) fn returned(&self) -> bool {
        // No published role means start was never permitted. This declaration
        // issues no socket/task grant and its constructor performs no I/O.
        self.actual.get().is_none_or(|provider| {
            provider
                .returned()
                .is_some_and(|receipt| receipt.instance() == provider.instance())
        })
    }
}
impl HostResources {
    pub(in crate::component) fn bind_accepted_carriers(
        &self,
        provider: Arc<AcceptedCarrierProvider>,
    ) -> Result<(), ChannelError> {
        self.accepted.bind(
            self.instance.ok_or_else(|| self.denied())?,
            &self.originals,
            provider,
        )
    }
}
