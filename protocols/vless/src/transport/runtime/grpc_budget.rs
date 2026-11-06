//! Dedicated totals of the actual gRPC registry; no executor or cache.
use super::VlessTransportRuntime;
use std::sync::Arc;
use zero_transport::{
    grpc::{PoolBudgets, PoolTotals, TotalsOptions},
    RuntimeError,
};
#[derive(Clone, Default)]
pub(super) struct RegistryBudget(Arc<PoolTotals>);
impl std::fmt::Debug for RegistryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcRegistryBudget").finish_non_exhaustive()
    }
}
impl VlessTransportRuntime {
    pub fn validate_grpc_candidate(&self, native: bool, legacy: bool) -> Result<(), RuntimeError> {
        let pools = self
            .grpc_pools
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for pool in pools {
            pool.validate_mode_candidate(native, legacy)?;
        }
        Ok(())
    }
    /// Prepare a cold dependency of this exact runtime registry. It allocates
    /// neither native sockets nor tasks, and cannot replace active old credits.
    pub fn grpc_budget_options(&self, budgets: PoolBudgets) -> Result<TotalsOptions, RuntimeError> {
        self.grpc_budget
            .0
            .options(budgets)
            .map_err(|source| std::io::Error::other(source).into())
    }
}
