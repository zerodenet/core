//! One native owner, installed by the actual Direct component build/start.
use super::{DirectBudgets, SessionOwner};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ResourceAccess},
        lifecycle::{BuildError, CloseReport, ResourceLifecycle},
        scope::{TaskRegistrar, WeakTaskRegistrar},
        CloseReason,
    },
    controlled_carrier::{CarrierGrants, CarrierOwner},
};
#[derive(Clone)]
pub(in crate::adapters::direct) struct Control {
    pub(in crate::adapters::direct) grants: CarrierGrants,
    pub(in crate::adapters::direct) tasks: WeakTaskRegistrar,
    pub(in crate::adapters::direct) retirement: Duration,
}
pub(super) struct DirectOwner {
    sessions: SessionOwner,
    carrier: Option<CarrierOwner>,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
}
impl DirectOwner {
    pub(super) fn new(
        sessions: SessionOwner,
        budgets: Option<&DirectBudgets>,
        context: &BuildContext,
    ) -> (Self, Option<Control>) {
        let carrier = budgets.map(|budgets| {
            CarrierOwner::new(
                ComponentInstanceId {
                    component: context.component,
                    generation: context.generation,
                },
                budgets.max_sockets,
                &context.cancel,
            )
        });
        let control = carrier
            .as_ref()
            .zip(budgets)
            .map(|(owner, budgets)| Control {
                grants: owner.grants(),
                tasks: context.tasks.downgrade(),
                retirement: budgets.retirement,
            });
        (
            Self {
                sessions,
                carrier,
                resources: context.resources.clone(),
                tasks: context.tasks.clone(),
            },
            control,
        )
    }
}
impl ResourceLifecycle for DirectOwner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if let Some(carrier) = &mut self.carrier {
            self.resources
                .register_controlled_carrier(0, carrier.registration(), &self.tasks)?;
            carrier.start(deadline).await?;
        }
        self.sessions.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.sessions.revoke(reason);
        if let Some(carrier) = &self.carrier {
            carrier.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        let mut report = self.sessions.shutdown(deadline).await?;
        if let Some(carrier) = &mut self.carrier {
            report.append(carrier.shutdown(deadline).await?);
        }
        Ok(report)
    }
}
