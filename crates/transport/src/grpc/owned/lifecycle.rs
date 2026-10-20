use super::*;
use crate::grpc::{pool::State, GrpcPool};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
};
pub struct NativePoolFactory;
impl ComponentFactory for NativePoolFactory {
    type Options = NativePoolOptions;
    fn describe(&self, options: &NativePoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("grpc.client_pool".into()),
            roles: vec![],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::BackgroundCarrier],
            task_limit: Some(options.carrier_limit),
        }
    }
    fn build(
        &self,
        options: Arc<NativePoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        if !context.dependencies.is_empty() || context.roles != Default::default() {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        let upstream = context
            .background_carrier
            .clone()
            .filter(|upstream| {
                upstream.binding().prefix.is_empty()
                    && upstream.binding().dial == options.settings.dial
            })
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::InvalidConfig))?;
        let pool = options
            .pool
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?;
        let cancel = context.cancel.child_token();
        let carriers = CarrierOwner::new(instance, options.carrier_limit, &cancel);
        let managed = Managed::new(
            instance,
            options,
            upstream,
            context.tasks,
            cancel,
            carriers.grants(),
        );
        Ok(ComponentInstance {
            roles: ComponentRoles::default(),
            lifecycle: erase_lifecycle(Owner {
                pool,
                managed,
                carriers,
                attempted: false,
                claimed: false,
            }),
        })
    }
}
struct Owner {
    pool: Arc<State>,
    managed: Arc<Managed>,
    carriers: CarrierOwner,
    attempted: bool,
    claimed: bool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.managed.ready() {
            return Ok(());
        }
        if self.attempted || self.managed.cancel.is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Timeout,
            ));
        }
        self.attempted = true;
        {
            let mut ownership = self.pool.ownership.lock().unwrap();
            if self.pool.retired.load(std::sync::atomic::Ordering::Acquire)
                || !matches!(*ownership, Ownership::Pristine)
                || self.pool.cached.lock().unwrap().is_some()
            {
                return Err(error(
                    self.managed.instance,
                    Stage::Prepare,
                    Reason::StalePlan,
                ));
            }
            *ownership = Ownership::Managed(self.managed.clone());
            self.claimed = true;
        }
        self.carriers.start(deadline).await?;
        self.managed.activate()
    }
    fn revoke(&self, _: CloseReason) {
        self.managed.stop();
        self.carriers.revoke();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        if self.claimed {
            GrpcPool(self.pool.clone()).retire();
        }
        let mut report = self.carriers.shutdown(deadline).await?;
        self.managed.harvest();
        if self.managed.physical_unconfirmed() && report.is_complete() {
            report.unfinished.push(Unfinished::Io(
                "gRPC original carrier lacks a matching close receipt".into(),
            ));
        }
        if self.managed.budget.retained() != 0 {
            report.unfinished.push(Unfinished::Resource(
                "gRPC pending/live/retiring logical streams".into(),
            ));
        }
        Ok(report)
    }
}
