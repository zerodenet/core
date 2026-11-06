use super::*;
use crate::grpc::{pool::State, GrpcPool};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceAccess, ResourceDeclaration, ResourceOwner,
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
            dependencies: options
                .totals
                .iter()
                .map(|totals| totals.dependency)
                .chain(options.node_dns)
                .collect(),
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.endpoint.clone(),
                dial: options.settings.dial.clone(),
                datagram: false,
                max_sockets: options.carrier_limit,
            }],
            task_limit: Some(options.carrier_limit),
        }
    }
    fn build(
        &self,
        options: Arc<NativePoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        if context.dependencies.len()
            != usize::from(options.totals.is_some()) + usize::from(options.node_dns.is_some())
            || options
                .node_dns
                .is_some_and(|node| context.dependency(node).is_none())
            || options.node_dns.is_some_and(|node| {
                options
                    .totals
                    .as_ref()
                    .is_some_and(|root| root.dependency == node)
            })
            || options
                .totals
                .as_ref()
                .is_some_and(|totals| context.dependency(totals.dependency).is_none())
            || context.roles != Default::default()
        {
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
                resources: context.resources,
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
    resources: Arc<dyn ResourceAccess>,
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
        self.resources.register_controlled_carrier(
            0,
            self.carriers.registration(),
            &self.managed.tasks,
        )?;
        // Read actual native/users/task ACK receipts outside the pool lock.
        let previous = match &*self.pool.ownership.lock().unwrap() {
            Ownership::Managed(old) => Some(old.clone()),
            _ => None,
        };
        let replaceable = self.managed.options.selected_background
            && previous
                .as_ref()
                .is_some_and(|old| old.instance != self.managed.instance && old.ready_to_replace());
        let retired = self.pool.retired.load(std::sync::atomic::Ordering::Acquire);
        let previous = {
            let mut ownership = self.pool.ownership.lock().unwrap();
            let allowed = match &*ownership {
                Ownership::Pristine => !retired || self.managed.options.selected_background,
                Ownership::Managed(old) => {
                    replaceable
                        && previous
                            .as_ref()
                            .is_some_and(|actual| Arc::ptr_eq(old, actual))
                }
                Ownership::LegacyUsed => false,
            };
            if !allowed || self.pool.cached.lock().unwrap().is_some() {
                return Err(error(
                    self.managed.instance,
                    Stage::Prepare,
                    Reason::StalePlan,
                ));
            }
            self.pool
                .retired
                .store(false, std::sync::atomic::Ordering::Release);
            let previous =
                std::mem::replace(&mut *ownership, Ownership::Managed(self.managed.clone()));
            self.claimed = true;
            previous
        };
        drop(previous);
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
        let physical = self.carriers.shutdown(deadline).await;
        // Foreign originals live in these same physical records, including a
        // prefix that failed before its loan could be installed in our slot.
        let pending = self.managed.reap(deadline).await;
        self.managed.harvest();
        let mut report = physical?;
        pending?;
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
