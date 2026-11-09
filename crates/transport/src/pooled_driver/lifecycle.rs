use super::{
    control::{Registry, Scope},
    failure, ManagedPool, PoolOptions,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::{sync::Notify, time::Instant};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceAccess, ResourceDeclaration, ResourceOwner,
        },
        lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle, Unfinished},
        CloseReason, Reason, Stage,
    },
    controlled_carrier::CarrierOwner,
};
pub struct PoolFactory;
impl ComponentFactory for PoolFactory {
    type Options = PoolOptions;
    fn describe(&self, options: &PoolOptions) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Endpoint("shared.stream_pool".into()),
            roles: vec![],
            dependencies: options.dependencies.clone(),
            static_identity: options.identity.clone(),
            resources: vec![ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.endpoint.clone(),
                dial: options.binding.dial.clone(),
                datagram: false,
                max_sockets: options.limit,
            }],
            task_limit: Some(options.limit),
        }
    }
    fn build(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        self.build_with_registration(options, context)
            .map(|(instance, _)| instance)
    }
}
impl PoolFactory {
    /// Return the same inert owner's registration to a protocol lifecycle that
    /// must install it before a fallible material preparation. This does not
    /// start admission, claim the protocol registry, acquire IO or spawn tasks.
    /// The existing ResourceAccess still validates the exact owner and Scope
    /// during the original lifecycle start phase.
    pub fn build_with_registration(
        &self,
        options: Arc<PoolOptions>,
        context: BuildContext,
    ) -> Result<
        (
            ComponentInstance,
            zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
        ),
        BuildError,
    > {
        let instance = context.instance();
        if context.dependencies.len() != options.dependencies.len()
            || options
                .dependencies
                .iter()
                .any(|dependency| context.dependency(*dependency).is_none())
            || (matches!(options.endpoint.address, zero_core::Address::Domain(_))
                && options.dependencies.is_empty())
            || context.roles != Default::default()
        {
            return Err(failure(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        let stream_dependency = options
            .stream_dependency
            .map(|dependency| {
                context
                    .dependency(dependency)
                    .filter(|actual| actual.component == dependency && *actual != instance)
                    .ok_or_else(|| failure(instance, Stage::Prepare, Reason::StalePlan))
            })
            .transpose()?;
        let registry = options
            .registry
            .upgrade()
            .ok_or_else(|| failure(instance, Stage::Prepare, Reason::Revoked))?;
        let upstream = context
            .background_carrier
            .filter(|upstream| upstream.binding() == &options.binding)
            .ok_or_else(|| failure(instance, Stage::Prepare, Reason::StalePlan))?;
        let cancel = context.cancel.child_token();
        let carriers = CarrierOwner::new(instance, options.limit, &cancel);
        let managed = Arc::new(ManagedPool {
            instance,
            stream_dependency,
            options,
            upstream,
            tasks: context.tasks,
            resources: Arc::downgrade(&context.resources),
            grants: carriers.grants(),
            cancel,
            ready: AtomicBool::new(false),
            records: Mutex::new(vec![]),
            changed: Notify::new(),
            connecting: tokio::sync::Mutex::new(()),
            epoch: Mutex::new(None),
            settled: Mutex::new(Default::default()),
            operations: AtomicUsize::new(0),
        });
        let registration = carriers.registration();
        Ok((
            ComponentInstance {
                roles: ComponentRoles::default(),
                lifecycle: erase_lifecycle(Owner {
                    registry,
                    managed,
                    carriers,
                    resources: context.resources,
                    scope: None,
                    attempted: false,
                    pending: CloseReport::default(),
                }),
            },
            registration,
        ))
    }
}
struct Owner {
    registry: Arc<Registry>,
    managed: Arc<ManagedPool>,
    carriers: CarrierOwner,
    resources: Arc<dyn ResourceAccess>,
    scope: Option<Arc<Scope>>,
    attempted: bool,
    pending: CloseReport,
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
        if self.attempted || self.managed.cancel.is_cancelled() || Instant::now() >= deadline {
            return Err(failure(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        self.attempted = true;
        self.resources.register_controlled_carrier(
            0,
            self.carriers.registration(),
            &self.managed.tasks,
        )?;
        self.scope = Some(
            self.registry
                .claim(&self.managed.options.key, &self.managed)?,
        );
        self.carriers.start(deadline).await?;
        if self.managed.cancel.is_cancelled() || self.managed.tasks.cancellation().is_cancelled() {
            return Err(failure(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        self.managed.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.managed.stop();
        self.carriers.revoke();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        self.pending
            .append(std::mem::take(&mut *self.managed.settled.lock().unwrap()));
        self.pending.unfinished.clear();
        self.pending.append(self.carriers.shutdown(deadline).await?);
        let records = self.managed.records.lock().unwrap().clone();
        for record in records {
            if let Some(children) = &record.protocols {
                if !children.wait_returned(deadline).await? {
                    self.pending.unfinished.push(Unfinished::Resource(
                        "original child protocol return/Host ACK remains pending".into(),
                    ));
                }
                children.reclaim_returned();
            }
            record.release_installed_pending();
            let pending = record.pending.lock().unwrap().clone();
            for original in pending {
                self.pending.append(original.retire(deadline).await?);
            }
            record.release_installed_pending();
        }
        loop {
            let changed = self.managed.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.managed.harvest();
            if self.managed.records.lock().unwrap().is_empty()
                && self.managed.operations.load(Ordering::Acquire) == 0
            {
                break;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                self.pending.unfinished.push(Unfinished::Resource(
                    "pending original pool carriers or logical users".into(),
                ));
                break;
            }
        }
        let mut collected = std::mem::take(&mut *self.managed.settled.lock().unwrap());
        // Old attempt timeouts are not current closure facts. Native/task
        // receipts and their sources remain retained across every retry.
        collected.unfinished.clear();
        self.pending.append(collected);
        let registration = self.carriers.registration();
        let confirmed = registration
            .closed()
            .is_some_and(|proof| proof.matches(&registration))
            && registration
                .users_closed()
                .is_some_and(|proof| proof.matches(&registration));
        if self.pending.is_complete() && !confirmed {
            self.pending.unfinished.push(Unfinished::Io(
                "original pool carrier closure remains unconfirmed".into(),
            ));
        }
        if self.pending.is_complete() {
            if let Some(reclaim) = &self.managed.options.on_closed {
                reclaim(self.managed.instance);
            }
            if let Some(scope) = &self.scope {
                self.registry.confirm(&self.managed.options.key, scope);
            }
        }
        Ok(std::mem::take(&mut self.pending))
    }
}
