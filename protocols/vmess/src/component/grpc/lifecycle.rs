use super::{state::Managed, *};
use std::sync::atomic::{AtomicBool, Ordering};
use zero_platform_tokio::{
    contract::{
        admission::SessionOwner,
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
            ResourceDeclaration,
        },
        lifecycle::{
            erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
            Unfinished,
        },
        stream::erase_stream,
        CloseReason,
    },
    controlled_carrier::CarrierOwner,
};
pub struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = super::super::Factory.describe(&options.base);
        spec.static_identity = options.identity.clone();
        spec.resources.push(ResourceDeclaration::BackgroundCarrier);
        spec.task_limit = Some(options.cap);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        if !context.dependencies.is_empty() || context.roles.outbound.is_none() {
            return Err(error(instance, Stage::Prepare, Reason::InvalidConfig));
        }
        let upstream = context
            .background_carrier
            .clone()
            .filter(|upstream| {
                upstream.binding().prefix.is_empty() && upstream.binding().dial == options.base.dial
            })
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::InvalidConfig))?;
        let (owner, admission) = SessionOwner::new(context.component, &context.cancel);
        let prepared = Arc::new(Mutex::new(None));
        let base = if options.base.ca_file.is_some() {
            erase_lifecycle(super::super::owner::CaOwner::new(
                owner,
                admission.clone(),
                options.base.clone(),
                prepared.clone(),
                context.resources,
            ))
        } else {
            erase_lifecycle(owner)
        };
        let cancel = context.cancel.child_token();
        let carriers = CarrierOwner::new(instance, options.cap, &cancel);
        let managed = Arc::new(Managed {
            instance,
            options,
            admission,
            prepared,
            upstream,
            tasks: context.tasks,
            cancel,
            ready: AtomicBool::new(false),
            grants: carriers.grants(),
            records: Mutex::new(Vec::new()),
        });
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(erase_stream(super::connect::Outbound(managed.clone()))),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Owner {
                managed,
                carriers,
                base,
                attempted: false,
            }),
        })
    }
}
struct Owner {
    managed: Arc<Managed>,
    carriers: CarrierOwner,
    base: Box<dyn DynResourceLifecycle>,
    attempted: bool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.managed.check().is_ok() {
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
        self.managed.options.state.claim(self.managed.instance)?;
        self.base.start(deadline).await?;
        self.carriers.start(deadline).await?;
        if self.managed.cancel.is_cancelled() || self.managed.tasks.cancellation().is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        self.managed.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.managed.stop();
        self.carriers.revoke();
        self.base.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let (base, physical) = tokio::join!(
            self.base.shutdown(deadline),
            self.carriers.shutdown(deadline)
        );
        let mut report = base?;
        report.append(physical?);
        self.managed
            .records
            .lock()
            .unwrap()
            .retain(|record| !record.closed());
        if !self.managed.records.lock().unwrap().is_empty() {
            report.unfinished.push(Unfinished::Io(
                "VMess original gRPC carriers remain unconfirmed".into(),
            ));
        }
        Ok(report)
    }
}
