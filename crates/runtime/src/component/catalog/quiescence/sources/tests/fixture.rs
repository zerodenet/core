//! The actual Direct role and same-Host auxiliary TCP/journal originals.
use super::*;
use crate::component::{ComponentRecipe, DirectFactory, DirectOptions};
use std::{num::NonZeroUsize, path::PathBuf, sync::OnceLock};
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentSpec, ResourceAccess,
        ResourceDeclaration,
    },
    lifecycle::{
        erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
    },
    scope::TaskRegistrar,
    CloseReason,
};
use zero_platform_tokio::controlled_carrier::{CarrierGrants, CarrierOwner};
use zero_platform_tokio::journal::{JournalGrant, JournalSpec};
#[derive(Default)]
pub(super) struct Issued {
    pub grants: OnceLock<CarrierGrants>,
    pub journal: OnceLock<JournalGrant>,
    pub tasks: OnceLock<TaskRegistrar>,
}
struct Options {
    direct: DirectOptions,
    endpoint: zero_core::request::Destination,
    path: PathBuf,
}
struct Factory(Arc<Issued>);
struct Lifecycle {
    inner: Box<dyn DynResourceLifecycle>,
    owner: CarrierOwner,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    ordinal: usize,
    issued: Arc<Issued>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut actual = DirectFactory.describe(&options.direct);
        actual
            .resources
            .push(ResourceDeclaration::ControlledCarrierPool {
                endpoint: options.endpoint.clone(),
                dial: Default::default(),
                datagram: false,
                max_sockets: NonZeroUsize::new(1).unwrap(),
            });
        actual
            .resources
            .push(ResourceDeclaration::ControlledJournal {
                spec: JournalSpec {
                    path: options.path.clone(),
                    max_bytes: NonZeroUsize::new(64).unwrap(),
                    parent_private: false,
                },
            });
        actual
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let ordinal = DirectFactory.describe(&options.direct).resources.len();
        let owner = CarrierOwner::new(
            context.instance(),
            NonZeroUsize::new(1).unwrap(),
            &context.cancel,
        );
        let tasks = context.tasks.clone();
        let resources = context.resources.clone();
        let actual = DirectFactory.build(Arc::new(options.direct.clone()), context)?;
        Ok(ComponentInstance {
            roles: actual.roles,
            lifecycle: erase_lifecycle(Lifecycle {
                inner: actual.lifecycle,
                owner,
                resources,
                tasks,
                ordinal,
                issued: self.0.clone(),
            }),
        })
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), BuildError> {
        self.inner.start(end).await?;
        self.resources.register_controlled_carrier(
            self.ordinal,
            self.owner.registration(),
            &self.tasks,
        )?;
        self.owner.start(end).await?;
        assert!(self.issued.grants.set(self.owner.grants()).is_ok());
        assert!(self.issued.tasks.set(self.tasks.clone()).is_ok());
        assert!(self
            .issued
            .journal
            .set(self.resources.journal(self.ordinal + 1)?)
            .is_ok());
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
        self.owner.revoke();
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        // Both actual owners progress even if either returned an error.
        let first = self.inner.shutdown(end).await;
        let second = self.owner.shutdown(end).await;
        let mut report = CloseReport::default();
        let mut unfinished = false;
        for result in [first, second] {
            match result {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    let source = Arc::new(source);
                    let received = self
                        .resources
                        .receive_protocol_failure(&self.tasks.scope_identity(), &source)?;
                    if !report
                        .resource_failures
                        .iter()
                        .any(|same| Arc::ptr_eq(same, &received))
                    {
                        report.resource_failures.push(received);
                    }
                    unfinished = true;
                }
            }
        }
        if unfinished {
            report.unfinished.push(
                zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                    "an actual fixture owner remains unconfirmed after its reported error".into(),
                ),
            );
        }
        Ok(report)
    }
}

pub(super) fn install(
    catalog: &ComponentCatalog,
    endpoint: zero_core::request::Destination,
    path: PathBuf,
) -> (Arc<ComponentPlan>, Arc<ComponentSlot>, Arc<Issued>) {
    let issued = Arc::new(Issued::default());
    let slot = Arc::new(ComponentSlot::from_recipe(
        catalog.allocate_id().unwrap(),
        ResourceGeneration(1),
        ComponentRecipe::from_validated(
            Arc::new(Factory(issued.clone())),
            Arc::new(Options {
                direct: DirectOptions::validate(Default::default()).unwrap(),
                endpoint,
                path,
            }),
        ),
        Default::default(),
        "actual-direct-with-declared-originals",
    ));
    slot.bind_original_budgets(catalog.original_budgets());
    slot.bind_admission(catalog.admission.clone());
    catalog.admission.stage([slot.admission_grant()]);
    let plan = Arc::new(ComponentPlan {
        builtin: slot.clone(),
        borrowing: catalog.admission.clone(),
        borrowing_revision: 0,
        outbounds: Default::default(),
        order: vec![slot.clone()],
        started: AtomicBool::new(false),
        deferred_devices: Default::default(),
        device_pass: false,
        failures: Default::default(),
        shared_identities: Default::default(),
        source_dependencies: Default::default(),
        confirmed_declared_close: Default::default(),
        closing: Default::default(),
        close_evidence: Default::default(),
    });
    let config =
        Arc::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap());
    catalog.state.lock().unwrap().current = Some(Current {
        config,
        plan: Ok(plan.clone()),
    });
    (plan, slot, issued)
}
