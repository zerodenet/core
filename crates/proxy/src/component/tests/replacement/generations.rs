//! Real started/failed catalog owners, rather than invented generation labels.
use super::*;
use crate::component::{DirectFactory, DirectOptions};
use zero_config::OutboundAddressFamily;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::{
        erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
    },
    CloseReason,
};
use zero_traits::ProtocolMetadata;
struct Factory {
    fail_build: bool,
}
struct Owner {
    inner: Box<dyn DynResourceLifecycle>,
    fail: bool,
}
impl ComponentFactory for Factory {
    type Options = DirectOptions;
    fn describe(&self, options: &DirectOptions) -> ComponentSpec {
        DirectFactory.describe(options)
    }
    fn build(
        &self,
        options: Arc<DirectOptions>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let fail = context.generation == ResourceGeneration(2);
        if fail && self.fail_build {
            return Err(invalid(
                Reason::InvalidConfig,
                "actual second generation build failed",
            ));
        }
        let mut instance = DirectFactory.build(options, context)?;
        instance.lifecycle = erase_lifecycle(Owner {
            inner: instance.lifecycle,
            fail,
        });
        Ok(instance)
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        self.inner.start(deadline).await?;
        if self.fail {
            return Err(invalid(
                Reason::InvalidConfig,
                "actual second generation initialization failed",
            ));
        }
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.inner.shutdown(deadline).await
    }
}
fn catalog(fail_build: bool) -> ComponentCatalog {
    let mut factories = ComponentRegistry::default();
    factories
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory { fail_build },
            crate::adapters::direct::factory::decode,
        )
        .unwrap();
    ComponentCatalog::with_factories(factories)
}
fn initial() -> Arc<RuntimeConfig> {
    Arc::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"selected","protocol":{"type":"direct"}},{"tag":"separate","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap())
}
async fn run(restore_last_good: bool, fail_build: bool) {
    let catalog = catalog(fail_build);
    let original = initial();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = catalog.current(&original).unwrap();
    let previous = old.configured("selected").unwrap();
    let separate = old.configured("separate").unwrap();
    let mut changed = original.as_ref().clone();
    changed.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv4;
    let changed = Arc::new(changed);
    let failed = catalog
        .start(changed.clone(), deadline())
        .await
        .unwrap_err();
    assert_eq!(failed.reason, Reason::InvalidConfig);
    let staged = catalog
        .state
        .lock()
        .unwrap()
        .staged
        .as_ref()
        .unwrap()
        .ready()
        .clone();
    let rejected = staged.configured("selected").unwrap();
    let stale = rejected.service_admission().unwrap();
    assert_eq!(rejected.generation, ResourceGeneration(2));
    assert!(
        !rejected.can_reuse(),
        "real build/start Err is not a cold reusable recipe"
    );
    assert!(Arc::ptr_eq(
        &separate,
        &staged.configured("separate").unwrap()
    ));
    let target = if restore_last_good {
        // Model only the declared Direct owner having actually closed during
        // StopOld, not unowned users or a resourceful production replacement.
        assert!(previous.shutdown(deadline()).await.unwrap().is_complete());
        original.clone()
    } else {
        changed.clone()
    };
    catalog.start(target.clone(), deadline()).await.unwrap();
    let restored = catalog
        .state
        .lock()
        .unwrap()
        .staged
        .as_ref()
        .unwrap()
        .ready()
        .clone();
    let selected = restored.configured("selected").unwrap();
    assert_eq!(selected.id, previous.id);
    assert_eq!(selected.generation, ResourceGeneration(3));
    assert!(!Arc::ptr_eq(&selected, &previous));
    assert!(!Arc::ptr_eq(&selected, &rejected));
    assert_eq!(stale.check_current().unwrap_err().reason, Reason::StalePlan);
    assert!(selected
        .service_admission()
        .unwrap()
        .check_current()
        .is_ok());
    assert!(Arc::ptr_eq(
        &separate,
        &restored.configured("separate").unwrap()
    ));
    assert!(Arc::ptr_eq(&old.builtin, &restored.builtin));
    catalog.commit(&target, || Ok(())).unwrap();
    assert_eq!(stale.check_current().unwrap_err().reason, Reason::StalePlan);
    assert_eq!(
        catalog.state.lock().unwrap().issued[&selected.id],
        ResourceGeneration(3)
    );
    catalog.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn failed_staged_generation_is_never_reissued_for_the_same_candidate() {
    run(false, false).await;
}
#[tokio::test]
async fn revoked_last_good_recovers_above_the_failed_staged_generation_without_rebuilding_unrelated_roles(
) {
    run(true, false).await;
}

#[tokio::test]
async fn failed_prepare_cannot_publish_partial_high_watermarks_or_poison_healthy_current() {
    let catalog = catalog(false);
    let original = initial();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let current = catalog.current(&original).unwrap();
    let selected = current.configured("selected").unwrap();
    catalog
        .state
        .lock()
        .unwrap()
        .issued
        .insert(selected.id, ResourceGeneration(u64::MAX));
    let before = catalog.state.lock().unwrap().issued.clone();
    let mut changed = original.as_ref().clone();
    changed.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv4;
    let error = catalog.prepare(Arc::new(changed)).unwrap_err();
    assert_eq!(error.component, Some(selected.id));
    assert_eq!(error.reason, Reason::Internal);
    assert_eq!(catalog.state.lock().unwrap().issued, before);
    assert!(Arc::ptr_eq(
        &catalog.prepare(original.clone()).unwrap(),
        &current
    ));
    assert!(selected.can_reuse());
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn failed_build_generation_is_not_reissued_as_a_cold_recipe() {
    run(false, true).await;
}
