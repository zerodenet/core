use super::super::catalog::ComponentCatalog;
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use zero_config::{OutboundAddressFamily, RuntimeConfig};

fn config() -> Arc<RuntimeConfig> {
    Arc::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"selected","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap())
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn cold_plan_is_cached_and_unrelated_updates_reuse_real_role() {
    let catalog = ComponentCatalog::default();
    let config = config();
    let plan = catalog.prepare(config.clone()).unwrap();
    let same = catalog.prepare(config.clone()).unwrap();
    assert!(Arc::ptr_eq(&plan, &same));
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    let original = plan.direct(Some("selected")).unwrap();
    let role = original.stream(deadline()).await.unwrap();
    let mut updated = config.as_ref().clone();
    updated.outbounds[0].udp.enabled = !updated.outbounds[0].udp.enabled;
    let updated = catalog.prepare(Arc::new(updated)).unwrap();
    let reused = updated.direct(Some("selected")).unwrap();
    assert!(Arc::ptr_eq(&original, &reused));
    assert!(Arc::ptr_eq(
        &role,
        &reused.stream(deadline()).await.unwrap()
    ));
    catalog.shutdown(deadline()).await.unwrap();
    assert!(reused.stream(deadline()).await.is_err());
}

#[tokio::test]
async fn changed_static_options_advance_generation_without_identity_reuse() {
    let catalog = ComponentCatalog::default();
    let original = config();
    let first = catalog.prepare(original.clone()).unwrap();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = first.direct(Some("selected")).unwrap();
    let mut changed = original.as_ref().clone();
    changed.outbounds[0].dial.address_family = OutboundAddressFamily::OnlyIpv4;
    let second = catalog.prepare(Arc::new(changed)).unwrap();
    let new = second.direct(Some("selected")).unwrap();
    assert_eq!(old.id, new.id);
    assert_eq!(old.generation.checked_next(), Some(new.generation));
    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(old.dial.address_family, zero_traits::AddressFamily::Auto);
    assert_eq!(
        new.dial.address_family,
        zero_traits::AddressFamily::OnlyIpv4
    );
    let mut removed = original.as_ref().clone();
    removed.outbounds.clear();
    let removed_config = Arc::new(removed);
    let removed = catalog.prepare(removed_config.clone()).unwrap();
    catalog
        .start(removed_config.clone(), deadline())
        .await
        .unwrap();
    catalog.commit(&removed_config, || Ok(())).unwrap();
    assert!(removed.direct(Some("selected")).is_err());
    let readded = catalog
        .prepare(original)
        .unwrap()
        .direct(Some("selected"))
        .unwrap();
    assert_ne!(readded.id, old.id);
}

#[tokio::test]
async fn stopped_catalog_builds_fresh_instances_for_a_later_runtime() {
    let catalog = ComponentCatalog::default();
    let config = config();
    let first = catalog.prepare(config.clone()).unwrap();
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
    let second = catalog.prepare(config.clone()).unwrap();
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    assert_ne!(
        first.direct(None).unwrap().id,
        second.direct(None).unwrap().id
    );
    assert!(first
        .direct(None)
        .unwrap()
        .stream(deadline())
        .await
        .is_err());
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn expired_caller_cannot_poison_an_unstarted_component() {
    let catalog = ComponentCatalog::default();
    let slot = catalog.prepare(config()).unwrap().direct(None).unwrap();
    assert!(slot.stream(Instant::now()).await.is_err());
    assert!(slot.stream(deadline()).await.is_ok());
    assert!(slot.shutdown(deadline()).await.unwrap().is_complete());
}

#[tokio::test]
async fn supplied_factory_state_builds_the_actual_cached_runtime_roles() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zero_platform_tokio::contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
        lifecycle::BuildError,
    };
    use zero_traits::ProtocolMetadata;

    struct Factory(Arc<AtomicUsize>);
    impl ComponentFactory for Factory {
        type Options = crate::component::DirectOptions;
        fn describe(&self, options: &Self::Options) -> ComponentSpec {
            crate::component::DirectFactory.describe(options)
        }
        fn build(
            &self,
            options: Arc<Self::Options>,
            context: BuildContext,
        ) -> Result<ComponentInstance, BuildError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            crate::component::DirectFactory.build(options, context)
        }
    }
    let builds = Arc::new(AtomicUsize::new(0));
    let retained = Arc::downgrade(&builds);
    let mut factories = crate::component::ComponentRegistry::default();
    factories
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory(builds.clone()),
            crate::adapters::direct::factory::decode,
        )
        .unwrap();
    let catalog = ComponentCatalog::with_factories(factories);
    let config = config();
    let plan = catalog.prepare(config.clone()).unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), 0, "prepare must stay cold");
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    let role = plan
        .direct(Some("selected"))
        .unwrap()
        .stream(deadline())
        .await
        .unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        2,
        "builtin and selected use supplied factory"
    );
    let cached = catalog.prepare(config).unwrap();
    cached.start(deadline()).await.unwrap();
    assert!(Arc::ptr_eq(
        &role,
        &cached
            .direct(Some("selected"))
            .unwrap()
            .stream(deadline())
            .await
            .unwrap()
    ));
    assert_eq!(builds.load(Ordering::SeqCst), 2);
    catalog.shutdown(deadline()).await.unwrap();
    drop(builds);
    drop(catalog);
    drop(plan);
    drop(cached);
    assert!(
        retained.upgrade().is_none(),
        "roles must not retain factory state"
    );
    drop(role);
}
