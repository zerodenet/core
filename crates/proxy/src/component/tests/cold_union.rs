//! Catalog-level cold assembly, not production activation or user I/O closure.
#![cfg(feature = "shadowsocks")]

use super::*;
use crate::component::ComponentRegistry;
use std::error::Error;
#[path = "cold_union/fixtures.rs"]
mod fixtures;
use fixtures::{catalog, config, resource, Controls};

#[test]
fn actual_ss_consumers_and_one_replay_owner_compile_in_the_same_host_union() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let plan = catalog.compile_plan(&config, None).unwrap();
    let shared = resource(&plan);
    assert!(shared.recipe.spec().task_limit.is_some());
    assert_eq!(controls.conversions.load(Ordering::SeqCst), 2);
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
    assert_eq!(plan.order.len(), 5);
    for tag in ["a", "b"] {
        let consumer = plan.configured(tag).unwrap();
        assert_ne!(consumer.id, shared.id);
        assert_eq!(consumer.recipe.spec().dependencies, [shared.id]);
        assert_eq!(consumer.dependency_instances(), &[shared.instance_id()]);
        let before = plan
            .order
            .iter()
            .position(|slot| slot.id == shared.id)
            .unwrap();
        let after = plan
            .order
            .iter()
            .position(|slot| slot.id == consumer.id)
            .unwrap();
        assert!(before < after);
        assert!(consumer.service_admission().is_none());
    }
    assert!(plan.builtin.recipe.spec().dependencies.is_empty());
    assert!(!plan.started.load(Ordering::Acquire));
    let state = catalog.state.lock().unwrap();
    assert!(state.current.is_none() && state.staged.is_none() && state.cold.is_none());
}

#[test]
fn unchanged_consumers_redeclare_resources_and_reuse_the_actual_bound_slots() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let first = Arc::new(catalog.compile_plan(&config, None).unwrap());
    let next = catalog
        .compile_plan(&config, Some((&config, &first)))
        .unwrap();
    assert_eq!(controls.conversions.load(Ordering::SeqCst), 4);
    for (old, new) in first.order.iter().zip(&next.order) {
        assert!(Arc::ptr_eq(old, new));
        assert_eq!(old.dependency_instances(), new.dependency_instances());
    }
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}

#[test]
fn same_resource_identity_with_changed_full_settings_advances_both_real_consumers_once() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let first = Arc::new(catalog.compile_plan(&config, None).unwrap());
    controls.extra_task.store(true, Ordering::SeqCst);
    let next = Arc::new(
        catalog
            .compile_plan(&config, Some((&config, &first)))
            .unwrap(),
    );
    assert_eq!(resource(&first).id, resource(&next).id);
    assert_eq!(resource(&next).generation, ResourceGeneration(2));
    for tag in ["a", "b"] {
        let old = first.configured(tag).unwrap();
        let new = next.configured(tag).unwrap();
        assert_eq!(old.id, new.id);
        assert_eq!(new.generation, ResourceGeneration(2));
        assert!(!Arc::ptr_eq(&old, &new));
        assert_eq!(new.dependency_instances(), &[resource(&next).instance_id()]);
    }
    assert!(Arc::ptr_eq(&first.builtin, &next.builtin));
    assert!(Arc::ptr_eq(
        &first.configured("separate").unwrap(),
        &next.configured("separate").unwrap()
    ));
    let repeated = catalog
        .compile_plan(&config, Some((&config, &next)))
        .unwrap();
    for (old, new) in next.order.iter().zip(&repeated.order) {
        assert!(Arc::ptr_eq(old, new));
    }
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}

#[test]
fn conflicting_full_resource_settings_discard_the_pass_without_polluting_prior_identity() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let original = Arc::new(catalog.compile_plan(&config, None).unwrap());
    controls.capacity.store(1, Ordering::SeqCst);
    controls.conflict.store(true, Ordering::SeqCst);
    let error = catalog
        .compile_plan(&config, Some((&config, &original)))
        .unwrap_err();
    let registration = error
        .source()
        .unwrap()
        .downcast_ref::<crate::component::RegistrationError>()
        .unwrap();
    let crate::component::RegistrationError::Planning(conflict) = registration else {
        panic!("actual shared-resource full-spec conflict")
    };
    assert_eq!(conflict.reason, Reason::InvalidConfig);
    let failed = *controls.requests.lock().unwrap().last().unwrap();
    assert_eq!(conflict.component, Some(failed));
    assert_ne!(failed, resource(&original).id);
    controls.conflict.store(false, Ordering::SeqCst);
    let recovered = catalog
        .compile_plan(&config, Some((&config, &original)))
        .unwrap();
    assert_ne!(
        resource(&recovered).id,
        failed,
        "failed pass cannot publish its identity index"
    );
    assert_ne!(resource(&recovered).id, resource(&original).id);
    controls.capacity.store(0, Ordering::SeqCst);
    let same = catalog
        .compile_plan(&config, Some((&config, &original)))
        .unwrap();
    assert!(Arc::ptr_eq(resource(&same), resource(&original)));
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn selected_consumer_failure_discards_requests_and_preserves_the_real_current_plan() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    catalog
        .drive_supervised(async {
            let config = config();
            let mut baseline = config.as_ref().clone();
            baseline.outbounds.retain(|input| input.tag == "separate");
            let baseline = Arc::new(baseline);
            let deadline = || tokio::time::Instant::now() + std::time::Duration::from_secs(2);
            let current = catalog.prepare(baseline.clone()).unwrap();
            catalog.start(baseline.clone(), deadline()).await.unwrap();
            catalog.commit(&baseline, || Ok(())).unwrap();
            let original = current
                .direct(Some("separate"))
                .unwrap()
                .stream(deadline())
                .await
                .unwrap();
            controls.fail.store(true, Ordering::SeqCst);
            let rejected = catalog.prepare(config.clone()).unwrap_err();
            let failed = *controls.requests.lock().unwrap().first().unwrap();
            assert!(catalog.prepare(config.clone()).is_err());
            assert_eq!(
                controls.conversions.load(Ordering::SeqCst),
                2,
                "same failed cold candidate stays failed"
            );
            controls.fail.store(false, Ordering::SeqCst);
            let fresh = catalog
                .compile_plan(&config, Some((&baseline, &current)))
                .unwrap();
            assert_ne!(resource(&fresh).id, failed);
            assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
            {
                let state = catalog.state.lock().unwrap();
                assert!(state.staged.is_none());
                assert!(Arc::ptr_eq(
                    state.current.as_ref().unwrap().ready(),
                    &current
                ));
                assert!(Arc::ptr_eq(
                    state.cold.as_ref().unwrap().plan.as_ref().unwrap_err(),
                    &rejected
                ));
            }
            assert!(Arc::ptr_eq(
                &original,
                &current
                    .direct(Some("separate"))
                    .unwrap()
                    .stream(deadline())
                    .await
                    .unwrap()
            ));
            catalog.shutdown(deadline()).await.unwrap();
        })
        .await;
}

#[test]
fn union_graph_validation_rejects_unknown_shared_dependencies_before_any_build() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    controls.invalid_dependency.store(true, Ordering::SeqCst);
    let error = catalog.compile_plan(&config, None).unwrap_err();
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert_eq!(
        error.component,
        controls.requests.lock().unwrap().first().copied()
    );
    controls.invalid_dependency.store(false, Ordering::SeqCst);
    let fresh = catalog.compile_plan(&config, None).unwrap();
    assert_ne!(Some(resource(&fresh).id), error.component);
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cold_shared_union_cannot_cross_the_existing_production_gate_or_publish_admission() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    assert!(catalog.prepare(config.clone()).is_err());
    assert!(catalog.start(config.clone(), deadline).await.is_err());
    assert!(catalog.commit(&config, || Ok(())).is_err());
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
    let state = catalog.state.lock().unwrap();
    assert!(state.current.is_none() && state.staged.is_none());
}
