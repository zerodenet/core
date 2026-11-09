//! Same actual cold union: omission is not a failed decoder fallback.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
#[test]
fn unused_alias_declined_by_actual_cold_semantics_is_not_decoded_or_built() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let plan = catalog
        .compile_with_floor(&config, None, &BTreeMap::new(), true, |resources| {
            let original = resources.named_component("a").unwrap();
            resources.omit_unused_device_alias("a")?;
            assert!(resources.named_component("a").is_none());
            assert_ne!(resources.builtin_component(), Some(original));
            assert_eq!(controls.conversions.load(Ordering::SeqCst), 0);
            Ok(BTreeSet::new())
        })
        .unwrap();
    assert!(plan.configured("a").is_none());
    assert!(plan.configured("b").is_some());
    assert_eq!(controls.conversions.load(Ordering::SeqCst), 1);
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}
#[test]
fn decoded_or_actual_source_referenced_alias_cannot_be_omitted() {
    for referenced in [false, true] {
        let controls = Arc::new(Controls::default());
        let catalog = catalog(&controls);
        let mut config = config().as_ref().clone();
        config.inbounds=RuntimeConfig::parse(r#"{"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"mixed"}}],"route":{"final":{"type":"direct"}}}"#).unwrap().inbounds;
        let config = Arc::new(config);
        let failure = catalog
            .compile_with_floor(&config, None, &BTreeMap::new(), true, |resources| {
                if referenced {
                    let id = resources.named_component("a").unwrap();
                    resources.set_source_dependencies(BTreeMap::from([(
                        "source".into(),
                        ([31; 32], BTreeSet::from([id])),
                    )]))?;
                } else {
                    resources.prepare_named_component("a")?;
                }
                let original = resources
                    .omit_unused_device_alias("a")
                    .err()
                    .expect("actual cold boundary rejects the prepared/reference");
                Err(original)
            })
            .err()
            .expect("actual prepared/reference gate must reject");
        assert_eq!(failure.reason, Reason::StalePlan);
        assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
    }
}
#[test]
fn accepted_named_identity_is_the_same_memoized_material_without_another_decoder() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let config = config();
    let mut identity = None;
    let plan = catalog
        .compile_with_floor(&config, None, &BTreeMap::new(), true, |resources| {
            let first = resources.prepared_named_identity("a")?;
            let second = resources.prepared_named_identity("a")?;
            assert!(Arc::ptr_eq(&first, &second));
            assert_eq!(controls.conversions.load(Ordering::SeqCst), 1);
            identity = Some(first);
            Ok(BTreeSet::new())
        })
        .unwrap();
    assert!(Arc::ptr_eq(
        &identity.unwrap(),
        &plan.configured("a").unwrap().recipe.spec().static_identity
    ));
    assert_eq!(controls.conversions.load(Ordering::SeqCst), 2);
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}
