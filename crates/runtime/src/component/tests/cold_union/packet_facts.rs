//! Same actual SS typed recipe; metadata never creates a Datagram role/owner.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn source_reads_once_prepared_actual_named_packet_facts_without_another_decoder_or_build() {
    let controls = Arc::new(Controls::default());
    controls.packet_facts.store(true, Ordering::SeqCst);
    let catalog = catalog(&controls);
    let candidate = config();
    let plan = catalog
        .compile_with_floor(&candidate, None, &BTreeMap::new(), true, |resources| {
            let facts = resources
                .prepared_named_packet_path("a")?
                .expect("actual decoder declared metadata");
            assert_eq!(facts.endpoint.port, 10001);
            assert!(!facts.associated);
            assert_eq!(controls.conversions.load(Ordering::SeqCst), 1);
            assert!(resources.prepared_named_packet_path("a")?.as_ref() == Some(&facts));
            assert_eq!(
                controls.conversions.load(Ordering::SeqCst),
                1,
                "the same source facts cannot call the protocol decoder again"
            );
            Ok(BTreeSet::new())
        })
        .unwrap();
    assert_eq!(controls.conversions.load(Ordering::SeqCst), 2);
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
    let recipe = plan.configured("a").unwrap().recipe.clone();
    let snapshot = recipe.clone();
    assert_eq!(snapshot.packet_path_facts().unwrap().endpoint.port, 10001);
    assert!(snapshot.packet_path_facts() == recipe.packet_path_facts());
    assert_eq!(
        snapshot.packet_path_facts().unwrap().recipe_identity(),
        recipe.spec().static_identity.as_ref()
    );
    assert!(
        !recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram),
        "data cannot manufacture a role/provider or a native grant"
    );
    assert_eq!(controls.conversions.load(Ordering::SeqCst), 2);
    assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
}

#[test]
fn typed_packet_facts_reject_a_foreign_active_name_or_different_actual_recipe_stamp() {
    for wrong_name in [false, true] {
        let controls = Arc::new(Controls::default());
        controls.packet_facts.store(true, Ordering::SeqCst);
        controls
            .packet_fact_wrong_name
            .store(wrong_name, Ordering::SeqCst);
        controls
            .packet_fact_wrong_stamp
            .store(!wrong_name, Ordering::SeqCst);
        let catalog = catalog(&controls);
        let failure = catalog
            .compile_plan(&config(), None)
            .err()
            .expect("reject foreign cold data");
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(&failure);
        let mut stale = false;
        while let Some(error) = current {
            if error
                .downcast_ref::<zero_platform_tokio::contract::ChannelError>()
                .is_some_and(|error| error.reason == Reason::StalePlan)
            {
                stale = true;
            }
            current = error.source();
        }
        assert!(
            stale,
            "actual typed StalePlan source must survive the named/catalog wrappers: {failure:?}"
        );
        assert_eq!(controls.builds.load(Ordering::SeqCst), 0);
    }
}
