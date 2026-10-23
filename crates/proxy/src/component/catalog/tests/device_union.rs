use super::*;

fn config() -> Arc<RuntimeConfig> {
    Arc::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap())
}
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(3)
}
#[tokio::test]
async fn same_config_explicit_device_candidate_keeps_actual_ordinary_slots_and_wins_the_cold_lookup(
) {
    let catalog = ComponentCatalog::default();
    let config = config();
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    let current = catalog.current(&config).unwrap();
    let candidate = catalog
        .prepare_with_device_resources(config.clone(), |_| Ok(BTreeSet::new()))
        .unwrap();
    assert!(!Arc::ptr_eq(&current, &candidate));
    assert!(Arc::ptr_eq(
        &current.direct(None).unwrap(),
        &candidate.direct(None).unwrap()
    ));
    assert!(Arc::ptr_eq(
        &candidate,
        &catalog.prepare(config.clone()).unwrap()
    ));
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    assert!(Arc::ptr_eq(&candidate, &catalog.current(&config).unwrap()));
    catalog.shutdown(deadline()).await.unwrap();
}
#[tokio::test]
async fn actual_host_identity_outside_this_resource_pass_is_rejected_and_cannot_fall_back_to_current(
) {
    let catalog = ComponentCatalog::default();
    let config = config();
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    // This really came from the original host, but was not issued for any
    // recipe in this actual resource pass. Scalar identity is not a grant.
    let foreign_to_pass = catalog.allocate_id().unwrap();
    let failed = catalog
        .prepare_with_device_resources(config.clone(), |_| Ok(BTreeSet::from([foreign_to_pass])))
        .unwrap_err();
    assert_eq!(failed.reason, Reason::InvalidConfig);
    assert!(Arc::ptr_eq(&failed, &catalog.prepare(config).unwrap_err()));
    catalog.shutdown(deadline()).await.unwrap();
}
