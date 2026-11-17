use super::*;

#[tokio::test]
async fn ordinary_source_network_refresh_uses_the_same_host_without_recovery_alias_authority() {
    let inventory = ProtocolInventory::default();
    let config =
        Arc::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let first = inventory.prepare_device_components(config.clone()).unwrap();
    let original = first.direct(None).unwrap().instance_id();
    let mut prepared = inventory
        .prepare_resource_start(config.clone(), deadline)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(prepared.plan(), &first));
    let refreshed = inventory
        .refresh_prepared_device_components(&mut prepared, deadline)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(prepared.plan(), &refreshed));
    assert!(!Arc::ptr_eq(&first, &refreshed));
    let current = refreshed.direct(None).unwrap().instance_id();
    assert_eq!(current.component, original.component);
    assert!(current.generation > original.generation);
    prepared.start(deadline).await.unwrap();
    inventory.commit_components(&config, || Ok(())).unwrap();
    assert!(Arc::ptr_eq(
        &refreshed,
        &inventory.host.components().current(&config).unwrap()
    ));
    inventory.shutdown_components(deadline).await.unwrap();
}
