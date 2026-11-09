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
    assert!(Arc::ptr_eq(
        &candidate,
        &catalog.prepared_candidate(&config).unwrap().unwrap()
    ));
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    assert!(Arc::ptr_eq(&candidate, &catalog.current(&config).unwrap()));
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn owning_network_restart_describes_all_old_users_before_any_actual_owner_is_stopped() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let catalog = ComponentCatalog::default();
    let config = config();
    catalog.start(config.clone(), deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    let previous = catalog.current(&config).unwrap();
    let original = previous.direct(None).unwrap();
    let users = original.own_user_root();
    let lease = crate::component::instance::users::Lease::enter(
        original.id,
        std::slice::from_ref(&users),
        None,
    )
    .unwrap()
    .unwrap();
    let (io, mut peer) = tokio::io::duplex(16);
    let mut business = lease.stream(Box::pin(io));
    let candidate = catalog
        .prepare_restarting_device_resources(config.clone(), false, |_| Ok(BTreeSet::new()))
        .unwrap();
    assert!(candidate.order.iter().all(|slot| previous
        .order
        .iter()
        .find(|old| old.id == slot.id)
        .is_none_or(|old| !Arc::ptr_eq(old, slot) && slot.generation > old.generation)));
    let error = match catalog.try_pause_replacement_users(&candidate) {
        Err(error) => error,
        Ok(_) => panic!("a live original Business cannot authorize network restart"),
    };
    assert_eq!(error.reason, Reason::Capacity);
    peer.write_all(b"still alive").await.unwrap();
    let mut bytes = [0; 11];
    business.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still alive");
    assert!(candidate
        .order
        .iter()
        .all(|slot| slot.original_carrier_owner(0).is_none()));
    drop(business);
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    let pause = catalog.try_pause_replacement_users(&candidate).unwrap();
    let mut prepared = catalog
        .prepare_resource_start(config.clone(), deadline())
        .await
        .unwrap();
    drop(pause);
    let refreshed = prepared
        .refresh_device_resources(deadline(), |_| Ok(BTreeSet::new()))
        .await
        .unwrap();
    assert!(refreshed.order.iter().all(|slot| candidate
        .order
        .iter()
        .find(|old| old.id == slot.id)
        .is_none_or(|old| !Arc::ptr_eq(old, slot) && slot.generation > old.generation)));
    assert!(Arc::ptr_eq(
        &refreshed,
        &catalog.prepared_candidate(&config).unwrap().unwrap()
    ));
    assert!(refreshed
        .order
        .iter()
        .all(|slot| slot.original_carrier_owner(0).is_none()));
    prepared.start(deadline()).await.unwrap();
    catalog.commit(&config, || Ok(())).unwrap();
    assert!(Arc::ptr_eq(&refreshed, &catalog.current(&config).unwrap()));
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
