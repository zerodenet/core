//! A byte-policy-only change must follow the existing actual StopOld path.
use super::*;
#[tokio::test]
async fn buffer_policy_only_change_keeps_old_stream_until_real_users_and_originals_retire() {
    let catalog = ComponentCatalog::default();
    let initial = Arc::new(RuntimeConfig::parse(r#"{"runtime":{"quic_max_queue_packets":2,"quic_total_max_buffered_bytes":32},"route":{"final":{"type":"direct"}}}"#).unwrap());
    catalog.start(initial.clone(), end()).await.unwrap();
    catalog.commit(&initial, || Ok(())).unwrap();
    let old = catalog.current(&initial).unwrap().direct(None).unwrap();
    let roots = catalog.original_budgets();
    let memory = roots.carrier.buffer_budget();
    assert_eq!(memory.limits().max_queue_packets, 2);
    assert_eq!(memory.limits().total_max_bytes, 32);
    let output = memory.try_reserve(8).unwrap();
    let user = old.own_user_root();
    let lease =
        crate::component::instance::users::Lease::enter(old.id, std::slice::from_ref(&user), None)
            .unwrap()
            .unwrap();
    let (io, mut peer) = tokio::io::duplex(16);
    let mut transferred = lease.stream(Box::pin(io));
    transferred.write_all(b"before").await.unwrap();
    let mut bytes = [0; 6];
    peer.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"before");
    let mut next = (*initial).clone();
    next.runtime.quic_max_queue_packets = Some(1);
    next.runtime.quic_total_max_buffered_bytes = Some(16);
    let next = Arc::new(next);
    let candidate = catalog.prepare(next.clone()).unwrap();
    assert_eq!(
        old.recipe.spec(),
        candidate.direct(None).unwrap().recipe.spec()
    );
    assert_eq!(
        catalog.start(next.clone(), end()).await.unwrap_err().reason,
        Reason::Capacity
    );
    assert_eq!(memory.limits().total_max_bytes, 32);
    peer.write_all(b"alive!").await.unwrap();
    transferred.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"alive!");
    drop(transferred);
    assert_eq!(peer.read(&mut [0; 1]).await.unwrap(), 0);
    let external = catalog.try_pause_replacement_users(&candidate).unwrap();
    let prepared = catalog
        .prepare_resource_start(next.clone(), end())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(prepared.plan(), &candidate));
    let actual = old.original_carrier_owner(0).unwrap();
    assert!(actual.closed().unwrap().matches(&actual));
    assert!(!actual.users_closed().unwrap().had_native_acquisition());
    assert_eq!(memory.limits().total_max_bytes, 16);
    assert_eq!(memory.limits().max_queue_packets, 1);
    assert_eq!(
        memory.used_bytes(),
        8,
        "held pure output survives policy installation and stays charged"
    );
    assert!(memory.same_budget(&catalog.original_budgets().carrier.buffer_budget()));
    drop(external);
    prepared.start(end()).await.unwrap();
    catalog.commit(&next, || Ok(())).unwrap();
    assert_eq!(memory.used_bytes(), 8);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
    catalog.shutdown(end()).await.unwrap();
}
