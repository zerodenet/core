//! A retired role cannot hide an active user of the still-current shared owner.
use super::*;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn shared_rebuild_waits_for_historical_role_users_without_retaining_old_configurations() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let original = config();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = catalog.current(&original).unwrap();
    let slot = old.configured("a").unwrap();
    let weak = Arc::downgrade(&slot);
    let root = Arc::new(Root {
        listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        peers: Mutex::new(vec![]),
    });
    let mut io = open(&slot, &root).await;
    let mut changed = original.as_ref().clone();
    let zero_config::OutboundProtocolConfig::Shadowsocks { port, .. } =
        &mut changed.outbounds[0].protocol
    else {
        unreachable!()
    };
    *port += 1;
    let changed = Arc::new(changed);
    catalog.start(changed.clone(), deadline()).await.unwrap();
    catalog.commit(&changed, || Ok(())).unwrap();
    let updated = catalog.current(&changed).unwrap();
    assert!(
        Arc::ptr_eq(resource(&old), resource(&updated)),
        "ordinary protocol options retain the actual shared owner"
    );
    drop(slot);
    drop(old);
    let unchanged = Arc::new(changed.as_ref().clone());
    catalog.start(unchanged.clone(), deadline()).await.unwrap();
    catalog.commit(&unchanged, || Ok(())).unwrap();
    assert!(
        weak.upgrade().is_none(),
        "the running historical stream keeps no slot, recipe or old configuration"
    );
    tokio::time::timeout_at(deadline(), io.write_all(b"still alive"))
        .await
        .unwrap()
        .unwrap();
    let built = controls.builds.load(Ordering::SeqCst);
    controls.extra_task.store(true, Ordering::SeqCst);
    let candidate = Arc::new(unchanged.as_ref().clone());
    let error = catalog
        .start(
            candidate.clone(),
            Instant::now() + Duration::from_millis(30),
        )
        .await
        .unwrap_err();
    assert_eq!(error.reason, Reason::Timeout);
    assert_eq!(
        controls.builds.load(Ordering::SeqCst),
        built,
        "the historical user still charges its actual resource generation"
    );
    assert_eq!(
        io.write_all(b"revoked").await.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    drop(io);
    catalog.start(candidate.clone(), deadline()).await.unwrap();
    catalog.commit(&candidate, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
}
