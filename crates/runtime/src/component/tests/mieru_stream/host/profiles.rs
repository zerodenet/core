use super::*;
use zero_config::OutboundProtocolConfig;

#[tokio::test]
async fn whole_profile_replacement_closes_all_endpoints_when_only_an_unused_credential_changes() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let one = Server::new().await;
        let two = Server::new().await;
        let fixture = Assembly::new(one.address);
        let mut config = (*fixture.config()).clone();
        if let OutboundProtocolConfig::Mieru { port, .. } = &mut config.outbounds[1].protocol {
            *port = two.address.port();
        } else {
            panic!();
        }
        config.outbounds[1].dial.address_family = OutboundAddressFamily::OnlyIpv4;
        let mut unused = config.outbounds[1].clone();
        unused.tag = "unused".into();
        if let OutboundProtocolConfig::Mieru { password, .. } = &mut unused.protocol {
            *password = "unused-old-secret".into();
        }
        config.outbounds.push(unused);
        let config = Arc::new(config);
        let host = &fixture.catalog;
        host.drive_supervised(async {
            let plan = host.prepare(config.clone()).unwrap();
            let a = plan.direct(Some("a")).unwrap();
            let b = plan.direct(Some("b")).unwrap();
            assert_eq!(a.dependency_instances(), b.dependency_instances());
            assert_eq!(
                a.dependency_instances(),
                plan.direct(Some("unused")).unwrap().dependency_instances()
            );
            host.start(config.clone(), deadline()).await.unwrap();
            host.commit(&config, || Ok(())).unwrap();
            let mut first = fixture.open(&a).await;
            let mut second = fixture.open(&b).await;
            first.write_all(b"one").await.unwrap();
            first.read_exact(&mut [0; 3]).await.unwrap();
            second.write_all(b"two").await.unwrap();
            second.read_exact(&mut [0; 3]).await.unwrap();
            assert_eq!(one.accepts.load(Ordering::SeqCst), 1);
            assert_eq!(two.accepts.load(Ordering::SeqCst), 1);
            let excess = fixture
                .try_open(&plan.direct(Some("unused")).unwrap())
                .await
                .err()
                .unwrap();
            assert_eq!(
                excess.reason,
                zero_platform_tokio::contract::Reason::Capacity
            );
            assert_eq!(
                fixture.pool.snapshot().connections,
                2,
                "all bindings share the same total physical budget"
            );
            let mut reordered = (*config).clone();
            reordered.outbounds.reverse();
            let reordered = Arc::new(reordered);
            let reordered_plan = host.prepare(reordered.clone()).unwrap();
            assert_eq!(
                a.dependency_instances(),
                reordered_plan
                    .direct(Some("a"))
                    .unwrap()
                    .dependency_instances()
            );
            host.start(reordered.clone(), deadline()).await.unwrap();
            host.commit(&reordered, || Ok(())).unwrap();
            assert_eq!(
                fixture.builds.load(Ordering::SeqCst),
                1,
                "reordering the same complete set must preserve the actual owner and traffic"
            );
            let mut changed = (*config).clone();
            if let OutboundProtocolConfig::Mieru { password, .. } =
                &mut changed.outbounds[2].protocol
            {
                *password = "unused-new-secret".into();
            }
            let changed = Arc::new(changed);
            let next = host.prepare(changed.clone()).unwrap();
            assert_ne!(
                a.dependency_instances(),
                next.direct(Some("a")).unwrap().dependency_instances()
            );
            let mut replacing = Box::pin(host.start(changed.clone(), deadline()));
            std::future::poll_fn(|cx| {
                assert!(replacing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
            drop(first);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), replacing.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
            drop(second);
            replacing.await.unwrap();
            host.commit(&changed, || Ok(())).unwrap();
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
            assert!(
                fixture.try_open(&a).await.is_err(),
                "historical handles cannot reopen the previous pool"
            );
            for tag in ["a", "b"] {
                let mut stream = fixture
                    .open(&host.current(&changed).unwrap().direct(Some(tag)).unwrap())
                    .await;
                stream.write_all(b"new").await.unwrap();
                stream.read_exact(&mut [0; 3]).await.unwrap();
            }
            assert_eq!(one.accepts.load(Ordering::SeqCst), 2);
            assert_eq!(two.accepts.load(Ordering::SeqCst), 2);
            host.shutdown(deadline()).await.unwrap();
            assert_eq!(fixture.pool.snapshot().connections, 0);
        })
        .await;
        one.close().await;
        two.close().await;
    })
    .await
    .unwrap();
}
