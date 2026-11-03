//! Real catalog, protocol pool, encrypted traffic and original host providers.
//! The literal-root carrier injector is fixture assembly, not production opt-in.
mod datagram;
mod dns;
mod fixtures;
mod loan;
mod packet;
mod profiles;
use super::*;
use fixtures::*;
use std::{future::Future, sync::atomic::Ordering, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_config::OutboundAddressFamily;

#[tokio::test]
async fn cancelled_real_mieru_authentication_keeps_original_native_custody_without_a_fake_driver() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fixture = Assembly::new(listener.local_addr().unwrap());
        let config = fixture.config();
        fixture
            .catalog
            .drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let mut opening = Box::pin(fixture.try_open(&slot));
                let peer = async {
                    let (mut peer, _) = listener.accept().await.unwrap();
                    peer.read_exact(&mut [0; 1]).await.unwrap();
                    peer
                };
                let mut peer = tokio::select! {
                    _=opening.as_mut()=>panic!("real protocol authentication has no peer response"),
                    peer=peer=>peer,
                };
                std::future::poll_fn(|cx| {
                    assert!(opening.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(opening);
                fixture.catalog.shutdown(deadline()).await.unwrap();
                // Supplementary peer observation; the host's success above still
                // requires its original provider receipt, never this EOF probe.
                peer.read_to_end(&mut Vec::new()).await.unwrap();
                assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
                let config = fixture.config();
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
                fixture.catalog.shutdown(deadline()).await.unwrap();
            })
            .await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shared_mieru_owner_replacement_waits_for_transferred_streams_and_original_scope_io() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let server = Server::new().await;
        let fixture = Assembly::new(server.address);
        let config = fixture.config();
        let host = &fixture.catalog;
        host.drive_supervised(async {
            let plan = host.prepare(config.clone()).unwrap();
            let a = plan.direct(Some("a")).unwrap();
            let b = plan.direct(Some("b")).unwrap();
            assert_eq!(a.dependency_instances(), b.dependency_instances());
            assert_eq!(a.dependency_instances().len(), 1);
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 0);
            host.start(config.clone(), deadline()).await.unwrap();
            host.commit(&config, || Ok(())).unwrap();
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
            let mut first = fixture.open(&a).await;
            let mut second = fixture.open(&b).await;
            first.write_all(b"ping").await.unwrap();
            first.read_exact(&mut [0; 4]).await.unwrap();
            second.write_all(b"pong").await.unwrap();
            second.read_exact(&mut [0; 4]).await.unwrap();
            assert_eq!(
                server.accepts.load(Ordering::SeqCst),
                2,
                "tags isolate carriers within the one shared pool"
            );
            let mut changed = (*config).clone();
            for outbound in &mut changed.outbounds {
                outbound.dial.address_family = OutboundAddressFamily::OnlyIpv4;
            }
            let changed = Arc::new(changed);
            let next = host.prepare(changed.clone()).unwrap();
            assert_ne!(
                next.direct(Some("a")).unwrap().dependency_instances(),
                a.dependency_instances()
            );
            let mut replacing = Box::pin(host.start(changed.clone(), deadline()));
            std::future::poll_fn(|cx| {
                assert!(replacing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), replacing.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(
                fixture.builds.load(Ordering::SeqCst),
                1,
                "no second pool owner while original logical views are retained"
            );
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
            let fresh = host.current(&changed).unwrap().direct(Some("a")).unwrap();
            let mut stream = fixture.open(&fresh).await;
            stream.write_all(b"next").await.unwrap();
            let mut response = [0; 4];
            stream.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"next");
            assert_eq!(server.accepts.load(Ordering::SeqCst), 3);
            drop(stream);
            host.shutdown(deadline()).await.unwrap();
            assert_eq!(fixture.pool.snapshot().connections, 0);
        })
        .await;
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_mieru_retirement_retains_the_original_pool_and_prevents_new_owner_build() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let server = Server::new().await;
        let fixture = Assembly::new(server.address);
        let config = fixture.config();
        fixture
            .catalog
            .drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let stream = fixture.open(&slot).await;
                let mut closing = Box::pin(fixture.catalog.shutdown(deadline()));
                std::future::poll_fn(|cx| {
                    assert!(closing.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(closing);
                assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
                drop(stream);
                fixture.catalog.shutdown(deadline()).await.unwrap();
                assert_eq!(fixture.pool.snapshot().connections, 0);
                let config = fixture.config();
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                drop(fixture.open(&slot).await);
                fixture.catalog.shutdown(deadline()).await.unwrap();
            })
            .await;
        server.close().await;
    })
    .await
    .unwrap();
}
