//! Actual old native cleanup suspends before any replacement port acquisition.
use super::*;
use std::{
    future::Future,
    sync::{atomic::Ordering, Arc},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration, Instant},
};
mod fixture;
#[cfg(feature = "vless")]
mod hot_users;
use fixture::*;

#[tokio::test]
async fn different_port_waits_for_original_native_cleanup_across_cancelled_reconcile() {
    prove_stop_before_bind(false).await;
}
#[tokio::test]
async fn every_affected_old_owner_closes_before_first_candidate_bind_and_unchanged_arc_serves() {
    prove_stop_before_bind(true).await;
}
async fn prove_stop_before_bind(multiple: bool) {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new(if multiple { 3 } else { 1 }).await;
        let indexes: &[usize] = if multiple { &[0, 2] } else { &[0] };
        let mut probes = Vec::new();
        let mut candidate = (*fixture.old).clone();
        for index in indexes {
            let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            candidate.inbounds[*index].listen.port = probe.local_addr().unwrap().port();
            probes.push(probe);
        }
        fixture
            .proxy
            .engine
            .stage_runtime_config(candidate)
            .unwrap();
        let candidate = fixture.proxy.engine().config();
        let next = factory(&fixture.proxy);
        let rollback = fixture.rollback.clone();
        let protocols = fixture.proxy.protocols.clone();
        let pending = fixture.gate.pending.clone();
        let original = fixture.originals.last().unwrap().clone();
        let old_address = format!(
            "127.0.0.1:{}",
            fixture.old.inbounds.last().unwrap().listen.port
        );
        let mut applying = Box::pin(reconcile_inbounds(
            &protocols,
            None,
            &next,
            &rollback,
            &candidate,
            fixture.state(),
        ));
        drop(probes);
        tokio::select! {
            _ = pending.notified() => {},
            _ = &mut applying => panic!("old actual native cleanup is still Pending"),
        }
        drop(applying);
        assert!(!fixture.gate.closed.load(Ordering::Acquire));
        assert!(original.terminal_for_test().is_none());
        assert!(Arc::ptr_eq(
            &original,
            &fixture
                .proxy
                .inbound_owners
                .owner(&fixture.old.inbounds.last().unwrap().tag)
                .unwrap()
        ));
        assert!(
            TcpListener::bind(&old_address).await.is_err(),
            "the original listener FD is still owned"
        );
        assert!(tokio::net::UdpSocket::bind(fixture.gate.address)
            .await
            .is_err());
        // Real probes prove no candidate pre-bind. Keep their sockets until
        // immediately before resuming, so the fixture cannot lose those ports.
        let mut still_free = Vec::new();
        for index in indexes {
            still_free.push(
                TcpListener::bind(("127.0.0.1", candidate.inbounds[*index].listen.port))
                    .await
                    .unwrap(),
            );
        }
        if multiple {
            assert!(
                fixture.proxy.inbound_owners.owner("old-0").is_none(),
                "first old owner already confirmed closed"
            );
            assert!(!fixture.active.contains_key("old-0"));
            assert!(Arc::ptr_eq(
                &fixture.originals[1],
                &fixture.proxy.inbound_owners.owner("old-1").unwrap()
            ));
            ready_socks(fixture.old.inbounds[1].listen.port).await;
        }
        fixture.gate.release.write_u8(1).await.unwrap();
        drop(still_free);
        reconcile_inbounds(
            &protocols,
            None,
            &next,
            &rollback,
            &candidate,
            fixture.state(),
        )
        .await
        .unwrap();
        assert!(fixture.gate.closed.load(Ordering::Acquire));
        let _closed_native = tokio::net::UdpSocket::bind(fixture.gate.address)
            .await
            .unwrap();
        let _closed_old = TcpListener::bind(&old_address).await.unwrap();
        assert!(original.terminal_for_test().unwrap().closed_io_receipts() >= 2);
        for index in indexes {
            let tag = &candidate.inbounds[*index].tag;
            let owner = fixture.proxy.inbound_owners.owner(tag).unwrap();
            assert_ne!(owner.identity(), fixture.originals[*index].identity());
            assert!(owner.is_running());
            ready_socks(candidate.inbounds[*index].listen.port).await;
        }
        if multiple {
            assert!(Arc::ptr_eq(
                &fixture.originals[1],
                &fixture.proxy.inbound_owners.owner("old-1").unwrap()
            ));
        }
        fixture.finish().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn switched_port_conflict_recovers_stopped_tags_from_actual_last_good_snapshot() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new(2).await;
        let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let next_port = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut candidate = (*fixture.old).clone();
        candidate.inbounds[0].listen.port = occupied.local_addr().unwrap().port();
        candidate.inbounds[1].listen.port = next_port.local_addr().unwrap().port();
        fixture.proxy.engine.stage_runtime_config(candidate).unwrap();
        let candidate = fixture.proxy.engine().config();
        let next = factory(&fixture.proxy);
        let rollback = fixture.rollback.clone();
        let protocols = fixture.proxy.protocols.clone();
        let pending = fixture.gate.pending.clone();
        let mut applying = Box::pin(reconcile_inbounds(&protocols, None, &next, &rollback, &candidate, fixture.state()));
        tokio::select! {
            _ = pending.notified() => {},
            result = &mut applying => panic!("a conflicting new port must not be tried before StopOld: {result:?}"),
        }
        drop(applying);
        assert!(fixture.proxy.inbound_owners.owner("old-0").is_none());
        assert!(!fixture.active.contains_key("old-0"));
        // Active no longer records the first old config; the immutable actual
        // rollback factory still does, including its original tag and port.
        assert_eq!(rollback.config_handle().inbounds[0], fixture.old.inbounds[0]);
        fixture.gate.release.write_u8(1).await.unwrap();
        drop(next_port);
        let error = reconcile_inbounds(&protocols, None, &next, &rollback, &candidate, fixture.state()).await.unwrap_err();
        assert!(fixture.gate.closed.load(Ordering::Acquire));
        assert!(fixture.originals.iter().all(|owner| owner.terminal_for_test().is_some()));
        assert!(crate::transport::channel_failure(&error).is_some(), "actual owned bind failure remains typed");
        let mut pending_faults = fixture.proxy.inbound_owners.identities();
        let old = fixture.old.clone();
        loop {
            match reconcile_inbounds(&protocols, None, &rollback, &rollback, &old, fixture.state()).await {
                Ok(()) => break,
                Err(error) if crate::runtime::inbound_owners::released_fault_identity(&error)
                    .is_some_and(|identity| pending_faults.remove(&identity)) => {},
                Err(error) => panic!("last-good recovery remained unconfirmed: {error}"),
            }
        }
        for (index, inbound) in old.inbounds.iter().enumerate() {
            assert_eq!(fixture.active.get(&inbound.tag), Some(inbound));
            let recovered = fixture.proxy.inbound_owners.owner(&inbound.tag).unwrap();
            assert_ne!(recovered.identity(), fixture.originals[index].identity());
            ready_socks(inbound.listen.port).await;
        }
        assert!(TcpListener::bind(occupied.local_addr().unwrap()).await.is_err(), "external conflicting owner is unchanged");
        fixture.finish().await;
    }).await.unwrap();
}
