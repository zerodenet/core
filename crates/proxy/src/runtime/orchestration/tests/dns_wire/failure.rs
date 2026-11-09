//! Failed real Node warm returns its candidate permission through the same job.
use super::*;
#[tokio::test]
async fn actual_failed_target_warm_restores_old_admission_after_its_original_query_ack_without_refunding_held_input(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let initial = config(&dns, 1024, None);
        let proxy = Proxy::new(initial.clone()).unwrap();
        let originals = proxy.protocols.runtime_host().original_budgets();
        let book = originals.dns_wire.clone();
        let running = proxy.clone().spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(initial.clone(), Duration::from_secs(5)).await.unwrap();
        let held = proxy.resolver.prepare_wire_input(256).unwrap();
        let baseline = book.snapshot().occupied;
        let target = config(&dns, 262144, Some(peer.local_addr().unwrap()));
        let mut applying = Box::pin(handle.apply_runtime_config_and_wait(target, Duration::from_secs(5)));
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut applying => panic!("target must reach its actual Node peer before rejection: {result:?}"),
        };
        assert!(!request.tcp);
        let original = proxy.resolver.native_query_observations().into_iter()
            .find(|actual| actual.io_pending).expect("same real pending Node Query");
        assert!(book.snapshot().occupied.bytes() > 1024);
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(1024, 1024));
        request.answer(false, true); // Genuine upstream SERVFAIL, not a cleanup fault.
        assert!(applying.await.is_err());
        let joined = tokio::time::timeout(Duration::from_secs(3), original.task.wait_registration_released()).await.unwrap();
        assert!(original.task.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        assert_eq!(proxy.engine().config().as_ref(), &initial);
        assert!(Arc::ptr_eq(&book, &proxy.protocols.runtime_host().original_budgets().dns_wire));
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(1024, 1024));
        assert_eq!(book.snapshot().occupied, baseline,
            "failed warm frees its actual backing but keeps the original input charge");
        assert_eq!(held.len(), 256);
        let recovered = proxy.resolver.prepare_wire_input(1).unwrap();
        drop(recovered);
        // A new accepted grow can commit on the very same counter without
        // re-dialing or waiting for the failed domain's existing error cooldown.
        handle.apply_runtime_config_and_wait(config(&dns, 262144, None), Duration::from_secs(5)).await.unwrap();
        assert_eq!(book.snapshot().limit, PacketMetadataLayout::new(262144, 262144));
        assert_eq!(book.snapshot().occupied, baseline);
        drop(held);
        running.shutdown().await.unwrap();
        assert_eq!(originals.carrier.snapshot().charged, 0);
        drop(peer);
        dns.close().await;
    }).await.unwrap();
}
