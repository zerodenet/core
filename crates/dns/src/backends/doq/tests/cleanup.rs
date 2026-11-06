//! The actual first endpoint cannot be bypassed while its IO lease blocks join.
use super::*;
#[tokio::test]
async fn unsettled_actual_endpoint_keeps_query_and_cleanup_errors_and_never_dials_next_candidate() {
    timeout(Duration::from_secs(5), async {
        let (endpoint, mut resolver) = fixture();
        let (unused, _) = fixture();
        resolver.addrs.push(unused.local_addr().unwrap());
        let registry = resolver.owners.lock().unwrap().clone();
        let resolver = Arc::new(resolver);
        let caller = tokio::spawn({
            let resolver = resolver.clone();
            async move {
                let query = crate::message::build_query(
                    "unsettled.actual.doq.test",
                    crate::message::TYPE_A,
                )
                .unwrap();
                crate::backends::ATTEMPT_DEADLINE
                    .scope(
                        Instant::now() + Duration::from_secs(1),
                        resolver.exchange(&query),
                    )
                    .await
            }
        });
        let connection = endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let size = recv.read_u16().await.unwrap() as usize;
        let mut query = vec![0; size];
        recv.read_exact(&mut query).await.unwrap();
        assert_eq!(
            crate::message::parse_question(&query).unwrap().domain,
            "unsettled.actual.doq.test"
        );
        let owners = registry.doq_snapshot();
        assert_eq!(owners.len(), 1);
        let original = owners[0].clone();
        assert_eq!(
            original.task_count(),
            2,
            "actual endpoint and connection drivers"
        );
        let held = original.query_user().unwrap();
        send.write_u16(1).await.unwrap();
        send.write_all(&[0]).await.unwrap();
        send.finish().unwrap();
        let returned = caller.await.unwrap().unwrap_err();
        assert_eq!(returned.kind(), io::ErrorKind::TimedOut);
        let failed = returned
            .get_ref()
            .unwrap()
            .downcast_ref::<crate::DnsQuicCleanupFailure>()
            .unwrap();
        assert_eq!(
            failed.query_error().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(failed.close_error().kind(), io::ErrorKind::TimedOut);
        assert!(original.closing() && !original.drained());
        assert_eq!(
            registry.doq_snapshot().len(),
            1,
            "no successor owner while this original remains unsettled"
        );
        let accept = unused.accept();
        tokio::pin!(accept);
        let observed = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(accept.as_mut(), cx))
        })
        .await;
        assert!(observed.is_pending(), "no second candidate QUIC wire");
        drop(held);
        original
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(original.drained());
        assert_eq!(
            original.task_count(),
            0,
            "both original driver exits were consumed"
        );
        assert_eq!(
            failed.query_error().unwrap().kind(),
            io::ErrorKind::InvalidData,
            "the caller retains both original errors after actual cleanup retry"
        );
        connection.closed().await;
        endpoint.close(0u32.into(), b"test finished");
        endpoint.wait_idle().await;
        unused.close(0u32.into(), b"unused candidate");
        unused.wait_idle().await;
    })
    .await
    .unwrap();
}
