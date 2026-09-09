use super::*;

#[tokio::test]
async fn retiring_logical_resume_closes_cached_udp_handle_without_closing_shared_connection() {
    timeout(Duration::from_secs(10), async {
        let server = Server::start();
        let pool = Hysteria2ConnectionPool::default();
        let leaf = server.leaf(&pool);
        let other = channel(&leaf).await;
        let resume = leaf.flow_resume();
        let cache_key = |resume: &super::super::super::Hysteria2ManagedDatagramFlowResume| {
            super::super::super::managed_datagram_connector_flow_from_resume(
                resume,
                "127.0.0.1",
                server.port,
            )
            .into_cache_key()
        };
        assert_eq!(cache_key(&resume), cache_key(&resume.clone()));
        assert_ne!(cache_key(&resume), cache_key(&leaf.flow_resume()));
        let flow = establish_hysteria2_udp_flow_connection(
            "127.0.0.1",
            server.port,
            &Address::Domain("echo.test".into()),
            53,
            b"initial",
            resume.clone(),
            &sockets(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut receiver = flow.subscribe_responses();
        assert_eq!(receiver.recv().await.unwrap().2, b"initial");
        flow.send(&Address::Domain("echo.test".into()), 53, b"ping")
            .await
            .unwrap();
        receiver.recv().await.unwrap();
        drop(resume);
        while !flow.is_closed() {
            tokio::task::yield_now().await;
        }
        assert!(flow
            .send(&Address::Domain("echo.test".into()), 53, b"after-close")
            .await
            .is_err());
        datagram(&other, 7).await;
        assert_eq!(server.authenticated.load(Ordering::Relaxed), 1);
    })
    .await
    .unwrap();
}
