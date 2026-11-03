use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_domain_network_replaces_pending_dns_and_blocks_new_io_until_old_originals_close(
) {
    timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let endpoint = Endpoint::new().await;
        let (proxy, running, _) = start_domain(endpoint.config(&dns, false)).await;
        let server = endpoint.serve(false, 2, vec![b"owned".to_vec()]);
        // Observe the actual DNS socket suspended, rather than assuming a delay means pending I/O.
        let old = {
            let proxy = proxy.clone();
            tokio::spawn(async move {
                let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("old")
                    .await
                    .unwrap();
                let result = dispatch.dispatch(input(53, b"owned")).await;
                assert!(result.is_err());
                dispatch.finish_all();
            })
        };
        let old_request = dns.request().await;
        let prior = pending(&proxy).await;
        proxy.egress_interface_control().invalidate_network();
        let next = tokio::spawn(open(proxy.clone(), 2, b"owned"));
        let request = dns.request().await;
        dns_closed(&prior);
        old.await.unwrap();
        let current = pending(&proxy).await;
        assert_eq!(
            prior.original.as_ref().unwrap().instance(),
            current.original.as_ref().unwrap().instance()
        );
        assert!(prior
            .task
            .scope_identity()
            .same_scope(&current.task.scope_identity()));
        request.answer(false, false);
        let mut dispatch = next.await.unwrap();
        current.task.wait().await;
        dns_closed(&current);
        let registration = original(&proxy);
        let observed = registration.observe().unwrap();
        let native = observed.slots()[0].original().clone();
        proxy.egress_interface_control().invalidate_network();
        let mut packet = input(53, b"owned");
        packet.client_session_id = Some(3);
        assert!(dispatch.dispatch(packet).await.is_err());
        dns.no_new_request();
        assert!(native.closed().is_none());
        retire(&proxy, dispatch).await;
        assert!(native.matches_closed(&native.closed().unwrap()));
        let next = tokio::spawn(open(proxy.clone(), 4, b"owned"));
        let request = dns.request().await;
        assert!(prior.task.is_joined());
        request.answer(false, false);
        let dispatch = next.await.unwrap();
        retire(&proxy, dispatch).await;
        drop(old_request);
        server.await.unwrap();
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}
