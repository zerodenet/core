//! Dynamic ECH material is an actual Node Query in the same ordinary Host.
use super::*;
#[tokio::test]
async fn ordinary_grpc_dynamic_ech_uses_declared_node_query_and_keeps_its_ca_material() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for multi in [false, true] {
            let directory = Directory::new();
            let path = directory.0.join("ca.pem");
            let peer = Peer::new().await;
            std::fs::write(&path, &peer.pem).unwrap();
            let dns = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut candidate = config(&peer, &path, "rustls", multi);
            let zero_config::OutboundProtocolConfig::Vless { tls: Some(tls), .. } =
                &mut candidate.outbounds[0].protocol
            else {
                unreachable!()
            };
            tls.options.ech_config_list =
                format!("cover.ech.test+udp://{}", dns.local_addr().unwrap());
            tls.options.ech_force_query = zero_config::EchForceQueryConfig::Half;
            candidate.runtime.native_dns_max_queries = Some(1);
            let proxy = Proxy::new(candidate.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
                .await
                .unwrap();
            let first_resolver = proxy.tcp_runtime_services().upstream().ech_resolver();
            let later_resolver = proxy.tcp_runtime_services().upstream().ech_resolver();
            assert!(
                Arc::ptr_eq(&first_resolver, &later_resolver),
                "short current-service borrows preserve the same original ECH cache"
            );
            std::fs::remove_file(&path).unwrap();
            let wire = async {
                let mut bytes = vec![0; 65536];
                let (size, source) = dns.recv_from(&mut bytes).await.unwrap();
                bytes.truncate(size);
                assert_eq!(&bytes[size - 4..], &[0, 65, 0, 1]);
                assert!(bytes.windows(b"cover".len()).any(|part| part == b"cover"));
                let query = proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|query| query.original.is_some())
                    .expect("real pending type65 Node Query owns a declared original");
                assert!(!query.task.is_joined());
                assert!(query.original.as_ref().unwrap().closed().is_none());
                let response = zero_dns::udp::build_dns_response(&bytes, &[]);
                dns.send_to(&response, source).await.unwrap();
                query
            };
            let (mut first, query) = tokio::join!(loan(&proxy), wire);
            echo(&mut first).await;
            let original_query = query.original.unwrap();
            assert!(query.task.wait().await.result().is_ok());
            assert!(original_query.closed().unwrap().matches(&original_query));
            assert!(original_query.closed().unwrap().had_native_acquisition());
            let physical = first.owner.original().clone();
            let services = proxy.tcp_runtime_services();
            let plan = services.components.as_ref().unwrap();
            let pool = plan.component(physical.instance().component).unwrap();
            assert!(pool
                .recipe
                .spec()
                .dependencies
                .contains(&original_query.instance().component));
            let driver = physical.driver().unwrap().clone();
            assert!(!driver.is_joined());
            let mut second = loan(&proxy).await;
            echo(&mut second).await;
            assert!(second.owner.original().same_original(&physical));
            assert_eq!(peer.accepts.load(Ordering::SeqCst), 1);
            returned(first).await;
            echo(&mut second).await;
            returned(second).await;
            running.shutdown().await.unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            assert!(physical.closed().unwrap().matches(&physical));
            assert!(physical.owner().closed().unwrap().matches(physical.owner()));
            peer.close().await;
        }
    })
    .await
    .unwrap();
}
