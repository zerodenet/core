//! Cancelling an initializing waiter cannot retire a pool already used by peers.
use super::*;
use zero_core::InboundRouteMultiplexer;

#[tokio::test]
async fn cancelling_first_tunnel_ack_waiter_keeps_the_actual_pool_borrowed_by_a_second_dns_query() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let (waiting_tx, waiting) = tokio::sync::oneshot::channel();
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = accepts.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            let profile = mieru::inbound::MieruInboundProfile::from_config(vec![("u".into(), "p".into())]);
            let connection = profile.accept_multiplexer(zero_platform_tokio::TcpRelayStream::new(socket)).await.unwrap();
            let mut first = connection.accept_next().await.unwrap().unwrap();
            // The literal DNS target yields this exact SOCKS5 CONNECT frame.
            // Consume it but withhold its ACK. ClientPool already published the
            // authenticated connection before this per-stream request was sent.
            let mut connect = [0u8; 10];
            first.read_exact(&mut connect).await.unwrap();
            assert_eq!(&connect[..4], &[5, 1, 0, 1]);
            waiting_tx.send(()).unwrap();
            let mut streams = tokio::task::JoinSet::new();
            loop {
                let next = tokio::select! { _ = stopped.cancelled() => break, next = connection.accept_next() => next };
                let Some(next) = next.unwrap() else { break; };
                let route = connection.accept_route(next).await.unwrap();
                let mieru::inbound::MieruInboundAcceptedSession::Tcp { session, mut stream } = route else { panic!("DNS detour must use actual Stream"); };
                let Address::Ipv4(ip) = session.target else { panic!("literal declared DNS target"); };
                let stopped = stopped.clone();
                streams.spawn(async move {
                    let mut peer = tokio::net::TcpStream::connect((std::net::Ipv4Addr::from(ip), session.port)).await.unwrap();
                    tokio::select! { _ = stopped.cancelled() => {}, _ = tokio::io::copy_bidirectional(&mut stream, &mut peer) => {} }
                });
            }
            drop(first);
            connection.close("fixture stopped");
            while let Some(joined) = streams.join_next().await { joined.unwrap(); }
        });
        let mut dns = Dns::new().await;
        let mut config = config([address; 3]);
        config.outbounds.truncate(1);
        config.outbound_groups.clear();
        let mut settings = dns.config();
        let mut detour = dns.server();
        let zero_config::DnsServerConfig::Udp { detour: target, .. } = &mut detour else { unreachable!() };
        *target = Some("a".into());
        settings.servers.insert("query".into(), detour);
        settings.default_server = "query".into();
        settings.policy.node_server = Some("native".into());
        config.runtime.dns = Some(settings);
        let (proxy, running, _) = start(config).await;
        let resolver = proxy.resolver.clone();
        let mut first = tokio::spawn(async move { resolver.resolve_real_type("first.ack-wait.test", 1).await });
        tokio::select! { result = waiting => result.unwrap(), result = &mut first => panic!("first query must suspend at its actual tunnel ACK: {result:?}") };
        let caller = proxy.resolver.native_query_observations().into_iter().find(|query| !query.task.is_joined()).expect("actual first DNS caller");
        assert!(!first.is_finished());
        let resolver = proxy.resolver.clone();
        let mut second = tokio::spawn(async move { resolver.resolve_real_type("second.concurrent.test", 1).await });
        let request = tokio::select! { request = dns.request() => request, result = &mut second => panic!("second query failed before actual DNS IO: {result:?}") };
        assert!(request.tcp);
        let second_caller = pending(&proxy).await;
        assert!(!caller.task.same_registration(&second_caller.task));
        let child = second_caller.original.clone().unwrap();
        let pool = child.parent().expect("the same actual Mieru donor").clone();
        assert!(!pool.driver().unwrap().is_joined());
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap();
        assert!(!second.is_finished(), "first waiter cancellation cannot terminate another actual query");
        assert!(!pool.driver().unwrap().is_joined(), "the original pool Scope still owns its actual driver");
        assert!(pool.closed().is_none(), "a pending waiter cannot retire a published shared donor");
        request.answer(false, false);
        second.await.unwrap().unwrap();
        second_caller.task.wait().await;
        let resolver = proxy.resolver.clone();
        let mut third = tokio::spawn(async move { resolver.resolve_real_type("third.concurrent.test", 1).await });
        let request = tokio::select! { request = dns.request() => request, result = &mut third => panic!("shared donor was not reusable: {result:?}") };
        let third_caller = pending(&proxy).await;
        assert!(third_caller.original.as_ref().unwrap().parent().unwrap().same_original(&pool));
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        request.answer(false, false);
        third.await.unwrap().unwrap();
        third_caller.task.wait().await;
        running.shutdown().await.unwrap();
        assert!(pool.driver().unwrap().is_joined());
        assert!(pool.closed().unwrap().matches(&pool));
        stop.cancel();
        server.await.unwrap();
        dns.close().await;
    }).await.unwrap();
}
