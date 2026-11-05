//! A failed record cannot reset this actual DNS caller to its first candidate.
use super::*;

#[tokio::test]
async fn domain_vmess_grpc_dns_retires_failed_originals_before_trying_each_actual_node_candidate() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let peer = Peer::new(false).await;
        // Keep both real refused endpoints bound without listening. Their
        // original sockets remain reserved throughout the candidate attempts.
        let refused = [[127, 0, 0, 2], [127, 0, 0, 3]].map(|ip| {
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.bind(std::net::SocketAddr::from((ip, peer.address.port()))).unwrap();
            socket
        });
        let mut wire = crate::dns_fixture::Dns::new().await;
        let mut node = crate::dns_fixture::Dns::new().await;
        let (proxy, running, _) = start(config(peer.address, false, &wire, Some(&node))).await;
        let mut query = query(&proxy, "ordered-candidates.grpc.dns.test");
        let bootstrap = tokio::select! {
            request = node.request() => request,
            result = &mut query => panic!("gRPC failed before actual Node candidate lookup: {result:?}"),
        };
        let node_caller = pending(&proxy).await;
        bootstrap.answer_ips(false, false, &[
            IpAddress::V4([127, 0, 0, 2]), IpAddress::V4([127, 0, 0, 3]), IpAddress::V4([127, 0, 0, 1]),
        ]);
        tokio::time::timeout_at(deadline(), node_caller.task.wait()).await.unwrap();
        let bootstrap_native = node_caller.original.as_ref().unwrap();
        let closed = bootstrap_native.closed().unwrap();
        assert!(closed.matches(bootstrap_native) && closed.had_native_acquisition());
        let request = tokio::select! {
            request = wire.request() => request,
            result = &mut query => panic!("gRPC did not advance from the failed original candidates: {result:?}"),
        };
        assert!(request.tcp);
        let caller = pending(&proxy).await;
        let (child, original) = originals(&caller);
        let services = proxy.tcp_runtime_services();
        let plan = services.components.as_ref().unwrap();
        let role = plan.configured("grpc").unwrap();
        let owner = role.recipe.spec().resources.iter().enumerate().filter_map(|(ordinal, _)| {
            plan.original_carrier_owner(role.instance_id(), ordinal)
        }).find(|owner| owner.observe().is_some_and(|held| held.retained() == 1)).expect("actual service issuer retains only the successful candidate");
        let held = owner.observe().unwrap();
        assert_eq!(held.retained(), 1, "closed failures cannot remain actual concurrent occupancy");
        let slot = &held.slots()[0];
        assert_eq!(slot.ordinal(), 3, "the same issuer reserved both failed candidates before this successful original");
        assert_eq!(original.endpoints().unwrap().peer(), peer.address);
        assert_eq!(peer.accepted(), 1);
        node.no_new_request();
        request.answer(false, false);
        query.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap();
        super::closed(&child, &original);
        assert_eq!(owner.observe().unwrap().retained(), 0);
        running.shutdown().await.unwrap();
        assert!(owner.closed().unwrap().matches(&owner));
        assert!(owner.users_closed().unwrap().matches(&owner));
        peer.close().await;
        wire.close().await;
        node.close().await;
        drop(refused);
    }).await.unwrap();
}
