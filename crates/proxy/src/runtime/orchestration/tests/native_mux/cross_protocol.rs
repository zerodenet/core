//! Whole standard relay execution over the actual protocol pools and originals.
use super::*;

#[tokio::test]
async fn nested_cross_protocol_pools_keep_one_owner_despite_unselected_compatibility_aliases() {
    timeout(Duration::from_secs(60), async {
        for protocols in [["vless", "vmess", "trojan"], ["trojan", "vless", "vmess"]] {
            let mut servers = Vec::new();
            let mut nodes = Vec::new();
            for (index, protocol) in protocols.iter().enumerate() {
                let server = Server::start(protocol).await;
                let mut node = config(protocol, server.fixture.front.port()).outbounds.remove(0);
                node.tag = format!("n{index}");
                nodes.push(node);
                servers.push(server);
            }
            let mut candidate = config("vless", 443);
            candidate.outbounds = nodes;
            candidate.outbounds.push(serde_json::from_value(json!({
                "tag":"plain", "protocol":{"type":"direct"}
            })).unwrap());
            candidate.outbound_groups = serde_json::from_value(json!([
                {"tag":"prefix", "type":"relay", "proxies":["n0", "n1"]},
                {"tag":"whole", "type":"relay", "proxies":["prefix", "n2"]},
                {"tag":"unselected", "type":"relay", "proxies":["n1", "plain"]},
                {"tag":"choice", "type":"selector", "outbounds":["whole", "unselected"], "selected":"whole"}
            ])).unwrap();
            candidate.route.final_action = zero_config::RouteActionConfig::Route { outbound: "choice".into() };
            let proxy = Proxy::new(candidate).unwrap();
            let running = start_components(&proxy).await;
            let plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let owners = (0..3).map(|index| {
                let slot = plan.configured(&format!("n{index}")).unwrap();
                assert_eq!(slot.dependency_instances().len(), 1);
                plan.original_carrier_owner(slot.dependency_instances()[0], 0).unwrap()
            }).collect::<Vec<_>>();
            let mut stream = connect_target(&proxy, servers[2].fixture.echo.port(), "choice").await.unwrap();
            echo(&mut stream).await;
            let mut originals = Vec::new();
            let mut drivers = Vec::new();
            for (index, owner) in owners.iter().enumerate() {
                let observed = owner.observe().unwrap();
                assert_eq!(observed.slots().len(), 1, "same actual {index} pool");
                originals.push(observed.slots()[0].original().clone());
                drivers.push(observed.slots()[0].driver().unwrap().1.clone());
                assert_eq!(servers[index].fixture.connections(), 1);
                assert!(proxy.protocols.claim_outbound_leaf(
                    proxy.engine().config().as_ref(),
                    zero_engine::ResolvedLeafOutbound::Proxy {
                        identity: zero_engine::OutboundIdentity::from_config_index(index),
                    },
                ).is_err());
            }
            drop(stream);
            // Reuse takes the same three physical lineages, not old compatibility pools.
            let mut reused = connect_target(&proxy, servers[2].fixture.echo.port(), "choice").await.unwrap();
            echo(&mut reused).await;
            for server in &servers { assert_eq!(server.fixture.connections(), 1); }
            drop(reused);
            proxy.protocols.shutdown_components(deadline()).await.unwrap();
            for original in originals { assert!(original.matches_closed(&original.closed().unwrap())); }
            for driver in drivers { assert!(driver.matches_joined(&driver.try_joined().unwrap())); }
            for owner in owners {
                assert!(owner.closed().unwrap().matches(&owner));
                assert!(owner.users_closed().unwrap().matches(&owner));
            }
            running.shutdown().await.unwrap();
            for server in servers { server.close().await; }
        }
    }).await.unwrap();
}
