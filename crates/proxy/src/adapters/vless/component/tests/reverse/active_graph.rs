//! Active Rvs ingress and virtual return roles share real standard providers.
use super::*;

#[tokio::test]
async fn active_ingress_keeps_real_tcp_udp_when_an_unrelated_business_alias_cannot_lend() {
    timeout(Duration::from_secs(60), async {
        for carrier in [
            serde_json::json!({"mux_concurrency":2,"xudp_concurrency":2}),
            serde_json::json!({"grpc":{"service_names":["active/Tun"]}}),
            serde_json::json!({"testpre":1,"grpc":{"service_names":["active/Tun"]}}),
        ] {
            let incoming = if carrier.get("grpc").is_some() {
                serde_json::json!({"grpc":{"service_names":["active/Tun"]}})
            } else {
                serde_json::json!({})
            };
            let mut outgoing = carrier;
            if outgoing.get("grpc").is_some() {
                // The real H2 original owns this mask, before TLS/H2 framing.
                outgoing["final_mask"] = serde_json::json!({"tcp":[{
                    "type":"fragment", "packets":{"minimum":1,"maximum":1},
                    "length":{"minimum":2,"maximum":2}
                }]});
            }
            let (portal, running, socks, bridge, bridge_running) =
                fixture::pair_with_bridge_graph(incoming, outgoing, None, true).await;
            let plan = bridge.tcp_runtime_services().components.clone().unwrap();
            let wire = plan
                .configured("wire")
                .expect("actual canonical Stream producer");
            assert!(plan.has_stream_loan_provider(wire.role_id()));
            assert!(wire.stream(end()).await.is_ok());
            assert!(
                bridge
                    .protocols
                    .claim_outbound_leaf(
                        bridge.engine().config().as_ref(),
                        zero_engine::ResolvedLeafOutbound::Proxy {
                            identity: zero_engine::OutboundIdentity::from_config_index(0),
                        },
                    )
                    .is_err(),
                "active ingress cannot recreate a private transport projection"
            );
            let remote = portal.inbound_owners.owner("entry").unwrap();
            let provider = portal
                .tcp_runtime_services()
                .components
                .clone()
                .unwrap()
                .configured("portal")
                .unwrap()
                .accepted_carrier_provider()
                .unwrap();
            let originals = provider.originals();
            framed::exercise(socks).await;
            empty_and_multiple_targets(socks).await;
            running.shutdown().await.unwrap();
            bridge_running.shutdown().await.unwrap();
            assert!(provider.returned().is_some());
            for original in originals {
                assert!(original.closed().unwrap().matches(&original));
            }
            let closed = remote.terminal_for_test().unwrap();
            assert!(closed.failure().is_none() && closed.task_failures().is_empty());
            assert!(closed.closed_io_receipts() > 0);
        }
    })
    .await
    .unwrap();
}
