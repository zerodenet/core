//! A pool may borrow its own strictly shorter original under the same Host.
use super::*;
#[tokio::test]
async fn same_mux_owner_prepares_distinct_prefixes_and_closes_consumers_before_donors() {
    timeout(Duration::from_secs(30), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let mut candidate = config(protocol, server.fixture.front.port());
            candidate.runtime.native_mux_max_carriers = Some(2);
            candidate.outbound_groups.push(
                serde_json::from_value(json!({
                    "tag":"nested", "type":"relay", "proxies":["a","a"]
                }))
                .unwrap(),
            );
            let proxy = Proxy::new(candidate).unwrap();
            let running = start_components(&proxy).await;
            let owner = registration(&proxy);
            let mut stream = connect_target(&proxy, server.fixture.echo.port(), "nested")
                .await
                .unwrap();
            echo(&mut stream).await;
            let observed = owner.observe().unwrap();
            assert_eq!(
                observed.slots().len(),
                2,
                "one owning pool keeps both real physical lineages"
            );
            let originals = observed
                .slots()
                .iter()
                .map(|slot| slot.original().clone())
                .collect::<Vec<_>>();
            let drivers = observed
                .slots()
                .iter()
                .map(|slot| slot.driver().unwrap().1.clone())
                .collect::<Vec<_>>();
            drop(observed);
            drop(stream);
            proxy
                .protocols
                .shutdown_components(deadline())
                .await
                .unwrap();
            for original in originals {
                let closed = original
                    .closed()
                    .expect("original close is independent of driver join");
                assert!(original.matches_closed(&closed));
            }
            for driver in drivers {
                assert!(driver.matches_joined(&driver.try_joined().unwrap()));
            }
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            running.shutdown().await.unwrap();
            server.close().await;
        }
    })
    .await
    .unwrap();
}
