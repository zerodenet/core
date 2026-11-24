//! Browser-labelled later hops keep the actual native prefix, including donors.
use super::*;
#[tokio::test]
async fn browser_prefix_preserves_standard_streams_udp_mux_and_original_retirement() {
    let _serial = SERIAL
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    tokio::time::timeout(Duration::from_secs(90), async {
        let first = fixture::start(serde_json::json!({"inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}), "first").await;
        let last = fixture::start(serde_json::json!({"inbounds":[{"tag":"last","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}), "last").await;
        let remote = fixture::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vmess","users":[{"id":ID}],"ws":{"path":"/original"}}}],"route":{"final":{"type":"direct"}}}), "remote").await;
        let ss = fixture::start(serde_json::json!({"inbounds":[{"tag":"ss","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"prefix-process"}}],"route":{"final":{"type":"direct"}}}),"ss").await;
        let helper=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocols/shadowsocks/tests/support/sip003.py");
        let directory=tempfile::tempdir().unwrap();
        for plugin in [false,true] {
        for mux in [false, true] {
            for donor in [false, true] {
                let mut protocol = serde_json::json!({"type":"vmess","server":"127.0.0.1","port":remote.address.port(),"id":ID,"ws":{"path":"/original","browser_dialer":{"listen":BROWSER,"idle_capacity":1}}});
                if mux { protocol["mux_concurrency"] = serde_json::json!(4); }
                let chain = if donor { vec!["first", "node", "last"] } else { vec!["first", "node"] };
                let first_protocol=if plugin {
                    serde_json::json!({"type":"shadowsocks","server":"127.0.0.1","port":ss.address.port(),"cipher":"aes-128-gcm","password":"prefix-process","plugin":{"command":"python3","mode":"tcp_only","options":"test=options;escaped=value","args":[helper,"client","tcp_only",directory.path().join("child.json")]}})
                } else {serde_json::json!({"type":"socks5","server":"127.0.0.1","port":first.address.port()})};
                let client = fixture::start_owned(serde_json::json!({"runtime":{"native_stream_max_originals":4,"native_mux_max_carriers":1,"process_max_jobs":1,"process_max_jobs_per_instance":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"first","protocol":first_protocol},{"tag":"node","protocol":protocol},{"tag":"last","protocol":{"type":"socks5","server":"127.0.0.1","port":last.address.port()}}],"outbound_groups":[{"tag":"chain","type":"relay","proxies":chain}],"route":{"final":{"type":"route","outbound":"chain"}}}), "entry").await;
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();
                let echo = tokio::spawn(async move {
                    let (mut io, _) = listener.accept().await.unwrap();
                    let mut bytes = [0; 13];
                    io.read_exact(&mut bytes).await.unwrap();
                    io.write_all(&bytes).await.unwrap();
                    let mut eof = [0]; assert_eq!(io.read(&mut eof).await.unwrap(), 0);
                    // The original receive half remains usable after upload EOF.
                    io.write_all(b"after-eof").await.unwrap();
                    io.shutdown().await.unwrap();
                });
                // No Browser agent exists. A false root Browser selection hangs
                // rather than reaching this actual echo behind both proxies.
                let mut io = super::mux::connect(client.address, port).await;
                io.write_all(b"actual-prefix").await.unwrap();
                let mut bytes = [0; 13];io.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"actual-prefix");
                let plan = client.proxy.tcp_runtime_services().components.clone().unwrap();
                let node = plan.configured("node").unwrap();
                let first_slot=plan.configured("first").unwrap();
                let first_owners=(0..first_slot.recipe.spec().resources.len()).filter_map(|ordinal|first_slot.original_carrier_owner(ordinal)).collect::<Vec<_>>();
                let mut first_originals=first_owners.iter().flat_map(|owner|owner.observe().unwrap().slots().iter().map(|slot|slot.original().clone()).collect::<Vec<_>>()).collect::<Vec<_>>();
                assert!(!first_originals.is_empty());
                assert_eq!(client.proxy.protocols.runtime_host().original_budgets().process.snapshot().charged_jobs,usize::from(plugin));

                let mut retained = Vec::new();
                if mux {
                    assert!(plan.stream_dependencies_supported(node.role_id()));
                    let pool = plan.component(node.dependency_instances()[0].component).unwrap();
                    let owner = pool.original_carrier_owner(0).unwrap();
                    let snapshot = owner.observe().unwrap();assert_eq!(snapshot.retained(),1);
                    let slot = &snapshot.slots()[0];
                    assert!(slot.native().is_none());
                    assert_eq!(slot.borrowed_origin().unwrap().instance(), plan.configured("first").unwrap().instance_id());
                    let driver = slot.driver().unwrap().1.clone();
                    retained.push((owner,slot.original().clone(),Some(driver)));
                    let raw = plan.component(pool.dependency_instances()[0].component).unwrap();
                    assert_eq!(raw.original_carrier_owner(0).unwrap().observe().unwrap().retained(),0,"the later hop never acquires a root Browser original");
                }
                if !donor {
                    datagram_exchange_observed(&client, || {
                        if !mux {
                            let udp_originals = first_owners.iter().flat_map(|owner| {
                                owner.observe().unwrap().slots().iter()
                                    .map(|slot| slot.original().clone()).collect::<Vec<_>>()
                            }).filter(|slot| !first_originals.iter().any(|tcp| tcp.same_original(slot)))
                                .collect::<Vec<_>>();
                            assert!(!udp_originals.is_empty(), "UDP borrowed its own actual prefix Stream");
                            first_originals.extend(udp_originals);
                        }
                    }).await;
                }
                io.shutdown().await.unwrap();let mut tail=Vec::new();io.read_to_end(&mut tail).await.unwrap();assert_eq!(tail,b"after-eof");drop(io);echo.await.unwrap();
                let budgets=client.proxy.protocols.runtime_host().original_budgets();client.close().await;
                for (owner,original,driver) in retained {
                    assert!(original.matches_closed(&original.closed().unwrap()));
                    assert!(owner.closed().unwrap().matches(&owner));
                    if let Some(driver)=driver {assert!(driver.matches_joined(&driver.try_joined().unwrap()));}
                }
                assert_eq!(budgets.carrier.snapshot().charged,0);
                assert_eq!(budgets.process.snapshot().charged_jobs,0);
                for original in first_originals {assert!(original.matches_closed(&original.closed().unwrap()));}
                for owner in first_owners {assert!(owner.closed().unwrap().matches(&owner));}

            }
        }
        }
        ss.close().await; remote.close().await; last.close().await; first.close().await;
    }).await.unwrap();
}
