//! Ordinary accepted Business consumes authentic taskless donor loans in Relay.
use super::*;
use zero_platform_tokio::contract::component::ResourceDeclaration;
#[tokio::test]
async fn ordinary_business_relays_keep_each_actual_original_until_views_caller_join_and_close() {
    tokio::time::timeout(Duration::from_secs(45), async {
        for protocols in [vec!["vless", "vmess"], vec!["vmess", "trojan"], vec!["trojan", "shadowsocks"], vec!["shadowsocks", "vless", "trojan"]] {
            let mut prefixes=vec![];
            for (index,protocol) in protocols.iter().enumerate() {
                let tag=format!("hop-{index}");
                let mut remote=if *protocol=="shadowsocks" {crate::runtime::MuxPrefix::shadowsocks("aes-128-gcm",&tag).await} else {crate::runtime::MuxPrefix::start(protocol,&tag).await};
                match &mut remote.node.protocol {
                    zero_config::OutboundProtocolConfig::Vless{mux_concurrency,xudp_concurrency,..}=>{*mux_concurrency=None;*xudp_concurrency=None;},
                    zero_config::OutboundProtocolConfig::Vmess{mux_concurrency,..}|zero_config::OutboundProtocolConfig::Trojan{mux_concurrency,..}=>*mux_concurrency=None,
                    _=>{},
                }
                prefixes.push(remote);
            }
            let target=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let target_address=target.local_addr().unwrap();
            let worker=tokio::spawn(async move {let(mut io,_)=target.accept().await.unwrap();let mut bytes=[0;4];io.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"wire");io.write_all(&bytes).await.unwrap();let mut tail=Vec::new();io.read_to_end(&mut tail).await.unwrap();assert!(tail.is_empty());});
            let nodes=prefixes.iter().map(|prefix|&prefix.node).collect::<Vec<_>>();let tags=(0..protocols.len()).map(|index|format!("hop-{index}")).collect::<Vec<_>>();
            let config=zero_config::RuntimeConfig::parse(&serde_json::json!({"runtime":{"native_stream_max_originals":1},"inbounds":[{"tag":"caller","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":nodes,"outbound_groups":[{"tag":"chain","type":"relay","proxies":tags}],"route":{"final":{"type":"route","outbound":"chain"}}}).to_string()).unwrap();
            let proxy=Proxy::new(config.clone()).unwrap();let registry=proxy.system_tun_source_registry();let running=proxy.spawn();
            ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone()).apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
            let source=tokio::time::timeout_at(deadline(),async {loop{if let Some(source)=registry.owner("caller"){source.wait_ready().await.unwrap();break source;}tokio::task::yield_now().await;}}).await.unwrap();
            let bound=*source.original_listener_address.get().unwrap();let mut client=tokio::net::TcpStream::connect(bound).await.unwrap();client.write_all(&[5,1,0]).await.unwrap();let mut auth=[0;2];client.read_exact(&mut auth).await.unwrap();assert_eq!(auth,[5,0]);
            let mut request=vec![5,1,0,1,127,0,0,1];request.extend_from_slice(&target_address.port().to_be_bytes());client.write_all(&request).await.unwrap();let mut reply=[0;10];tokio::time::timeout_at(deadline(),client.read_exact(&mut reply)).await.unwrap().unwrap();assert_eq!(&reply[..2],&[5,0]);
            client.write_all(b"wire").await.unwrap();let mut bytes=[0;4];tokio::time::timeout_at(deadline(),client.read_exact(&mut bytes)).await.unwrap().unwrap();assert_eq!(&bytes,b"wire");
            let services=proxy.tcp_runtime_services();let plan=services.components.as_ref().unwrap();let mut registrations=vec![];let mut originals=vec![];
            for (index,tag) in tags.iter().enumerate() {
                let slot=plan.configured(tag).unwrap();let ordinal=slot.recipe.spec().resources.iter().position(|resource|matches!(resource,ResourceDeclaration::ControlledNodeCarrier{..})).unwrap();
                let registration=plan.original_carrier_owner(slot.instance_id(),ordinal).expect("same ordinary registered original owner");assert_eq!(registration.limit().get(),1);
                let observed=registration.observe().unwrap();assert_eq!(observed.slots().len(),1,"one actual carrier at {tag}");let original=&observed.slots()[0];assert!(original.original().closed().is_none());assert!(original.driver().is_none());assert!(original.auxiliary().is_none());
                if index==0 {assert!(original.borrowed_origin().is_none());assert!(original.native().is_some());} else {let donor=original.borrowed_origin().expect("authentic shorter protocol donor");let prior=plan.configured(&tags[index-1]).unwrap();assert_eq!(donor.instance(),prior.instance_id());assert_eq!(donor.native_origin().instance(),plan.configured(&tags[0]).unwrap().instance_id());}
                originals.push(original.original().clone());registrations.push(registration);
            }
            assert_eq!(proxy.engine().active_sessions().len(),1);drop(client);worker.await.unwrap();
            tokio::time::timeout_at(deadline(),async {loop {if originals.iter().all(|original|original.closed().is_some_and(|closed|original.matches_closed(&closed))) && registrations.iter().all(|registration|registration.observe().is_some_and(|observation|observation.slots().is_empty())) {break;}tokio::task::yield_now().await;}}).await.expect("no next dial/shutdown is needed to settle the actual cancelled originals and caller");
            let completed=proxy.engine().completed_sessions();assert_eq!(completed.len(),1);let record=&completed[0];assert!(record.failure.is_none(),"{:?}",record.failure);assert_eq!(record.inbound_tag.as_deref(),Some("caller"));assert_eq!(record.outbound_tag.as_deref(),tags.last().map(String::as_str));assert_eq!(record.outbound_tx_bytes,4);assert_eq!(record.outbound_rx_bytes,4);assert_eq!(record.path.remote.as_ref().unwrap().port,prefixes[0].node.protocol.endpoint().unwrap().1);
            running.shutdown().await.unwrap();for registration in registrations {assert!(registration.closed().unwrap().matches(&registration));assert!(registration.users_closed().unwrap().matches(&registration));}
            let terminal=source.terminal_for_test().unwrap();assert!(terminal.failure().is_none());assert!(terminal.joined_tasks()>0);assert!(terminal.closed_io_receipts()>=2);
            for prefix in prefixes {prefix.close().await;}
        }
    }).await.unwrap();
}
