//! Qualified donors retain real caller custody before an existing receiver.
use super::business_carrier::sources::{Caller, Kind};
use crate::adapters::socks5::relay_tests::fixtures;
use crate::{protocol_registry::OutboundAdapterContext, Proxy};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Instant},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{ResolvedOutbound, RouteDecision};
#[tokio::test]
async fn native_direct_and_socket_prefixes_preserve_actual_wire_halfclose_and_original_caller() {
    exercise(false).await;
}
#[tokio::test]
async fn native_prefix_rejection_keeps_actual_failure_without_replay_or_unowned_redial() {
    exercise(true).await;
}
async fn exercise(reject: bool) {
    timeout(Duration::from_secs(35), async {
        for direct in [false, true] {
            for count in [2,3] {
                for kind in Kind::all() {
                    let mut listeners=Vec::new();let mut addresses=Vec::new();
                    let mut nodes=Vec::new();let mut tags=Vec::new();
                    for index in 0..count {
                        let tag=if index+1==count {"last".to_owned()}else{format!("node-{index}")};
                        let protocol=if direct&&index==0 {serde_json::json!({"type":"direct"})}else{
                            let socket=TcpListener::bind("127.0.0.1:0").await.unwrap();let address=socket.local_addr().unwrap();
                            addresses.push(address);listeners.push(socket);
                            serde_json::json!({"type":"socks5","server":"127.0.0.1","port":address.port()})
                        };
                        nodes.push(serde_json::json!({"tag":tag,"protocol":protocol}));tags.push(tag);
                    }
                    let value=serde_json::json!({"outbounds":nodes,"outbound_groups":[{"tag":"chain","type":"relay","proxies":tags}],"route":{"final":{"type":"route","outbound":"chain"}}});
                    let proxy=Proxy::new(zero_config::RuntimeConfig::parse(&value.to_string()).unwrap()).unwrap();
                    let config=proxy.engine().config();let end=||Instant::now()+Duration::from_secs(5);
                    proxy.protocols.start_components(config.clone(),end()).await.unwrap();
                    proxy.protocols.commit_components(&config,||Ok(())).unwrap();
                    let mut caller=Caller::new_for_session(&proxy,kind,Session::new(0,Address::Ipv4([203,0,113,9]),443,Network::Tcp,ProtocolType::new("actual-prefix"))).await;
                    let session=caller.session().clone();let witness=caller.witness();
                    let services=proxy.tcp_runtime_services();let mut execution=services.execution();execution.invocation=Some(caller.invocation().clone());
                    let (resolved,_)=proxy.engine().resolve_route_decision(RouteDecision::Route("chain".into())).unwrap();
                    let ResolvedOutbound::Relay {chain}=resolved else {unreachable!()};
                    let prepared=proxy.protocols.prepare_tcp_relay(OutboundAdapterContext::new(services.config()),&chain,execution.components.as_deref().ok())
                        .unwrap_or_else(|failure|panic!("{}",failure.error));
                    assert!(prepared.0.standard_nodes().unwrap().is_some());
                    let mut peers=tokio::task::JoinSet::new();
                    for (index,listener) in listeners.into_iter().enumerate() {
                        let next=addresses.get(index+1).copied();
                        // Transfer each retained bound original into its real worker.
                        peers.spawn(async move {
                            let (mut io,_)=listener.accept().await.unwrap();
                            if let Some(next)=next {
                                assert_eq!(fixtures::request(&mut io).await,(Address::Domain("127.0.0.1".into()),next.port()));
                                let mut back=tokio::net::TcpStream::connect(next).await.unwrap();fixtures::accept(&mut io).await;
                                tokio::io::copy_bidirectional(&mut io,&mut back).await.unwrap();
                            }else if reject {
                                fixtures::greeting(&mut io).await;io.write_all(&[5,255]).await.unwrap();
                                let mut byte=[0];assert_eq!(io.read(&mut byte).await.unwrap(),0);
                            }else{
                                assert_eq!(fixtures::request(&mut io).await,(Address::Ipv4([203,0,113,9]),443));
                                fixtures::accept(&mut io).await;io.write_all(b"pre").await.unwrap();
                                let mut bytes=Vec::new();io.read_to_end(&mut bytes).await.unwrap();assert_eq!(&bytes,b"request");
                                io.write_all(b"tail").await.unwrap();io.shutdown().await.unwrap();
                            }
                        });
                    }
                    let result=crate::runtime::tcp_dispatch::relay::dispatch_tcp_relay(execution.clone(),&session,prepared.0,prepared.1,crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic).await;
                    if reject {
                        let error=result.err().expect("actual handshake rejection");let original=crate::transport::channel_failure(&error.error).unwrap();
                        assert_eq!(original.reason,zero_platform_tokio::contract::Reason::Protocol);
                        assert_eq!(original.component,Some(execution.components.as_ref().unwrap().configured("last").unwrap().id));
                        assert_eq!(original.session,Some(zero_core::component::SessionId(session.id)));
                        assert_eq!(original.delivery,zero_platform_tokio::contract::DeliveryState::Unknown);assert!(!original.retry_permitted());
                    }else{
                        let outbound=result.unwrap_or_else(|failure|panic!("{}: {}",failure.stage,failure.error));
                        let mut io=crate::transport::extract_tcp_stream(outbound).unwrap();
                        assert_eq!(io.relay_chain.len(),count);assert_eq!(io.outbound_tag,"last");
                        let mut prefix=[0;3];io.upstream.read_exact(&mut prefix).await.unwrap();assert_eq!(&prefix,b"pre");
                        assert!(!witness.is_joined());io.upstream.write_all(b"request").await.unwrap();io.upstream.shutdown().await.unwrap();
                        let mut tail=Vec::new();io.upstream.read_to_end(&mut tail).await.unwrap();assert_eq!(&tail,b"tail");drop(io);
                        for (index,tag) in tags.iter().enumerate() {
                            let remaining=(count-if direct {index.max(1)}else{index}) as u64;
                            let observed=proxy.engine().traffic_snapshot(&zero_api::TrafficGetQuery {scope:zero_api::TrafficScope::Outbound {tag:tag.clone()}}).unwrap();
                            let outer=&observed.planes[2].counters;
                            assert_eq!((outer.tx_bytes,outer.rx_bytes),(Some(remaining*13+(remaining-1)*6+7),Some(remaining*12+7)),"same original carrier accounting for {tag}");
                        }
                    }
                    while let Some(peer)=peers.join_next().await {peer.unwrap();}
                    caller.join().await;assert!(witness.is_joined());caller.verify_carrier_completion();
                    proxy.engine().track_session(session.id).finish(zero_engine::SessionOutcome::Cancelled).unwrap();
                    proxy.protocols.shutdown_components(end()).await.unwrap();
                    assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
                }
            }
        }
    }).await.unwrap();
}
