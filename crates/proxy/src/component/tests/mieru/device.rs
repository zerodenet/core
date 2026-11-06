//! The real endpoint-free first WG and one Mieru pool serve ordinary TCP/UDP.
mod peer;
use super::*;
use crate::runtime::{pipe::UdpPipeInput, udp_ingress::UdpIngressRuntime};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use zero_core::{
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address, ProtocolType,
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity},
        network::{UpstreamBinding, UpstreamTransport},
        stream::ConnectRequest,
        CancellationToken,
    },
    controlled_carrier::loan::StreamLoan,
};

async fn open(proxy: &Proxy) -> StreamLoan {
    let services = proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let wg = plan.configured("wg").unwrap();
    let pool = plan.configured("pool").unwrap();
    assert!(plan.has_device_carrier_provider(wg.role_id()));
    let context = OpenContext {
        identity: RequestIdentity::Service(std::sync::Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::BackgroundControl,
        })),
        deadline: Some(deadline()),
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![wg.role_id()],
                dial: pool.dial.clone(),
                network_generation: zero_core::component::NetworkGeneration(
                    services.upstream().network_generation(),
                ),
            },
            std::sync::Arc::new(Denied),
        ),
    };
    pool.stream(deadline())
        .await
        .unwrap()
        .connect_carrier(
            ConnectRequest {
                destination: destination(),
            },
            context,
        )
        .await
        .unwrap()
        .io
}
struct Denied;
impl zero_platform_tokio::contract::network::UpstreamOpener for Denied {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<
            zero_platform_tokio::contract::stream::BoxStream,
            zero_platform_tokio::contract::ChannelError,
        >,
    > {
        Box::pin(async { panic!("the real Mieru owner must rebuild its own WG prefix") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: zero_platform_tokio::contract::network::CarrierContext,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<
            zero_platform_tokio::contract::datagram::BoxDatagramSession,
            zero_platform_tokio::contract::ChannelError,
        >,
    > {
        Box::pin(async { panic!("the real Mieru owner must rebuild its own WG prefix") })
    }
}
async fn echo(loan: &mut StreamLoan) {
    loan.io.write_all(b"same-pool").await.unwrap();
    let mut response = [0; 9];
    loan.io.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"same-pool");
}
async fn returned(loan: StreamLoan) {
    let StreamLoan { io, mut owner } = loan;
    drop(io);
    assert!(owner.close(deadline()).await.unwrap().is_complete());
    assert!(owner.closed().unwrap().matches(&owner));
}
#[tokio::test]
async fn ordinary_root_device_prefix_uses_real_setup_caller_and_one_mieru_pool_for_both_underlays()
{
    tokio::time::timeout(Duration::from_secs(35),async {
        for udp in [false,true] {
            let server=if udp {Server::packet(false).await} else {Server::new(false).await};
            let peer=peer::Peer::new(server.address).await;
            let mut config=zero_config::RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"mieru_max_carriers":1},
                "endpoints":[{"tag":"wg","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"peers":[{"public_key":peer::public(122),"endpoint":peer.address.to_string(),"allowed_ips":["0.0.0.0/0"]}]}}],
                "outbounds":[{"tag":"pool","protocol":{"type":"mieru","server":"127.0.0.1","port":server.address.port(),"username":"u","password":"p"}}],
                "outbound_groups":[{"tag":"chain","type":"relay","proxies":["wg","pool"]}],
                "route":{"final":{"type":"route","outbound":"chain"}}
            }).to_string()).unwrap();
            if udp {let zero_config::OutboundProtocolConfig::Mieru {transport,..}=&mut config.outbounds.iter_mut().find(|node|node.tag=="pool").unwrap().protocol else {unreachable!()};*transport=zero_config::MieruTransport::Udp;}
            let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
            ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone()).apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
            let mut first=open(&proxy).await;echo(&mut first).await;
            let original=first.owner.original().clone();
            assert_eq!(original.owner().limit().get(),1);
            assert!(original.parent().is_some(),"the actual WG loan must remain in the physical lineage");
            let driver=original.driver().unwrap().clone();
            let mut second=open(&proxy).await;echo(&mut second).await;assert!(second.owner.original().same_original(&original));
            returned(first).await;assert!(!driver.is_joined());echo(&mut second).await;
            let mut dispatch=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("incoming").await.unwrap();
            let id=dispatch.dispatch(UdpPipeInput {peer_identity:None,target:Address::Ipv4([203,0,113,1]),port:443,payload:b"packet",route_target:None,sniffed_original_target:None,sniffed_host_source:None,skip_fake_ip_restore:false,protocol:ProtocolType::new("test"),auth:None,source_addr:None,client_session_id:None,transparent_target:false,transparent_original_target:None,transparent_host_source:None}).await.unwrap();
            let (_,tasks,_)=dispatch.poll_sockets();let packet=tasks.join_next_with_id().await.unwrap().unwrap().unwrap();
            assert_eq!(packet.0,Address::Ipv4([203,0,113,1]));assert_eq!(packet.1,443);assert_eq!(packet.2,b"packet");
            crate::runtime::udp_delivery::record_chain_udp_response_parts(&proxy.udp_runtime_services(),&dispatch,packet.0,packet.1,packet.2,packet.3);
            let session=proxy.engine().active_sessions().into_iter().find(|session|session.id==id).unwrap();
            assert_eq!(session.outbound_tx_bytes,6);assert_eq!(session.outbound_rx_bytes,6);
            assert_eq!(server.accepts.load(Ordering::SeqCst),1,"ordinary TCP and UDP must share the same physical pool");
            let retired=proxy.tcp_runtime_services().execution().retired_datagrams();dispatch.finish_all();assert!(retired.wait_closed(deadline()).await.unwrap().is_complete());
            echo(&mut second).await;returned(second).await;
            running.shutdown().await.unwrap();let joined=driver.wait_registration_released().await;assert!(driver.matches_joined(&joined)&&joined.registration_released());
            assert!(original.closed().unwrap().matches(&original));assert!(original.owner().closed().unwrap().matches(original.owner()));
            peer.close().await;server.close().await;
        }
    }).await.unwrap();
}
