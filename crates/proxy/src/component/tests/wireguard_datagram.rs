//! Ordinary SOCKS flow -> one canonical Packet-backed WG Datagram provider.
#![cfg(all(
    feature = "wireguard",
    feature = "socks5",
    feature = "managed-stream-runtime"
))]
#[path = "../../../tests/wireguard_traffic/peer.rs"]
mod peer;
use crate::{
    inventory::{DatagramAssociationTarget, PreparedUdpLeafCandidate, PreparedUdpOutbound},
    protocol_registry::UdpAdapterContext,
    Proxy, ProxyHandle,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{RouteDecision, RouteMode};

#[tokio::test]
async fn ordinary_device_datagrams_keep_absent_endpoint_and_actual_source_across_empty_payloads() {
    timeout(Duration::from_secs(20), async {
        let (remote, peer_task) = peer::echo_peer().await;
        let reserved=std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port=reserved.local_addr().unwrap().port();drop(reserved);
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"socks5"}}],
            "endpoints":[{"tag":"wg","directions":{"inbound":true,"outbound":true},"listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"peers":[{"public_key":peer::public(122),"endpoint":remote.to_string(),"allowed_ips":["0.0.0.0/0"]}]}}],
            "route":{"rules":[],"final":{"type":"route","outbound":"wg"}}
        }).to_string()).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();
        assert!(DatagramAssociationTarget::resolve_endpoint(&proxy.tcp_runtime_services().execution(),"wg",None).is_err(),"cold role metadata cannot supply the native source grant");
        let running=proxy.spawn();
        let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        let execution=proxy.tcp_runtime_services().execution();
        let target=DatagramAssociationTarget::resolve_endpoint(&execution,"wg",None).unwrap();
        assert_eq!(target.tag(),"wg");assert_eq!(target.endpoint(),None);
        let slot=target.current_slot(&execution).unwrap();
        assert!(execution.components.as_ref().unwrap().has_device_carrier_provider(slot.role_id()));
        let (resolved,_plan)=proxy.engine().resolve_route_decision(RouteDecision::Route("wg".into())).unwrap();
        let session=Session::new(7,Address::Ipv4([198,51,100,10]),80,Network::Udp,ProtocolType::new("test"));
        let snapshot=proxy.engine().runtime_snapshot();
        let prepared=proxy.protocols.prepare_udp_outbound(UdpAdapterContext::new(snapshot.config(),proxy.udp_runtime_services()),&session,&resolved,b"probe",RouteMode::Flow).unwrap_or_else(|failure|panic!("{}",failure.error));
        let PreparedUdpOutbound::Single(PreparedUdpLeafCandidate::DatagramFlow(selected))=prepared else {panic!("actual canonical device must dispatch its standard Datagram role")};
        assert_eq!(selected.endpoint(),None);
        let mut control=TcpStream::connect(("127.0.0.1",port)).await.unwrap();
        control.write_all(&[5,1,0]).await.unwrap();
        let mut auth=[0;2];control.read_exact(&mut auth).await.unwrap();assert_eq!(auth,[5,0]);
        control.write_all(&[5,3,0,1,0,0,0,0,0,0]).await.unwrap();
        let mut reply=[0;10];control.read_exact(&mut reply).await.unwrap();assert_eq!(reply[1],0);
        let relay=u16::from_be_bytes([reply[8],reply[9]]);
        let client=UdpSocket::bind("127.0.0.1:0").await.unwrap();let mut buffer=[0;2048];
        let codec=socks5::udp::Socks5InboundUdpCodec;
        for payload in [&b""[..],&b"wire"[..],&b"again"[..]] {
            let packet=codec.encode_response_to_client(&Address::Ipv4([198,51,100,10]),80,payload).unwrap();
            client.send_to(&packet,("127.0.0.1",relay)).await.unwrap();
            let (size,_)=timeout(Duration::from_secs(5),client.recv_from(&mut buffer)).await.unwrap().unwrap();
            let (address,port,bytes)=codec.decode_response(&buffer[..size]).unwrap().into_parts();
            assert_eq!(address,Address::Ipv4([198,51,100,10]));assert_eq!(port,80);assert_eq!(bytes.as_ref(),payload);
        }
        assert_eq!(target.current_slot(&proxy.tcp_runtime_services().execution()).unwrap().instance_id(),slot.instance_id());
        let traffic=proxy.engine().traffic_snapshot(&zero_api::TrafficGetQuery {scope:zero_api::TrafficScope::Outbound {tag:"wg".into()}}).unwrap();
        assert_eq!(traffic.planes[0].counters.tx_bytes,Some(9));assert_eq!(traffic.planes[0].counters.rx_bytes,Some(9));
        drop(control);drop(client);
        running.shutdown().await.unwrap();
        assert!(target.current_slot(&proxy.tcp_runtime_services().execution()).is_err(),"shutdown withdraws the same actual source rather than leaving a live metadata provider");
        peer_task.abort();let _=peer_task.await;
    }).await.unwrap();
}
