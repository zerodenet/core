//! Actual SOCKS Source -> same configured SS Datagram -> original cached wire.
use crate::Proxy;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::timeout,
};
use zero_core::Address;
use zero_platform_tokio::controlled_carrier::{CarrierOriginObservation, CarrierOwnerRegistration};

async fn associate(endpoint: SocketAddr) -> (TcpStream, UdpSocket, SocketAddr) {
    let mut control = TcpStream::connect(endpoint).await.unwrap();
    control.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    control.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let relay = SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([reply[8], reply[9]])));
    (
        control,
        UdpSocket::bind("127.0.0.1:0").await.unwrap(),
        relay,
    )
}
fn original(
    proxy: &Proxy,
) -> (
    CarrierOwnerRegistration,
    CarrierOriginObservation,
    zero_platform_tokio::contract::scope::TaskJoinWitness,
) {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let mut pending = vec![plan.configured("ss").unwrap()];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(slot) = pending.pop() {
        if !seen.insert(slot.instance_id()) {
            continue;
        }
        for ordinal in 0..slot.recipe.spec().resources.len() {
            let Some(owner) = slot.original_carrier_owner(ordinal) else {
                continue;
            };
            let Some(observation) = owner.observe() else {
                continue;
            };
            for native in observation.slots() {
                if native.native().is_some_and(|native| !native.is_closed()) {
                    let driver = native
                        .driver()
                        .expect("same actual SS cached receive driver")
                        .1
                        .clone();
                    return (owner, native.original().clone(), driver);
                }
            }
        }
        pending.extend(
            slot.dependency_instances()
                .iter()
                .filter_map(|id| plan.component(id.component)),
        );
    }
    panic!("ordinary authenticated wire must have one actual original and driver")
}
async fn send(socket: &UdpSocket, relay: SocketAddr, target: u8, payload: &[u8]) {
    let codec = socks5::udp::Socks5InboundUdpCodec;
    let wire = codec
        .encode_response_to_client(&Address::Ipv4([203, 0, 113, target]), 53, payload)
        .unwrap();
    socket.send_to(&wire, relay).await.unwrap();
}
async fn receive(socket: &UdpSocket, target: u8, payload: &[u8]) {
    let mut bytes = [0; 2048];
    let (size, _) = timeout(Duration::from_secs(4), socket.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let codec = socks5::udp::Socks5InboundUdpCodec;
    let (address, port, bytes) = codec.decode_response(&bytes[..size]).unwrap().into_parts();
    assert_eq!(address, Address::Ipv4([203, 0, 113, target]));
    assert_eq!(port, 53);
    assert_eq!(bytes.as_slice(), payload);
}
#[tokio::test]
async fn ordinary_cached_datagrams_keep_one_original_for_reordered_targets_empty_payload_and_reuse()
{
    timeout(Duration::from_secs(25),async {
        let peer=UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"ss_udp_max_pending_responses":2,"ss_udp_total_max_pending_responses":2},
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"ss","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":peer.local_addr().unwrap().port(),"cipher":"aes-128-gcm","password":"ordinary-original"}}],
            "route":{"final":{"type":"route","outbound":"ss"}}
        }).to_string()).unwrap();
        let codec=super::super::super::transport_leaf(&Default::default(),"ss",&config.outbounds[0].protocol).unwrap().udp_packet_path_plan().unwrap().carrier_codec();
        let proxy=Proxy::new(config).unwrap();let registry=proxy.system_tun_source_registry();let running=proxy.spawn();
        let entry=loop {if let Some(owner)=registry.owner("entry") {break owner;}tokio::task::yield_now().await;};
        entry.wait_ready().await.unwrap();let endpoint=*entry.original_listener_address.get().unwrap();
        let (_control,socket,relay)=associate(endpoint).await;
        send(&socket,relay,1,b"first").await;send(&socket,relay,2,b"").await;
        let mut buffer=[0;65536];let mut sender=None;let mut requests=std::collections::BTreeMap::new();
        for _ in 0..2 {
            let (size,source)=timeout(Duration::from_secs(4),peer.recv_from(&mut buffer)).await.unwrap().unwrap();
            if let Some(previous)=sender {assert_eq!(previous,source);}sender=Some(source);
            let (address,port,payload)=codec.decode(&buffer[..size]).unwrap();assert_eq!(port,53);
            let Address::Ipv4([203,0,113,target])=address else {panic!("actual SS target differs: {address:?}")};
            requests.insert(target,payload);
        }
        assert_eq!(requests.remove(&1).unwrap(),b"first");assert_eq!(requests.remove(&2).unwrap(),b"");
        let (owner,native,driver)=original(&proxy);assert!(!driver.is_joined());
        for (target,payload) in [(2,&b""[..]),(1,&b"reply"[..])] {
            let wire=codec.encode(&Address::Ipv4([203,0,113,target]),53,payload).unwrap();peer.send_to(&wire,sender.unwrap()).await.unwrap();receive(&socket,target,payload).await;
        }
        send(&socket,relay,1,b"again").await;
        let (size,source)=timeout(Duration::from_secs(4),peer.recv_from(&mut buffer)).await.unwrap().unwrap();assert_eq!(Some(source),sender);
        let (address,port,payload)=codec.decode(&buffer[..size]).unwrap();assert_eq!(payload,b"again");
        let wire=codec.encode(&address,port,b"same").unwrap();peer.send_to(&wire,source).await.unwrap();receive(&socket,1,b"same").await;
        let (same_owner,same_native,same_driver)=original(&proxy);assert!(owner.same_owner(&same_owner));assert!(native.same_original(&same_native));assert!(driver.same_registration(&same_driver));
        drop(socket);drop(_control);running.shutdown().await.unwrap();
        let closed=native.closed().expect("matching native close after actual cache driver cleanup");assert!(native.matches_closed(&closed));assert!(closed.had_native_acquisition());
        assert!(owner.closed().unwrap().matches(&owner));assert!(owner.users_closed().unwrap().matches(&owner));
        let joined=driver.try_joined().expect("actual original receive driver joined");assert!(driver.matches_joined(&joined));assert!(joined.registration_released());
    }).await.unwrap();
}
