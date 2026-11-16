//! The unaffected half of a plugin uses its actual standard native provider.
use super::*;
use crate::{Proxy, ProxyHandle};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use zero_platform_tokio::contract::component::RoleKind;

async fn start(value: serde_json::Value) -> (Proxy, crate::RunningProxy, std::net::SocketAddr) {
    let config = zero_config::RuntimeConfig::parse(&value.to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
    .apply_runtime_config_and_wait(config, Duration::from_secs(5))
    .await
    .unwrap();
    let owner = proxy.inbound_owners.owner("entry").unwrap();
    owner.wait_ready().await.unwrap();
    let address = *owner.original_listener_address.get().unwrap();
    (proxy, running, address)
}
#[tokio::test]
async fn udp_only_plugin_keeps_real_standard_tcp_without_starting_a_process() {
    tokio::time::timeout(Duration::from_secs(30),async {
        for (cipher,password) in [("chacha20-ietf-poly1305","role-wire"),("2022-blake3-aes-128-gcm","MDEyMzQ1Njc4OWFiY2RlZg==")] {
            let (remote,remote_running,wire)=start(serde_json::json!({
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"shadowsocks","cipher":cipher,"password":password}}],
                "route":{"final":{"type":"direct"}}
            })).await;
            let (proxy,running,entry)=start(serde_json::json!({
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
                "outbounds":[{"tag":"node","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":wire.port(),"cipher":cipher,"password":password,
                    "plugin":{"command":"zero-udp-plugin-must-not-run-for-tcp","mode":"udp_only"}}}],
                "route":{"final":{"type":"route","outbound":"node"}}
            })).await;
            let plan=proxy.tcp_runtime_services().components.clone().unwrap();let slot=plan.configured("node").unwrap();
            assert!(slot.recipe.spec().roles.contains(&RoleKind::Stream));
            assert!(!slot.recipe.spec().roles.contains(&RoleKind::Datagram),"deferred Process ownership must not fabricate UDP");
            let config=proxy.engine().config();
            let claim=proxy.protocols.claim_outbound_leaf(config.as_ref(),zero_engine::ResolvedLeafOutbound::Proxy {
                identity:zero_engine::OutboundIdentity::from_config_index(0),
            }).unwrap();
            assert!(!claim.has_tcp_capability());assert!(claim.has_udp_flow_capability());
            let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let target=listener.local_addr().unwrap();
            let peer=tokio::spawn(async move {let(mut io,_)=listener.accept().await.unwrap();let mut bytes=[0;4];io.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"wire");io.write_all(&bytes).await.unwrap();io.shutdown().await.unwrap();});
            let mut client=TcpStream::connect(entry).await.unwrap();client.write_all(&[5,1,0]).await.unwrap();let mut auth=[0;2];client.read_exact(&mut auth).await.unwrap();assert_eq!(auth,[5,0]);
            let mut request=vec![5,1,0,1,127,0,0,1];request.extend_from_slice(&target.port().to_be_bytes());client.write_all(&request).await.unwrap();let mut response=[0;10];client.read_exact(&mut response).await.unwrap();assert_eq!(&response[..2],&[5,0]);
            client.write_all(b"wire").await.unwrap();let mut bytes=[0;4];client.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"wire");
            let owner=slot.original_carrier_owner(0).expect("same standard Stream original provider");
            let observed=owner.observe().unwrap();assert_eq!(observed.slots().len(),1);let original=observed.slots()[0].original().clone();
            client.shutdown().await.unwrap();let mut tail=Vec::new();client.read_to_end(&mut tail).await.unwrap();drop(client);peer.await.unwrap();
            let source=proxy.inbound_owners.owner("entry").unwrap();let inbound=remote.inbound_owners.owner("entry").unwrap();
            running.shutdown().await.unwrap();remote_running.shutdown().await.unwrap();
            assert!(original.matches_closed(&original.closed().unwrap()));assert!(owner.closed().unwrap().matches(&owner));assert!(owner.users_closed().unwrap().matches(&owner));
            for source in [source,inbound] {let closed=source.terminal_for_test().unwrap();assert!(closed.failure().is_none()&&closed.task_failures().is_empty());assert!(closed.joined_tasks()>0 && closed.closed_io_receipts()>0);}
        }
    }).await.unwrap();
}
