//! Actual accepted SOCKS Source -> HY2 original -> Host-paid nested SS codecs.
use super::*;
use tokio::net::{TcpStream, UdpSocket};

#[tokio::test]
async fn real_packet_source_preserves_nested_codecs_empty_multitarget_packets_and_original_hy2_provider(
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let codec_node: zero_config::OutboundConfig = serde_json::from_value(serde_json::json!({
            "tag":"codec","protocol":{"type":"shadowsocks","server":"203.0.113.254","port":9876,"cipher":"chacha20-ietf-poly1305","password":"packet-codec"}
        })).unwrap();
        let codec = crate::adapters::shadowsocks::transport_leaf(&Default::default(), "codec", &codec_node.protocol)
            .unwrap().udp_packet_path_plan().unwrap().carrier_codec();
        let middle: zero_config::OutboundConfig = serde_json::from_value(serde_json::json!({
            "tag":"middle","protocol":{"type":"shadowsocks","server":"192.0.2.254","port":8765,"cipher":"chacha20-ietf-poly1305","password":"middle-codec"}
        })).unwrap();
        let first = crate::adapters::shadowsocks::transport_leaf(&Default::default(), "middle", &middle.protocol)
            .unwrap().udp_packet_path_plan().unwrap().carrier_codec();
        let server = Server::with_codecs(vec![first, codec]).await;
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[
                {"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1","port":server.port,"password":"p","insecure":true}},
                middle,
                {"tag":"codec","protocol":{"type":"shadowsocks","server":"203.0.113.254","port":9876,"cipher":"chacha20-ietf-poly1305","password":"packet-codec"}}
            ],
            "outbound_groups":[{"tag":"chain","type":"relay","proxies":["hy","middle","codec"]}],
            "route":{"final":{"type":"route","outbound":"chain"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone())
            .apply_runtime_config_and_wait(config, Duration::from_secs(5)).await.unwrap();
        let source = proxy.system_tun_source_registry().owner("source").unwrap();
        source.wait_ready().await.unwrap();
        let endpoint = *source.original_listener_address.get().unwrap();
        let mut control = TcpStream::connect(endpoint).await.unwrap();
        control.write_all(&[5,1,0]).await.unwrap();
        let mut auth = [0;2]; control.read_exact(&mut auth).await.unwrap(); assert_eq!(auth,[5,0]);
        control.write_all(&[5,3,0,1,0,0,0,0,0,0]).await.unwrap();
        let mut reply=[0;10];control.read_exact(&mut reply).await.unwrap();assert_eq!(&reply[..2],&[5,0]);
        let relay=std::net::SocketAddr::from(([127,0,0,1],u16::from_be_bytes([reply[8],reply[9]])));
        let socket=UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for (target,payload) in [(1,b"packet".as_slice()),(2,b"".as_slice()),(1,b"reused".as_slice())] {
            let mut wire=vec![0,0,0,1,203,0,113,target,0,53];wire.extend_from_slice(payload);
            socket.send_to(&wire,relay).await.unwrap();let mut bytes=[0;2048];
            let size=socket.recv(&mut bytes).await.unwrap();assert_eq!(&bytes[..size],wire);
        }
        assert_eq!(server.authenticated.load(Ordering::SeqCst),1);
        assert_eq!(server.sessions.lock().unwrap().len(),1,"all inner targets share one actual Packet carrier");
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();
        let mut pending=vec![plan.configured("hy").unwrap()];let mut seen=BTreeSet::new();let mut original=None;
        while let Some(slot)=pending.pop() {
            if !seen.insert(slot.instance_id()) {continue;}
            for ordinal in 0..slot.recipe.spec().resources.len() {
                if let Some(provider)=slot.original_metadata_owner(ordinal) {
                    if let Some(wire)=provider.current_original() {original=Some((provider,wire));break;}
                }
            }
            if original.is_some(){break;}
            pending.extend(slot.dependency_instances().iter().filter_map(|id|plan.component(id.component)));
        }
        let (provider,original)=original.expect("the actual HY2 Metadata native original");
        assert!(original.closed().is_none());
        for _ in 0..3 {
        }
        drop(control);drop(socket);
        running.shutdown().await.unwrap();
        assert!(original.closed().unwrap().matches(&original));
        assert!(provider.closed().unwrap().matches(&provider));
        let closed=source.terminal_for_test().unwrap();
        assert!(closed.failure().is_none() && closed.task_failures().is_empty());
        assert!(closed.joined_tasks()>0 && closed.closed_io_receipts()>=2);
        server.close().await;
    }).await.unwrap();
}
