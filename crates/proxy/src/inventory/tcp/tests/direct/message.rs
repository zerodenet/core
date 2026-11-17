//! Sequential HTTP exchanges use their original Engine admission and Source.
use super::*;
#[tokio::test]
async fn forward_keepalive_messages_use_the_same_standard_direct_provider_and_real_admissions() {
    timeout(Duration::from_secs(15), async {
        let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = origin.local_addr().unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"http"}}],
            "outbounds":[{"tag":"named","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv4","source_ip":"127.0.0.1"}}],
            "route":{"final":{"type":"route","outbound":"named"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone())
            .apply_runtime_config_and_wait(config, Duration::from_secs(5)).await.unwrap();
        let source = proxy.system_tun_source_registry().owner("entry").unwrap();
        source.wait_ready().await.unwrap();
        let entry = *source.original_listener_address.get().unwrap();
        let services = proxy.tcp_runtime_services();
        let slot = services.components.as_ref().unwrap().direct(Some("named")).unwrap();
        let physical = slot.original_carrier_owner(0).unwrap();
        let before = proxy.engine().mint_session_identity().unwrap().id().0;
        let mut client = TcpStream::connect(entry).await.unwrap();
        let mut originals = Vec::new();
        let mut uploaded = Vec::new();
        let mut downloaded = Vec::new();
        for index in 0..2 {
            client.write_all(format!("GET http://{target}/message-{index} HTTP/1.1\r\nHost: {target}\r\nConnection: {}\r\n\r\n", if index==0 {"keep-alive"} else {"close"}).as_bytes()).await.unwrap();
            // The bound origin stays reserved until this actual native accept.
            let (mut peer, remote) = origin.accept().await.unwrap();
            assert_eq!(remote.ip(), target.ip());
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") { head.push(peer.read_u8().await.unwrap()); }
            assert!(head.starts_with(format!("GET /message-{index} HTTP/1.1\r\n").as_bytes()));
            uploaded.push(head.len() as u64);
            let observed = physical.observe().unwrap();
            let current = observed.slots().iter().find(|row| row.original().closed().is_none() && row.physical_view_live()).expect("actual message uses standard Direct native owner");
            let original = current.original().clone();
            assert!(!originals.iter().any(|previous: &zero_platform_tokio::controlled_carrier::CarrierOriginObservation| previous.same_original(&original)));
            originals.push(original);
            let response = format!("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n{index}");
            peer.write_all(response.as_bytes()).await.unwrap();peer.shutdown().await.unwrap();
            let mut returned = Vec::new();
            while !returned.ends_with(b"\r\n\r\n") { returned.push(client.read_u8().await.unwrap()); }
            assert!(returned.starts_with(b"HTTP/1.1 200 OK\r\n"));
            let body = client.read_u8().await.unwrap();assert_eq!(body,b'0'+index as u8);
            downloaded.push((returned.len()+1) as u64);drop(peer);
        }
        let mut tail=Vec::new();client.read_to_end(&mut tail).await.unwrap();assert!(tail.is_empty());drop(client);
        while proxy.engine().completed_sessions().len()!=2 {tokio::task::yield_now().await;}
        let mut records=proxy.engine().completed_sessions();records.sort_by_key(|record|record.id);
        assert_eq!(records.iter().map(|record|record.id).collect::<Vec<_>>(),[before+1,before+2]);
        for (index,record) in records.iter().enumerate() {
            assert_eq!(record.protocol.as_str(),"http");assert_eq!(record.outbound_tag.as_deref(),Some("named"));
            assert_eq!((record.bytes_up,record.bytes_down),(uploaded[index],downloaded[index]));
        }
        running.shutdown().await.unwrap();
        for original in &originals {let closed=original.closed().expect("matching original message native close");assert!(original.matches_closed(&closed)&&closed.had_native_acquisition());}
        assert_eq!(physical.observe().unwrap().retained(),0);
        let terminal=source.terminal_for_test().unwrap();
        assert!(terminal.failure().is_none()&&terminal.task_failures().is_empty()&&terminal.native_bound_failures().is_empty());
        assert!(terminal.closed_io_receipts()>0&&terminal.joined_tasks()>0);
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
