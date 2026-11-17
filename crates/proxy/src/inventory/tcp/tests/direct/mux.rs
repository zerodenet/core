//! Concurrent protocol requests keep the Source's real accepted child Scopes.
use super::*;
use zero_core::{Address, Network, ProtocolType, Session};
const UUID: &str = "11111111-2222-3333-4444-555555555555";
fn frame(id: u16, status: u8, target: Option<std::net::SocketAddr>, payload: &[u8]) -> Vec<u8> {
    let mut meta = id.to_be_bytes().to_vec();
    meta.push(status);
    meta.push(u8::from(!payload.is_empty()));
    if let Some(target) = target {
        let std::net::IpAddr::V4(ip) = target.ip() else {
            unreachable!()
        };
        meta.push(1);
        meta.extend(target.port().to_be_bytes());
        meta.push(1);
        meta.extend(ip.octets());
    }
    let mut bytes = (meta.len() as u16).to_be_bytes().to_vec();
    bytes.extend(meta);
    if !payload.is_empty() {
        bytes.extend((payload.len() as u16).to_be_bytes());
        bytes.extend(payload);
    }
    bytes
}
#[tokio::test]
async fn two_concurrent_mux_streams_use_actual_source_children_and_standard_direct_originals() {
    timeout(Duration::from_secs(15),async{
        let first=TcpListener::bind("127.0.0.1:0").await.unwrap();let first_address=first.local_addr().unwrap();
        let second=TcpListener::bind("127.0.0.1:0").await.unwrap();let second_address=second.local_addr().unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"mux","listen":{"address":"127.0.0.1","port":0},"udp":{"enabled":false},"protocol":{"type":"vmess","users":[{"id":UUID}]}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone()).apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        let source=proxy.system_tun_source_registry().owner("mux").unwrap();source.wait_ready().await.unwrap();let entry=*source.original_listener_address.get().unwrap();
        let services=proxy.tcp_runtime_services();let slot=services.components.as_ref().unwrap().direct(None).unwrap();let physical=slot.original_carrier_owner(0).unwrap();
        let tcp=TcpStream::connect(entry).await.unwrap();let session=Session::new(99,Address::Domain("v1.mux.cool".into()),666,Network::Tcp,ProtocolType::new("library-client"));
        let(mut client,_)=::vmess::outbound::PreparedVmessOutboundRequestBundle::from_config(UUID,"chacha20-poly1305",None).unwrap().establish_tcp_outbound_stream(zero_platform_tokio::TcpRelayStream::new(tcp),&session).await.unwrap();
        client.write_all(&frame(1,1,Some(first_address),b"first")).await.unwrap();client.flush().await.unwrap();
        let(mut one,_)=first.accept().await.unwrap();let mut uploaded=[0;5];one.read_exact(&mut uploaded).await.unwrap();assert_eq!(&uploaded,b"first");
        // Retain the genuinely live first stream while the second is admitted.
        {let mut byte=[0];let reading=one.read(&mut byte);tokio::pin!(reading);assert!(futures_util::poll!(reading.as_mut()).is_pending());}
        client.write_all(&frame(2,1,Some(second_address),b"second")).await.unwrap();client.flush().await.unwrap();
        let(mut two,_)=second.accept().await.unwrap();let mut uploaded=[0;6];two.read_exact(&mut uploaded).await.unwrap();assert_eq!(&uploaded,b"second");
        let observed=physical.observe().unwrap();assert_eq!(observed.retained(),2);let originals=observed.slots().iter().map(|row|row.original().clone()).collect::<Vec<_>>();assert!(originals.iter().all(|original|original.closed().is_none()));
        for id in [1,2]{client.write_all(&frame(id,3,None,b"")).await.unwrap();}client.flush().await.unwrap();
        let mut rest=Vec::new();one.read_to_end(&mut rest).await.unwrap();assert!(rest.is_empty());two.read_to_end(&mut rest).await.unwrap();assert!(rest.is_empty());
        one.write_all(b"one").await.unwrap();one.shutdown().await.unwrap();two.write_all(b"two").await.unwrap();two.shutdown().await.unwrap();drop(one);drop(two);
        let mut payloads=[Vec::new(),Vec::new()];let mut ended=[false,false];while !ended.iter().all(|value|*value){let size=client.read_u16().await.unwrap();let mut meta=vec![0;size as usize];client.read_exact(&mut meta).await.unwrap();assert!(meta.len()>=4);let id=u16::from_be_bytes([meta[0],meta[1]])as usize;assert!((1..=2).contains(&id));if meta[3]&1!=0{let size=client.read_u16().await.unwrap()as usize;let mut bytes=vec![0;size];client.read_exact(&mut bytes).await.unwrap();payloads[id-1].extend(bytes);}
        if meta[2]==3{ended[id-1]=true;}}
        assert_eq!(payloads,[b"one".to_vec(),b"two".to_vec()]);drop(client);
        while proxy.engine().completed_sessions().len()!=2 {tokio::task::yield_now().await;}
        let records=proxy.engine().completed_sessions();assert_ne!(records[0].id,records[1].id);assert!(records.iter().all(|record|record.protocol.as_str()=="vmess"));
        running.shutdown().await.unwrap();
        for original in originals{let receipt=original.closed().expect("actual concurrent Direct native close");assert!(original.matches_closed(&receipt)&&receipt.had_native_acquisition());}
        let terminal=source.terminal_for_test().unwrap();assert!(terminal.failure().is_none()&&terminal.task_failures().is_empty()&&terminal.native_bound_failures().is_empty());assert!(terminal.joined_tasks()>=3&&terminal.closed_io_receipts()>0);
        assert_eq!(physical.observe().unwrap().retained(),0);assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
