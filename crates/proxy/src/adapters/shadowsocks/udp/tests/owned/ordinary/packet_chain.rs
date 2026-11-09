//! Ordinary admitted SOCKS Source -> SOCKS Datagram carrier -> original SS
//! codec. Peers use the real existing codecs; native/Scope proof comes only
//! from the actual product owners, independently of peer EOF/socket Drop.
use super::*;
use futures_util::FutureExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::net::TcpListener;
use zero_core::Network;

#[tokio::test]
async fn ordinary_packet_chain_returns_real_dynamic_bnd_originals_and_survives_first_business_end()
{
    timeout(Duration::from_secs(25),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let physical=UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bnd=physical.local_addr().unwrap();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[
                {"tag":"carrier","protocol":{"type":"socks5","server":"127.0.0.1","port":listener.local_addr().unwrap().port()}},
                {"tag":"ss","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":9876,"cipher":"aes-128-gcm","password":"actual-packet-chain"}}
            ],
            "outbound_groups":[{"tag":"chain","type":"relay","proxies":["carrier","ss"]}],
            "route":{"final":{"type":"route","outbound":"chain"}}
        }).to_string()).unwrap();
        let codec=crate::adapters::shadowsocks::transport_leaf(&Default::default(),"ss",&config.outbounds[1].protocol).unwrap().udp_packet_path_plan().unwrap().carrier_codec();
        let phase=Arc::new(AtomicUsize::new(0));
        let peer_phase=phase.clone();
        let peer=tokio::spawn(async move {
            let (tcp,_)=listener.accept().await.unwrap(); peer_phase.store(1,Ordering::SeqCst);
            let mut control=zero_platform_tokio::TokioSocket::new(tcp);
            assert!(matches!(socks5::Socks5Inbound.accept_command(&mut control).await.unwrap(),socks5::Socks5Request::UdpAssociate(_)));
            socks5::Socks5Inbound.send_success_response_with_bound(&mut control,&Address::Ipv4([127,0,0,1]),bnd.port()).await.unwrap();
            peer_phase.store(2,Ordering::SeqCst);
            let mut control=control.into_inner().unwrap();
            let wire=socks5::udp::Socks5InboundUdpCodec;
            let mut buffer=[0u8;65536];
            let mut original_sender=None;
            let mut packet=0usize;
            for (target,expected) in [(1,b"first".as_slice()),(2,b"".as_slice()),(2,b"still-live".as_slice())] {
                peer_phase.store(10*packet+3,Ordering::SeqCst);
                let (size,sender)=physical.recv_from(&mut buffer).await.unwrap(); peer_phase.store(10*packet+4,Ordering::SeqCst);
                if let Some(original)=original_sender {assert_eq!(sender,original);}original_sender=Some(sender);
                let outer=wire.decode_request(&buffer[..size]).unwrap();
                assert_eq!(outer.port(),9876); peer_phase.store(10*packet+5,Ordering::SeqCst);
                let (address,port,payload)=codec.decode(outer.payload()).unwrap();
                assert_eq!(address,Address::Ipv4([203,0,113,target]));assert_eq!(port,53);assert_eq!(payload,expected);
                peer_phase.store(10*packet+6,Ordering::SeqCst);
                let response=codec.encode(&address,port,expected).unwrap();
                let response=wire.encode_response_to_client(outer.target(),outer.port(),&response).unwrap();
                physical.send_to(&response,sender).await.unwrap(); peer_phase.store(10*packet+7,Ordering::SeqCst);packet+=1;
            }
            let mut tail=Vec::new();control.read_to_end(&mut tail).await.unwrap();assert!(tail.is_empty());control.shutdown().await.unwrap();
        });
        let proxy=Proxy::new(config).unwrap();
        let registry=proxy.system_tun_source_registry();let running=proxy.spawn();
        let incoming=loop {if let Some(owner)=registry.owner("entry") {break owner;}tokio::task::yield_now().await;};
        incoming.wait_ready().await.unwrap();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait((*proxy.engine().config()).clone(),Duration::from_secs(5)).await.unwrap();
        let endpoint=*incoming.original_listener_address.get().unwrap();
        let (control,socket,relay)=associate(endpoint).await;
        send(&socket,relay,1,b"first").await;receive_observed(&socket,1,b"first",&proxy,&phase,&peer).await;
        send(&socket,relay,2,b"").await;receive_observed(&socket,2,b"",&proxy,&phase,&peer).await;
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();
        let carrier=plan.configured("carrier").unwrap().original_carrier_owner(0).unwrap();
        let observations=carrier.observe().unwrap();
        assert_eq!(observations.slots().len(),2,"dynamic SOCKS BND retains its actual TCP control and UDP original");
        let originals=observations.slots().iter().map(|slot|{
            assert!(slot.native().is_some());slot.original().clone()
        }).collect::<Vec<_>>();
        let sessions=proxy.engine().active_sessions();
        let first=sessions.iter().find(|session|session.network==Network::Udp&&session.target==Address::Ipv4([203,0,113,1])).expect("first actual business").id;
        let second=sessions.iter().find(|session|session.network==Network::Udp&&session.target==Address::Ipv4([203,0,113,2])).expect("second actual business").id;
        assert_ne!(first,second);
        // Use the real administrative close operation. A second bookkeeping
        // SessionHandle::finish would remove only Engine's row while the actual
        // UdpDispatch owner still holds and must finalize this flow exactly once.
        proxy.engine().close_flow(&first.to_string()).unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                if let Some(completed) = proxy.engine().completed_sessions().iter().find(|flow| flow.id == first) {
                    assert_eq!(completed.outcome, zero_engine::SessionOutcome::Cancelled);
                    assert_eq!(completed.close_reason.as_deref(), Some("manual"));
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(!proxy.engine().active_sessions().iter().any(|flow| flow.id == first));
        assert!(proxy.engine().active_sessions().iter().any(|flow| flow.id == second));
        send(&socket,relay,2,b"still-live").await;receive_observed(&socket,2,b"still-live",&proxy,&phase,&peer).await;
        let same=carrier.observe().unwrap();assert_eq!(same.slots().len(),2);
        for original in &originals {assert!(same.slots().iter().any(|slot|original.same_original(slot.original())));assert!(original.closed().is_none());}
        assert!(proxy.engine().active_sessions().iter().any(|session|session.id==second));
        drop(socket);drop(control);
        running.shutdown().await.unwrap();peer.await.unwrap();
        for original in originals {let closed=original.closed().expect("actual native provider close, not peer EOF");assert!(original.matches_closed(&closed));assert!(closed.had_native_acquisition());}
        assert!(carrier.closed().unwrap().matches(&carrier));assert!(carrier.users_closed().unwrap().matches(&carrier));
        let terminal=incoming.terminal().expect("actual Source joins and native cleanup before publication");
        assert_eq!(terminal.identity(),incoming.identity());assert!(terminal.joined_tasks()>=2);assert!(terminal.task_failures().is_empty());
    }).await.unwrap();
}

// Preserve the existing receive helper, exact 4s timer and original assertion.
// Only on its real panic, print finite actor/flow metadata and rethrow it.
async fn receive_observed(
    socket: &UdpSocket,
    target: u8,
    payload: &[u8],
    proxy: &Proxy,
    phase: &AtomicUsize,
    peer: &tokio::task::JoinHandle<()>,
) {
    if let Err(panic) = std::panic::AssertUnwindSafe(receive(socket, target, payload))
        .catch_unwind()
        .await
    {
        eprintln!(
            "actual packet-chain peer phase={} finished={} active={:?} completed={:?}",
            phase.load(Ordering::SeqCst),
            peer.is_finished(),
            proxy
                .engine()
                .active_sessions()
                .iter()
                .map(|flow| (flow.id, flow.network, flow.target.clone(), flow.port))
                .collect::<Vec<_>>(),
            proxy
                .engine()
                .completed_sessions()
                .iter()
                .map(|flow| (
                    flow.id,
                    flow.target.clone(),
                    flow.port,
                    flow.failure.clone()
                ))
                .collect::<Vec<_>>()
        );
        if let Some(plan) = proxy.tcp_runtime_services().components.as_ref().ok() {
            if let Some(owner) = plan
                .configured("carrier")
                .and_then(|slot| slot.original_carrier_owner(0))
            {
                if let Some(actual) = owner.observe() {
                    eprintln!(
                        "actual SOCKS carrier original rows={:?}",
                        actual
                            .slots()
                            .iter()
                            .map(|row| (
                                row.ordinal(),
                                row.native().map(|native| native.is_closed()),
                                row.driver().map(|(_, driver)| driver.is_joined()),
                                row.auxiliary()
                                    .and_then(|aux| aux.try_joined())
                                    .map(|joined| (
                                        joined.id(),
                                        joined.registration_released(),
                                        joined.result().is_ok()
                                    ))
                            ))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        std::panic::resume_unwind(panic);
    }
}
