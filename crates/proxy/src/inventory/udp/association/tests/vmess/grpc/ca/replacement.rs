use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
pub(super) fn material(
    dir: &std::path::Path,
    label: &str,
) -> (zero_transport::tls::TlsAcceptor, String) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = cert.cert.pem();
    let cert_path = dir.join(format!("{label}-server.pem"));
    let key_path = dir.join(format!("{label}-key.pem"));
    std::fs::write(&cert_path, &pem).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let acceptor = zero_transport::tls::build_tls_acceptor(
        &zero_transport::profile::OwnedServerTlsProfile {
            options: Default::default(),
            cert_path: cert_path.display().to_string(),
            key_path: key_path.display().to_string(),
            alpn: vec!["h2".into()],
            server_fingerprint: None,
        },
        None,
    )
    .unwrap();
    (acceptor, pem)
}
#[tokio::test]
async fn ordinary_vmess_grpc_ca_apply_waits_for_originals_before_loading_new_trust_and_delivering_udp(
) {
    timeout(Duration::from_secs(20),async {
        let dir=tempfile::tempdir().unwrap();
        let (old_tls,old_ca)=material(dir.path(),"old");
        let (new_tls,new_ca)=material(dir.path(),"next");
        std::fs::write(dir.path().join("old.pem"),old_ca).unwrap();
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
            "outbounds":[{"tag":"node","protocol":{"type":"vmess","server":"127.0.0.1","port":listener.local_addr().unwrap().port(),"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["ca/Tun"]},"tls":{"server_name":"localhost","alpn":["h2"],"ca_cert_path":"old.pem"}}}],
            "route":{"final":{"type":"route","outbound":"node"}}
        }).to_string(),Some(dir.path().to_path_buf())).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();
        let running=proxy.spawn();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.unwrap();
        std::fs::remove_file(dir.path().join("old.pem")).unwrap();
        let (waiting,ready)=tokio::sync::oneshot::channel();
        let (release,finished)=tokio::sync::watch::channel(false);
        let server=tokio::spawn(async move {
            let mut tasks=tokio::task::JoinSet::new();
            let mut waiting=Some(waiting);
            for (index,acceptor) in [old_tls,new_tls].into_iter().enumerate() {
                let (stream,_)=listener.accept().await.unwrap();
                let waiting=if index==0 {waiting.take()} else {None};
                let mut finished=finished.clone();
                tasks.spawn(async move {
                    let io=zero_transport::tls::accept_tls_handshake(&acceptor,zero_platform_tokio::TokioSocket::new(stream)).await.unwrap();
                    let incoming=zero_transport::grpc::accept_grpc_connection(io,&zero_transport::profile::OwnedGrpcProfile {service_names:vec!["ca/Tun".into()],..Default::default()}).unwrap();
                    let io=incoming.accept().await.unwrap();
                    let profile=vmess::inbound::VmessInboundProfile::from_config_users([(ID.into(),"aes-128-gcm".into(),None::<String>,None,None,None,None,None)]).unwrap();
                    let (_,mut io)=profile.accept_request_stream(vmess::inbound::VmessInbound,zero_platform_tokio::TcpRelayStream::new(io)).await.unwrap();
                    let size=io.read_u16().await.unwrap();let mut payload=vec![0;size as usize];io.read_exact(&mut payload).await.unwrap();
                    assert_eq!(&payload[7..],if index==0 {b"held".as_slice()} else {b"new-trust".as_slice()});
                    io.write_u16(size).await.unwrap();io.write_all(&payload).await.unwrap();io.flush().await.unwrap();
                    if let Some(waiting)=waiting {
                        let mut byte=[0];let read=io.read(&mut byte);tokio::pin!(read);
                        poll_fn(|cx| {assert!(read.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
                        waiting.send(()).unwrap();let _=read.await;
                    }
                    if !*finished.borrow() {finished.wait_for(|done|*done).await.unwrap();}
                    incoming.close();
                });
            }
            while let Some(result)=tasks.join_next().await {result.unwrap();}
        });
        let runtime=UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let mut dispatch=runtime.new_dispatch("incoming").await.unwrap();
        dispatch.dispatch(input(53,b"held")).await.unwrap();assert_eq!(response(&mut dispatch).await.2,b"held");ready.await.unwrap();
        let registration=original(&proxy);let observed=registration.observe().unwrap();
        let native=observed.slots()[0].original().clone();let driver=observed.slots()[0].driver().unwrap().1.clone();
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();let old=plan.configured("node").unwrap();
        let historical=old.datagram(Instant::now()+Duration::from_secs(3)).await.unwrap();
        let mut candidate=config;
        let zero_config::OutboundProtocolConfig::Vmess{tls,..}=&mut candidate.outbounds[0].protocol else {unreachable!()};
        tls.as_mut().unwrap().ca_cert_path=Some("next.pem".into());
        let applying=handle.clone();let replacing=tokio::spawn(async move {applying.apply_runtime_config_and_wait(candidate,Duration::from_secs(10)).await});
        driver.wait().await;
        assert!(!replacing.is_finished(),"old logical views/control still held; next.pem does not exist yet");
        assert!(registration.closed().is_none());
        assert!(old.datagram(Instant::now()+Duration::from_secs(3)).await.is_err());
        std::fs::write(dir.path().join("next.pem"),new_ca).unwrap();
        let retired=proxy.tcp_runtime_services().execution().retired_datagrams();dispatch.finish_all();
        let report=proxy.protocols.drive_component_supervision(retired.progress_retired(Instant::now()+Duration::from_secs(3))).await.unwrap();assert!(report.is_complete(),"{report:?}");
        replacing.await.unwrap().unwrap();
        assert!(native.matches_closed(&native.closed().unwrap()));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(old.shutdown(Instant::now()+Duration::from_secs(3)).await.unwrap().is_complete());
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();let new=plan.configured("node").unwrap();assert_ne!(old.instance_id(),new.instance_id());
        assert_eq!(original(&proxy).observe().unwrap().retained(),0);
        std::fs::remove_file(dir.path().join("next.pem")).unwrap();
        let mut fresh=UdpIngressRuntime::new(proxy.tcp_runtime_services()).new_dispatch("new").await.unwrap();
        fresh.dispatch(input(53,b"new-trust")).await.unwrap();assert_eq!(response(&mut fresh).await.2,b"new-trust");
        let replacement=original(&proxy);let native=replacement.observe().unwrap().slots()[0].original().clone();
        fresh.finish_all();assert!(retired.wait_closed(Instant::now()+Duration::from_secs(3)).await.unwrap().is_complete());
        assert!(native.matches_closed(&native.closed().unwrap()));
        drop(historical);release.send_replace(true);server.await.unwrap();running.shutdown().await.unwrap();
        assert!(replacement.closed().unwrap().matches(&replacement));
    }).await.unwrap();
}
