use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_budget_apply_waits_for_old_users_driver_and_original_before_successor()
{
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    timeout(Duration::from_secs(20),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port=listener.local_addr().unwrap().port();
        let mut config=zero_config::RuntimeConfig::parse(&serde_json::json!({"outbounds":[{"tag":"node","protocol":{"type":"vmess","server":"127.0.0.1","port":port,"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["old/Tun"]}}}],"route":{"final":{"type":"route","outbound":"node"}}}).to_string()).unwrap();
        config.runtime.vmess_grpc_max_originals=Some(2);
        let proxy=Proxy::new(config.clone()).unwrap();
        let running=proxy.spawn();
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.unwrap();
        let (waiting,ready)=tokio::sync::oneshot::channel();
        let (release,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (stream,_)=listener.accept().await.unwrap();
            let incoming=zero_transport::grpc::accept_grpc_connection(stream,&zero_transport::profile::OwnedGrpcProfile{service_names:vec!["old/Tun".into()],..Default::default()}).unwrap();
            let stream=incoming.accept().await.unwrap();
            let profile=vmess::inbound::VmessInboundProfile::from_config_users([(ID.into(),"aes-128-gcm".into(),None::<String>,None,None,None,None,None)]).unwrap();
            let (_,mut stream)=profile.accept_request_stream(vmess::inbound::VmessInbound,zero_platform_tokio::TcpRelayStream::new(stream)).await.unwrap();
            let length=stream.read_u16().await.unwrap();let mut payload=vec![0;length as usize];stream.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload[7..],b"held");
            stream.write_u16(length).await.unwrap();stream.write_all(&payload).await.unwrap();stream.flush().await.unwrap();
            let mut byte=[0];let read=stream.read(&mut byte);tokio::pin!(read);
            poll_fn(|cx|{assert!(read.as_mut().poll(cx).is_pending(),"actual H2 logical read must still be pending");Poll::Ready(())}).await;
            waiting.send(()).unwrap();
            // Host cancellation may end the peer with EOF or an H2 reset.
            // Neither is used as the client's native completion evidence.
            if let Ok(length)=read.await {assert_eq!(length,0);}
            finished.await.unwrap();incoming.close();
        });
        let runtime=UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let mut dispatch=runtime.new_dispatch("incoming").await.unwrap();
        dispatch.dispatch(input(53,b"held")).await.unwrap();
        assert_eq!(response(&mut dispatch).await.2,b"held");
        ready.await.unwrap();
        let registration=original(&proxy);let observed=registration.observe().unwrap();
        let original=observed.slots()[0].original().clone();
        let driver=observed.slots()[0].driver().unwrap().1.clone();
        let old_plan=proxy.tcp_runtime_services().components.clone().unwrap();let old=old_plan.configured("node").unwrap();
        let historical=old.datagram(Instant::now()+Duration::from_secs(3)).await.unwrap();
        let mut candidate=config.clone();
        candidate.runtime.vmess_grpc_max_originals=Some(1);
        assert_eq!(registration.limit().get(),2);
        let applying=handle.clone();
        let replacing=tokio::spawn(async move {applying.apply_runtime_config_and_wait(candidate,Duration::from_secs(10)).await});
        timeout(Duration::from_secs(3),driver.wait()).await.unwrap();
        assert!(!replacing.is_finished(),"joined driver alone cannot return held Datagram views/control");
        assert!(registration.closed().is_none());
        assert!(old.datagram(Instant::now()+Duration::from_secs(3)).await.is_err());
        let retired=proxy.tcp_runtime_services().execution().retired_datagrams();
        dispatch.finish_all();
        let report=proxy.protocols.drive_component_supervision(retired.progress_retired(Instant::now()+Duration::from_secs(3))).await.unwrap();
        assert!(report.is_complete(),"{report:?}");
        replacing.await.unwrap().unwrap();
        assert!(original.matches_closed(&original.closed().unwrap()));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        let new_plan=proxy.tcp_runtime_services().components.clone().unwrap();let new=new_plan.configured("node").unwrap();
        assert_ne!(old.instance_id(),new.instance_id());
        assert_eq!(new.recipe.spec().task_limit.unwrap().get(),1);
        assert_eq!(new.original_carrier_owner(0).unwrap().limit().get(),1);
        assert_eq!(new.original_carrier_owner(1).unwrap().limit().get(),1);
        assert_eq!(new.original_carrier_owner(0).unwrap().observe().unwrap().retained(),0,"candidate starts only after confirmed cleanup and stays cold until business demand");
        // A retained old plan cannot pin or reacquire a retired profile. Reuse
        // its recipe only after the actual original owner has confirmed close.
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        let restored=proxy.tcp_runtime_services().components.clone().unwrap();
        assert_ne!(old.instance_id(),restored.configured("node").unwrap().instance_id());
        assert!(old.datagram(Instant::now()+Duration::from_secs(3)).await.is_err());
        drop(historical);
        release.send(()).unwrap();server.await.unwrap();
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
