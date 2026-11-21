//! Ordinary Source -> selected Direct role -> original controlled native owner.
use super::{PreparedTcpCandidateExecution, PreparedTcpOutbound};
use crate::{Proxy, ProxyHandle};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};
use zero_engine::{ResolvedOutbound, RouteDecision, RouteMode};
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
#[tokio::test]
async fn ordinary_named_and_builtin_direct_use_original_provider_with_halfclose_and_full_policy() {
    timeout(Duration::from_secs(25),async {
        for named in [false,true] {
            // This bound peer is retained until its actual accept operation.
            let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let peer=listener.local_addr().unwrap();
            let mut value=serde_json::json!({
                "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"udp":{"enabled":false},
                    "protocol":{"type":"direct","target":"127.0.0.1","port":peer.port()}}],
                "route":{"final":{"type":"direct"}}
            });
            if named {
                value["outbounds"]=serde_json::json!([{"tag":"named","protocol":{"type":"direct"},
                    "dial":{"address_family":"only_ipv4","source_ip":"127.0.0.1"}},
                    {"tag":"alternate","protocol":{"type":"direct"}}]);
                value["outbound_groups"]=serde_json::json!([{"tag":"ordered","type":"fallback","outbounds":["named","alternate"]}]);
                value["route"]["final"]=serde_json::json!({"type":"route","outbound":"named"});
            }
            let config=zero_config::RuntimeConfig::parse(&value.to_string()).unwrap();
            let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
            ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone())
                .apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
            let source=proxy.system_tun_source_registry().owner("entry").unwrap();source.wait_ready().await.unwrap();
            let entry=*source.original_listener_address.get().unwrap();
            let services=proxy.tcp_runtime_services();let plan=services.components.as_ref().unwrap();
            let slot=plan.direct(named.then_some("named")).unwrap();
            let decision=if named {RouteDecision::Route("named".into())}else{RouteDecision::Direct};
            let (resolved,_)=proxy.engine().resolve_route_decision(decision).unwrap();
            let ResolvedOutbound::Single(_leaf)=resolved.clone() else {unreachable!()};
            assert!(services.invocation.is_none(), "metadata selection requires no fabricated request identity");
            let original_provider = slot.original_carrier_owner(0).unwrap();
            assert_eq!(original_provider.observe().unwrap().retained(), 0);
            for mode in [RouteMode::Auto,RouteMode::Flow,RouteMode::Translate] {
                let selected=services.prepare_tcp_outbound(&resolved,mode).unwrap_or_else(|failure|panic!("{}: {}",failure.stage,failure.error));
                let PreparedTcpOutbound::Single(selected)=selected else {unreachable!()};
                let PreparedTcpCandidateExecution::Native(actual)=selected.execution else {panic!("Direct restored a private connector");};
                assert_eq!(actual.instance(),slot.instance_id());
            }
            assert_eq!(services.prepare_tcp_outbound(&resolved,RouteMode::Packet).err().unwrap().stage,"data_plane_route");
            assert_eq!(original_provider.observe().unwrap().retained(), 0,
                "selecting actual role generations cannot acquire native I/O");
            if named {
                let (group,_)=proxy.engine().resolve_route_decision(RouteDecision::Route("ordered".into())).unwrap();
                let PreparedTcpOutbound::Fallback(selected)=services.prepare_tcp_outbound(&group,RouteMode::Flow)
                    .unwrap_or_else(|failure|panic!("{}",failure.error)) else {panic!("ordered fallback required");};
                assert_eq!(selected.iter().map(|candidate|candidate.tag.as_deref().unwrap()).collect::<Vec<_>>(),["named","alternate"]);
                for selected in selected {assert!(matches!(selected.execution,PreparedTcpCandidateExecution::Native(_)));}
            }
            let mut front=TcpStream::connect(entry).await.unwrap();
            let (mut back,remote)=tokio::time::timeout_at(end(),listener.accept()).await.unwrap().unwrap();
            assert_eq!(remote.ip(),peer.ip());
            front.write_all(b"original-forward").await.unwrap();let mut bytes=[0;16];
            back.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"original-forward");
            let owner=slot.original_carrier_owner(0).expect("same standard Direct provider");
            let observed=owner.observe().unwrap();assert_eq!(observed.retained(),1);
            let original=observed.slots()[0].original().clone();assert!(original.closed().is_none());
            front.shutdown().await.unwrap();let mut tail=Vec::new();back.read_to_end(&mut tail).await.unwrap();assert!(tail.is_empty());
            back.write_all(b"original-return").await.unwrap();back.shutdown().await.unwrap();
            let mut reply=Vec::new();front.read_to_end(&mut reply).await.unwrap();assert_eq!(&reply,b"original-return");
            drop(front);drop(back);
            running.shutdown().await.unwrap();
            let closed=original.closed().expect("same actual native close receipt");assert!(original.matches_closed(&closed));
            assert_eq!(owner.observe().unwrap().retained(),0);
            let terminal=source.terminal_for_test().expect("original Source terminal");
            assert!(terminal.failure().is_none()&&terminal.task_failures().is_empty()&&terminal.native_bound_failures().is_empty());
            assert!(terminal.closed_io_receipts()>0&&terminal.joined_tasks()>0);
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
        }
    }).await.unwrap();
}
#[tokio::test]
async fn retired_direct_plan_never_rebinds_a_cached_candidate_to_a_new_owner() {
    let config=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"named","protocol":{"type":"direct"}}],"route":{"final":{"type":"route","outbound":"named"}}}"#).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let services = proxy.tcp_runtime_services();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("named".into()))
        .unwrap();
    let PreparedTcpOutbound::Single(selected) = services
        .prepare_tcp_outbound(&resolved, RouteMode::Flow)
        .unwrap_or_else(|failure| panic!("{}", failure.error))
    else {
        unreachable!()
    };
    let PreparedTcpCandidateExecution::Native(actual) = selected.execution else {
        unreachable!()
    };
    let previous = actual.instance();
    running.shutdown().await.unwrap();
    drop(services);
    drop(proxy);
    let replacement=Proxy::new(zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"named","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
    assert!(
        actual.slot().is_err(),
        "a cached weak selection cannot retarget another host by tag"
    );
    assert_eq!(actual.instance(), previous);
    drop(replacement);
}

#[tokio::test]
async fn ordinary_direct_probe_retains_the_original_os_cause_without_a_raw_connector() {
    timeout(Duration::from_secs(10),async {
        // TCP cannot connect to broadcast. Compare the original controlled
        // failure to the real native kernel cause on this platform.
        let endpoint = "255.255.255.255:9";
        let expected = TcpStream::connect(endpoint).await.unwrap_err();
        assert!(expected.raw_os_error().is_some());
        let config=zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"named","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();
        ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone())
            .apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        // Use the public diagnostic entry so the same Host retains its real
        // Probe caller and original controller through failure and cleanup.
        let error=proxy.probe_outbound_single("named", &format!("http://{endpoint}/actual"))
            .await.expect_err("same actual controlled connect rejection");
        let mut pending=vec![&error as &(dyn std::error::Error+'static)];let mut found=false;
        let mut inspected=0;
        while let Some(source)=pending.pop() {
            inspected+=1;assert!(inspected<128,"bounded original error chain");
            if let Some(channel)=source.downcast_ref::<zero_platform_tokio::contract::ChannelError>() {
                if let Some(actual)=zero_platform_tokio::controlled_carrier::original_connect_error(channel) {
                    assert_eq!(actual.error().kind(),expected.kind());
                    assert_eq!(actual.error().raw_os_error(),expected.raw_os_error());
                    assert!(actual.error().raw_os_error().is_some());
                    assert_eq!(actual.stage(),"connect_socket");found=true;break;
                }
            }
            if let Some(zero_engine::EngineError::Io(io))=source.downcast_ref::<zero_engine::EngineError>() {pending.push(io);}
            if let Some(io)=source.downcast_ref::<std::io::Error>() {
                if let Some(inner)=io.get_ref() {pending.push(inner);}
            }
            if let Some(next)=source.source() {pending.push(next);}
        }
        assert!(found,"diagnostics must preserve the actual native provider cause");
        running.shutdown().await.unwrap();
    }).await.unwrap();
}

#[cfg(feature = "http")]
#[path = "direct/message.rs"]
mod message;

#[cfg(feature = "vmess")]
#[path = "direct/mux.rs"]
mod mux;
