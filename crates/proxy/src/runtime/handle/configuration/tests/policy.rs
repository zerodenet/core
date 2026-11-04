use super::owned::paused;
use super::*;
use zero_api::{CommandRequest, CommandService, ModeSetCommand, PolicySelectCommand};

fn at_port(mut config: RuntimeConfig, port: u16) -> RuntimeConfig {
    config.inbounds[0].listen.port = port;
    config
}

fn operation_facts(proxy: &Proxy, reconciler: &super::owned::Paused) -> String {
    let pending = proxy
        .host
        .reload_ack()
        .try_lock()
        .map(|pending| {
            pending.as_ref().map(|pending| {
                (
                    pending.expected.config_revision(),
                    pending.previous.config_revision(),
                    pending.expected.configuration_publication().is_committed(),
                    pending.compensating,
                )
            })
        })
        .map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => "original ACK lock busy",
            std::sync::TryLockError::Poisoned(_) => "original ACK lock poisoned",
        });
    let owner = proxy.inbound_owners.owner("reload-test");
    format!(
        "ready={}, revision={}, host={:?}, pending_ack={pending:?}, calls={:?}, original={:?}",
        *proxy.orchestration_ready.borrow(),
        proxy.engine().config_revision(),
        proxy.host.apply(),
        reconciler.calls.lock().unwrap(),
        owner.map(|owner| (
            owner.identity(),
            owner.is_ready(),
            owner.terminal_for_test()
        ))
    )
}

async fn start_ready(
    proxy: &Proxy,
    reserved: (std::net::TcpListener, std::net::UdpSocket),
) -> crate::RunningProxy {
    let mut ready = proxy.orchestration_ready.subscribe();
    drop(reserved);
    let mut running = proxy.spawn();
    let mut exited = false;
    let readiness = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            result = ready.wait_for(|ready| *ready) => result.map(|_| ()).map_err(|error| error.to_string()),
            result = &mut running.task => {
                exited = true;
                Err(format!("original startup task exited: {result:?}"))
            }
        }
    }).await;
    if !matches!(readiness, Ok(Ok(()))) {
        let cleanup = if exited {
            "original runtime task already joined above".to_owned()
        } else {
            if let Some(stop) = running.shutdown.take() {
                let _ = stop.send(());
            }
            format!("original runtime cleanup: {:?}", (&mut running.task).await)
        };
        panic!("policy fixture never reached actual startup Ready: {readiness:?}; {cleanup}");
    }
    running
}

async fn wait_entered(
    proxy: &Proxy,
    reconciler: &super::owned::Paused,
    request: &mut tokio::task::JoinHandle<Result<(), String>>,
    running: &mut crate::RunningProxy,
) {
    let mut request_joined = false;
    let mut runtime_joined = false;
    let entered = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! { biased;
            _ = reconciler.entered.notified() => Ok(()),
            result = &mut *request => {
                request_joined = true;
                Err(format!("actual caller completed before application Ready: {result:?}"))
            }
            result = &mut running.task => {
                runtime_joined = true;
                Err(format!("original runtime exited before application Ready: {result:?}"))
            }
        }
    })
    .await;
    if !matches!(entered, Ok(Ok(()))) {
        let facts = operation_facts(proxy, reconciler);
        // Release this same domain-only gate, then request normal host stop.
        // Neither caller timeout nor a missing notification is cleanup proof.
        reconciler.release.notify_one();
        if let Some(stop) = running.shutdown.take() {
            let _ = stop.send(());
        }
        let runtime_cleanup = if runtime_joined {
            "original runtime task already joined above".to_owned()
        } else {
            format!("original runtime cleanup: {:?}", (&mut running.task).await)
        };
        let request_cleanup = if request_joined {
            "actual caller result already consumed above".to_owned()
        } else {
            format!("actual caller join: {:?}", request.await)
        };
        // An exited/failed runtime may have left an incomplete accepted Scope.
        // Drive that same owner with its existing shutdown grace as well; a
        // caller JoinHandle alone never proves the accepted operation joined.
        let accepted_cleanup = proxy
            .host
            .apply()
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        panic!("application Ready was not observed: {entered:?}; {facts}; {runtime_cleanup}; {request_cleanup}; original accepted Scope cleanup: {accepted_cleanup:?}");
    }
}

fn selected(proxy: &Proxy) -> String {
    let snapshot = proxy.engine().runtime_snapshot();
    let id = snapshot.plan().target_id("pick").unwrap();
    let selection = snapshot.group_selection_binding(id).unwrap();
    snapshot
        .plan()
        .target(selection.selected())
        .unwrap()
        .tag()
        .to_owned()
}
fn selector_config(cidr: &str, reverse: bool) -> RuntimeConfig {
    let mut config = config(cidr);
    let nodes = if reverse { ["b", "a"] } else { ["a", "b"] };
    config.outbounds = serde_json::from_value(serde_json::json!(
        nodes.map(|tag| serde_json::json!({"tag":tag,"protocol":{"type":"direct"}}))
    ))
    .unwrap();
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"pick","type":"selector","outbounds":["a","b"],"selected":"a"}
    ]))
    .unwrap();
    config
}
#[tokio::test]
async fn selector_ack_during_actual_pending_transaction_survives_plan_id_remapping() {
    let reserved = crate::socket_fixture::tcp_udp();
    let port = reserved.0.local_addr().unwrap().port();
    let proxy = Proxy::new(at_port(selector_config("198.18.0.0/24", false), port)).unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let apply = handle.clone();
    let mut running = start_ready(&proxy, reserved).await;
    let mut request = tokio::spawn(async move {
        apply
            .apply_runtime_config_and_wait(
                at_port(selector_config("198.19.0.0/24", true), port),
                Duration::from_secs(5),
            )
            .await
    });
    wait_entered(&proxy, &reconciler, &mut request, &mut running).await;
    assert_eq!(selected(&proxy), "a");
    let ack = handle
        .execute(CommandRequest::PolicySelect(PolicySelectCommand {
            policy_tag: "pick".into(),
            target_tag: "b".into(),
        }))
        .unwrap();
    assert!(ack.accepted);
    assert_eq!(selected(&proxy), "b");
    reconciler.release.notify_one();
    request.await.unwrap().unwrap();
    assert_eq!(selected(&proxy), "b");
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn mode_ack_during_pending_configuration_conflicts_without_rolling_back_its_revision() {
    let reserved = crate::socket_fixture::tcp_udp();
    let port = reserved.0.local_addr().unwrap().port();
    let proxy = Proxy::new(at_port(config("198.18.0.0/24"), port)).unwrap();
    let reconciler = paused(false);
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .with_config_apply_reconciler(reconciler.clone());
    let apply = handle.clone();
    let mut running = start_ready(&proxy, reserved).await;
    let mut request = tokio::spawn(async move {
        apply
            .apply_runtime_config_and_wait(
                at_port(config("198.19.0.0/24"), port),
                Duration::from_secs(5),
            )
            .await
    });
    wait_entered(&proxy, &reconciler, &mut request, &mut running).await;
    let before = proxy.engine().config_revision();
    let ack = handle
        .execute(CommandRequest::ModeSet(ModeSetCommand {
            mode: "direct".into(),
            outbound: None,
        }))
        .unwrap();
    assert!(ack.accepted);
    let committed_mode = proxy.engine().runtime_snapshot();
    assert_eq!(committed_mode.config_revision(), before + 1);
    reconciler.release.notify_one();
    let error = request.await.unwrap().unwrap_err();
    assert!(
        error.contains("cannot publish a snapshot across a committed configuration change"),
        "{error}"
    );
    assert!(Arc::ptr_eq(
        &proxy.engine().runtime_snapshot(),
        &committed_mode
    ));
    assert_eq!(proxy.engine().config_revision(), before + 1);
    assert_eq!(proxy.engine().mode_kind(), "direct");
    running.shutdown().await.unwrap();
}
