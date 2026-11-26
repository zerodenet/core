//! Real native Pending: cancel the inline probe without losing its controller.
use super::*;
use std::{future::Future, task::Poll};
#[tokio::test]
async fn process_endpoint_pending_probe_retains_original_until_native_close_and_actual_actor_ack() {
    let mut last_error = None;
    for _ in 0..5 {
        match try_pending_probe().await {
            Ok(()) => return,
            Err(error) => last_error = Some(error),
        }
    }
    panic!(
        "process endpoint pending probe child lost ephemeral port race repeatedly: {:?}",
        last_error
    );
}
async fn try_pending_probe() -> Result<(), String> {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let handoff = ProcessEndpointHandoff::new(
        instance(723),
        0,
        ProcessEndpointSpec {
            stream: true,
            datagram: false,
            ipv6: false,
            network_generation: NetworkGeneration(0),
        },
        &scope.registrar(),
        CancellationToken::new(),
    )
    .unwrap();
    let process =
        OwnedProcess::prepare(ProcessRequest::new("python3"), &scope.registrar()).unwrap();
    let budget = ProcessBudget::new(ProcessBudgetLimits {
        max_jobs: 1,
        max_jobs_per_instance: 1,
    })
    .unwrap();
    budget.reserve(instance(723), &process).unwrap();
    let (go, begin) = oneshot::channel();
    let (ready, waiting) = oneshot::channel::<Result<(), String>>();
    let (resume, resumed) = oneshot::channel();
    let original = handoff.clone();
    let actual = process.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let actor = begin.await.unwrap();
            actual.start(actor).unwrap();
            actual.wait_started(end()).await.unwrap();
            let deadline = end();
            let mut ready = Some(ready);
            // Queue capacities differ by kernel. Consume only probes that
            // actually completed, then retain the same awaited native connect
            // once a second real poll still observes Pending. The timer only
            // permits I/O progress; it is never evidence of pending readiness.
            loop {
                assert!(Instant::now() < deadline, "native probe never suspended");
                let probe_actor = actual.actor().unwrap();
                let mut probe = Box::pin(original.open_probe(&probe_actor, deadline));
                let completed = tokio::select! {
                    result = probe.as_mut() => Some(result),
                    _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {
                        std::future::poll_fn(|cx| match probe.as_mut().poll(cx) {
                            Poll::Pending => Poll::Ready(None),
                            Poll::Ready(result) => Poll::Ready(Some(result)),
                        }).await
                    },
                };
                if let Some(result) = completed {
                    drop(probe);
                    match result {
                        Ok(mut completed) => {
                            assert!(completed
                                .close(deadline)
                                .await
                                .unwrap()
                                .unwrap()
                                .matches(&completed));
                        }
                        Err(error) => {
                            let closed_probe = original.close_probe(deadline).await.unwrap();
                            if !actual.endpoint_is_running() || closed_probe.is_none() {
                                if let Some(ready) = ready.take() {
                                    let _ = ready.send(Err(error.to_string()));
                                }
                                break;
                            }
                            assert!(Instant::now() < deadline, "{error}");
                            if let Ok(status) = tokio::time::timeout(
                                std::time::Duration::from_millis(5),
                                actual.wait_root_exit(),
                            )
                            .await
                            {
                                if let Some(ready) = ready.take() {
                                    let _ = ready.send(Err(format!(
                                        "pending-probe child exited during startup ({error}): {status:?}"
                                    )));
                                }
                                break;
                            }
                            continue;
                        }
                    }
                    tokio::task::yield_now().await;
                    continue;
                }
                let native = { original.lock().probe.clone().unwrap() };
                assert!(!native.is_closed());
                ready.take().unwrap().send(Ok(())).unwrap();
                resumed.await.unwrap();
                assert!(probe.await.is_err());
                let closed = original.close_probe(deadline).await.unwrap().unwrap();
                assert!(closed.matches_original(&native) && closed.had_native_acquisition());
                assert!(!actual.actor().unwrap().is_joined());
                break;
            }
            actual.request_stop();
            #[cfg(target_os = "linux")]
            actual.wait_tree_ended(deadline).await.unwrap();
            #[cfg(not(target_os = "linux"))]
            actual.wait_child_ended(deadline).await.unwrap();
            Ok(())
        })
        .unwrap();
    process.bind_actor(witness.clone()).unwrap();
    process.attach_endpoint(&handoff).unwrap();
    let address = handoff.acquire(&scope.registrar(), &witness).unwrap();
    let receipt = handoff.close_reservations(end()).await.unwrap();
    assert!(receipt.matches(&handoff));
    assert_eq!((receipt.tcp_originals, receipt.udp_originals), (1, 0));
    let mut request = ProcessRequest::new("python3");
    request.arguments=vec!["-c".into(),"import socket,sys,signal;s=socket.socket();s.bind((sys.argv[1],int(sys.argv[2])));s.listen(1);signal.pause()".into(),address.ip().to_string().into(),address.port().to_string().into()];
    process.install_endpoint_request(request, &handoff).unwrap();
    go.send(witness.clone()).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(20), waiting)
        .await
        .unwrap()
        .unwrap();
    if let Err(error) = outcome {
        handoff.request_stop();
        let (native, tasks) = tokio::join!(process.wait_closed(end()), scope.shutdown(end()));
        assert!(tasks.is_complete(), "{tasks:?}");
        assert!(witness.is_joined());
        assert!(native.unwrap().unwrap().matches(&process));
        assert!(handoff.closed().unwrap().matches(&handoff));
        assert_eq!(budget.snapshot().charged_jobs, 0);
        return Err(error);
    }
    assert_eq!(budget.snapshot().charged_jobs, 1);
    handoff.request_stop();
    assert!(process.closed().is_none());
    resume.send(()).unwrap();
    let (native, tasks) = tokio::join!(process.wait_closed(end()), scope.shutdown(end()));
    assert!(tasks.is_complete(), "{tasks:?}");
    assert!(witness.is_joined());
    assert!(native.unwrap().unwrap().matches(&process));
    assert!(handoff.closed().unwrap().matches(&handoff));
    assert_eq!(budget.snapshot().charged_jobs, 0);
    Ok(())
}
