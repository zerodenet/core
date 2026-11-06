//! Real native Pending: cancel the inline probe without losing its controller.
use super::*;
use std::{future::Future, task::Poll};
#[tokio::test]
async fn process_endpoint_pending_probe_retains_original_until_native_close_and_actual_actor_ack() {
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
    let (ready, waiting) = oneshot::channel();
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
            // Fill the actual server's unconsumed listen queue through genuine
            // connects. Each previous probe closes its original before the next.
            for _ in 0..2 {
                loop {
                    match original
                        .open_probe(&actual.actor().unwrap(), deadline)
                        .await
                    {
                        Ok(mut probe) => {
                            assert!(
                                probe
                                    .close(deadline)
                                    .await
                                    .unwrap()
                                    .unwrap()
                                    .matches(&probe)
                            );
                            break;
                        }
                        Err(error) => {
                            assert!(Instant::now() < deadline, "{error}");
                            assert!(original.close_probe(deadline).await.unwrap().is_some());
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
            let probe_actor = actual.actor().unwrap();
            let mut probe = Box::pin(original.open_probe(&probe_actor, deadline));
            std::future::poll_fn(|cx| {
                assert!(
                    probe.as_mut().poll(cx).is_pending(),
                    "actual native TCP connect must suspend"
                );
                Poll::Ready(())
            })
            .await;
            let native = { original.lock().probe.clone().unwrap() };
            assert!(!native.is_closed());
            ready.send(()).unwrap();
            // Suspend only after the real native Pending, so cancellation cannot
            // be replaced by a fake unopened future or a delayed marker.
            resumed.await.unwrap();
            assert!(probe.await.is_err());
            let closed = original.close_probe(deadline).await.unwrap().unwrap();
            assert!(closed.matches_original(&native) && closed.had_native_acquisition());
            assert!(!actual.actor().unwrap().is_joined());
            actual.request_stop();
            actual.wait_tree_ended(deadline).await.unwrap();
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
    tokio::time::timeout_at(end(), waiting)
        .await
        .unwrap()
        .unwrap();
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
}
