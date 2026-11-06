use super::*;
use std::{future::Future, task::Poll, time::Duration};

#[tokio::test]
async fn cancelled_tun_join_retains_the_original_handle_after_body_notification() {
    let (done, notification) = oneshot::channel();
    let completion = TunCompletion::new(notification, None);
    let (release, released) = oneshot::channel();
    let (pending, suspended) = oneshot::channel();
    completion.install_main(tokio::spawn(async move {
        done.send(()).unwrap();
        let mut released = Box::pin(released);
        let mut pending = Some(pending);
        std::future::poll_fn(|cx| match released.as_mut().poll(cx) {
            Poll::Ready(value) => Poll::Ready(value),
            Poll::Pending => {
                if let Some(pending) = pending.take() {
                    pending.send(()).unwrap();
                }
                Poll::Pending
            }
        })
        .await
        .unwrap();
        Ok(())
    }));
    suspended.await.unwrap();
    let id = completion.main.lock().await.task.as_ref().unwrap().id();
    let mut joining = Box::pin(completion.wait_main(Instant::now() + Duration::from_secs(1)));
    std::future::poll_fn(|cx| {
        assert!(joining.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(joining);
    {
        let retained = completion.main.lock().await;
        assert!(retained.notified.as_ref().unwrap().is_ok());
        assert_eq!(retained.task.as_ref().unwrap().id(), id);
        assert!(retained.joined.is_none());
    }
    release.send(()).unwrap();
    completion
        .wait_main(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    completion
        .wait_main(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(completion.main.lock().await.task.is_none());
}

#[tokio::test]
async fn original_tun_panic_is_retained_alongside_missing_notification() {
    let (done, notification) = oneshot::channel();
    let completion = TunCompletion::new(notification, None);
    completion.install_main(tokio::spawn(async move {
        drop(done);
        panic!("original TUN task");
    }));
    for _ in 0..2 {
        let error = completion
            .wait_main(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        let failures = error.get_ref().unwrap().downcast_ref::<Failures>().unwrap();
        assert_eq!(failures.0.len(), 2);
        assert!(failures.0[1]
            .downcast_ref::<JoinError>()
            .unwrap()
            .is_panic());
    }
}

#[tokio::test]
async fn exited_main_failure_is_reported_while_its_quiescent_control_is_reclaimed() {
    let proxy = crate::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap();
    let (done, notification) = oneshot::channel();
    let completion = TunCompletion::new(notification, None);
    completion.install_main(tokio::spawn(async move {
        done.send(()).unwrap();
        Err(zero_engine::EngineError::Io(io::Error::other(
            "actual main runtime failure",
        )))
    }));
    let (shutdown, _) = tokio::sync::watch::channel(false);
    *proxy.tun_control.lock().unwrap() = Some(crate::runtime::TunControl {
        id: 812,
        shutdown,
        completion: completion.clone(),
        #[cfg(feature = "raw-ip-runtime")]
        budget_recovery: None,
        route_recovery: None,
    });
    let error = proxy.stop_tun_if_running().await.unwrap_err();
    assert!(error.to_string().contains("actual main runtime failure"));
    assert!(proxy.tun_control.lock().unwrap().is_none());
    let retained = completion.main.lock().await;
    assert!(retained.task.is_none());
    assert!(retained
        .joined
        .as_ref()
        .unwrap()
        .as_ref()
        .unwrap_err()
        .downcast_ref::<zero_engine::EngineError>()
        .is_some());
}
