use super::*;
use std::future::Future;

fn engine() -> zero_engine::Engine {
    zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn cancelled_statistics_poll_and_shutdown_retain_the_same_original_blocking_join() {
    let owner = Arc::new(Sampler::default());
    owner.start(engine()).await.unwrap();
    let (started, running) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let witness = {
        let mut state = owner.state.lock().await;
        let state = state.as_mut().unwrap();
        state
            .register(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
            .unwrap();
        state.pending.clone().unwrap()
    };
    running.await.unwrap();
    let mut poll = Box::pin(owner.next_sample());
    std::future::poll_fn(|cx| {
        assert!(poll.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(poll);
    assert!(!witness.is_joined());
    let mut shutdown = Box::pin(owner.shutdown());
    std::future::poll_fn(|cx| {
        assert!(shutdown.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(shutdown);
    assert!(!witness.is_joined());
    {
        let state = owner.state.lock().await;
        let state = state.as_ref().unwrap();
        assert!(state.closing);
        assert!(!state.closed);
        assert!(state.pending.as_ref().unwrap().same_registration(&witness));
    }
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), owner.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(witness.wait().await.result().is_ok());
    assert!(owner.state.lock().await.as_ref().unwrap().closed);
}

#[tokio::test]
async fn statistics_original_blocking_panic_is_joined_and_retained_for_shutdown() {
    let owner = Sampler::default();
    owner.start(engine()).await.unwrap();
    let witness = {
        let mut state = owner.state.lock().await;
        let state = state.as_mut().unwrap();
        state
            .register(|| panic!("original statistics blocking failure"))
            .unwrap();
        state.pending.clone().unwrap()
    };
    let observed = tokio::time::timeout(Duration::from_secs(1), owner.next_sample())
        .await
        .unwrap()
        .unwrap_err();
    let original = witness.wait().await.result().unwrap_err();
    assert!(Arc::ptr_eq(
        owner.failure.lock().unwrap().as_ref().unwrap(),
        &original
    ));
    let retained = observed
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &original));
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(original.as_ref());
    let mut actual_panic = false;
    while let Some(error) = source {
        actual_panic |= error
            .downcast_ref::<tokio::task::JoinError>()
            .is_some_and(|error| error.is_panic());
        source = error.source();
    }
    assert!(actual_panic);
    let closed = owner.shutdown().await.unwrap_err();
    let retained = closed
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &original));
    assert!(owner.state.lock().await.as_ref().unwrap().closed);
}
