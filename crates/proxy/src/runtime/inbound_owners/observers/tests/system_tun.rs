//! Planned System TUN observer completion is distinct from listener failure.
use super::*;

#[tokio::test]
async fn requested_system_source_stop_keeps_steady_supervision_waiting_after_original_join() {
    timeout(Duration::from_secs(5), async {
        let proxy = super::super::super::bind::tests::proxy();
        let mut observers = proxy.inbound_owners.take_listener_observers().unwrap();
        let (stop, _) = watch::channel(false);
        let installed = proxy
            .inbound_owners
            .install_listener("planned-system-source", &proxy.protocols, 1, stop)
            .unwrap();
        let owner = installed.owner();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut remote = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut io, _) = listener.accept().await.unwrap();
        let (entered, pending) = tokio::sync::oneshot::channel();
        owner
            .spawn_driver(async move {
                let mut entered = Some(entered);
                let mut byte = [0];
                let read = io.read(&mut byte);
                tokio::pin!(read);
                let size = std::future::poll_fn(|cx| {
                    let result = read.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(entered) = entered.take() {
                            let _ = entered.send(());
                        }
                    }
                    result
                })
                .await?;
                assert_eq!(size, 0);
                Ok(())
            })
            .unwrap();
        owner.seal_listener_installation();
        pending.await.unwrap();
        let original = owner.task_access().source_main_witness().await.unwrap();
        proxy
            .inbound_owners
            .observers
            .enqueue_system_tun(owner.clone(), None)
            .unwrap();
        let mut waiting = Box::pin(observers.join_next_running());
        assert!(futures_util::poll!(&mut waiting).is_pending());
        owner.task_access().request_stop();
        remote.shutdown().await.unwrap();
        let joined = tokio::select! {
            result = &mut waiting => panic!("planned original became a listener failure: {result:?}"),
            joined = original.wait_registration_released() => joined,
        };
        assert!(original.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        drop(waiting);
        loop {
            let mut waiting = Box::pin(observers.join_next_running());
            assert!(futures_util::poll!(&mut waiting).is_pending());
            drop(waiting);
            if observers.is_empty() { break; }
            tokio::task::yield_now().await;
        }
        proxy
            .inbound_owners
            .confirm_shutdown(
                "planned-system-source",
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
            .unwrap();
    })
    .await
    .unwrap();
}
