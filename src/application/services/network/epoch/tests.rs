use super::*;
use std::future::Future;
use tokio::{io::AsyncReadExt, sync::Notify};
use zero_connector::EventSinkTcpStream;
use zero_platform_tokio::EgressInterfaceControl;
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
async fn connected(epoch: &Arc<Epoch>) -> (Box<dyn EventSinkTcpStream>, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let egress = EgressInterfaceControl::default();
    let stream = tokio::select! {
        result=epoch.connect(peer,&egress,end()) => result.unwrap(),
        _=epoch.drive()=>panic!("original scope unexpectedly ended"),
    };
    (stream, listener.accept().await.unwrap().0)
}
#[tokio::test]
async fn joined_connect_and_cancellation_do_not_close_a_held_transfer_view() {
    let epoch = Epoch::new(EgressInterfaceControl::default().generation());
    let (stream, mut peer) = connected(&epoch).await;
    let original = epoch.originals.lock().unwrap()[0].clone();
    assert!(original
        .custody
        .users
        .lock()
        .unwrap()
        .iter()
        .all(TaskJoinWitness::is_joined));
    assert!(epoch.retire(end()).await.is_err());
    assert!(!epoch.closed());
    assert!(original.receipt.lock().unwrap().is_none());
    let mut byte = [0];
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), peer.read(&mut byte))
            .await
            .is_err(),
        "actual peer remains open before native receipt"
    );
    assert!(epoch.spawn(Box::pin(async {})).is_err());
    drop(stream);
    epoch.retire(end()).await.unwrap();
    assert!(epoch.closed());
    assert!(
        !original
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .closed_io
            .is_empty(),
        "same original native close report retained"
    );
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}
#[tokio::test]
async fn pending_http_user_requires_real_scope_join_before_original_close() {
    let epoch = Epoch::new(EgressInterfaceControl::default().generation());
    let (mut stream, mut peer) = connected(&epoch).await;
    let original = epoch.originals.lock().unwrap()[0].clone();
    let entered = Arc::new(Notify::new());
    let notice = entered.clone();
    let witness = epoch
        .spawn(Box::pin(async move {
            let mut byte = [0];
            let read = stream.read(&mut byte);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    notice.notify_one();
                }
                result
            })
            .await
            .ok();
        }))
        .unwrap();
    entered.notified().await;
    epoch.stop.cancel();
    // The original supervisor has not consumed this task's exit yet.
    assert!(!witness.is_joined());
    epoch.close_returned(end()).await.unwrap();
    assert!(original.receipt.lock().unwrap().is_none());
    let report = epoch.scope.lock().await.shutdown(end()).await;
    assert!(report.is_complete());
    assert!(witness.is_joined());
    assert!(witness.try_joined().unwrap().result().is_ok());
    assert!(
        original.receipt.lock().unwrap().is_none(),
        "TaskJoined alone is not native close"
    );
    epoch.retire(end()).await.unwrap();
    assert!(!original
        .receipt
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .closed_io
        .is_empty());
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}

#[cfg(feature = "connector")]
#[tokio::test]
async fn actual_webhook_http_drivers_join_before_the_same_original_native_close() {
    use super::super::ApplicationEventSinkTcpDialer;
    use tokio::io::AsyncWriteExt;
    use zero_connector::{EventDispatcherNetwork, EventDispatcherOptions, EventSinkTcpDialer};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "route":{"final":{"type":"direct"}},
            "api":{"event_sinks":[{"type":"webhook","tag":"actual-http",
                "url":format!("http://{address}/events"),"allow_insecure":true,"events":["engine.warning"]}]}
        })
        .to_string(),
    )
    .unwrap();
    let engine = zero_engine::Engine::new(config.clone()).unwrap();
    let dialer = Arc::new(ApplicationEventSinkTcpDialer::new(
        EgressInterfaceControl::default(),
    ));
    let network = EventDispatcherNetwork::new(dialer.clone());
    network.begin_generation().unwrap();
    let dispatcher = zero_connector::spawn_event_dispatcher_with_network(
        engine.clone(),
        config.api,
        None,
        EventDispatcherOptions::default(),
        network.clone(),
    )
    .unwrap()
    .unwrap();
    engine.emit_warning("actual-http-original", "actual-http-original");
    let mut peer = tokio::time::timeout_at(end(), listener.accept())
        .await
        .unwrap()
        .unwrap()
        .0;
    let mut request = Vec::new();
    tokio::time::timeout_at(end(), async {
        loop {
            let mut chunk = [0u8; 2048];
            let length = peer.read(&mut chunk).await.unwrap();
            assert_ne!(length, 0, "actual HTTP request unexpectedly closed");
            request.extend_from_slice(&chunk[..length]);
            if let Some(split) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..split]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .expect("actual Full body content length");
                if request.len() >= split + 4 + length {
                    break;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(String::from_utf8_lossy(&request).contains("actual-http-original"));
    peer.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    tokio::time::timeout_at(end(), async {
        while dispatcher.sink_status()[0].total_delivered != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let epoch = dialer.current().unwrap();
    let original = epoch.originals.lock().unwrap()[0].clone();
    let witnesses = original.custody.users.lock().unwrap().clone();
    assert!(
        witnesses.len() >= 2,
        "real connect and HTTP I/O tasks retained separately"
    );
    assert!(witnesses.iter().any(|witness| !witness.is_joined()));
    assert!(original.receipt.lock().unwrap().is_none());
    dispatcher.shutdown().await;
    network.retire_generation(end()).await.unwrap();
    assert!(witnesses.iter().all(TaskJoinWitness::is_joined));
    assert!(!original
        .receipt
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .closed_io
        .is_empty());
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    // Same original epoch is closed, never silently revived by a new caller.
    assert!(dialer
        .connect(address.ip().to_string(), address.port())
        .await
        .is_err());
}
