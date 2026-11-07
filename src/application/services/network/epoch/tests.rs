use super::*;
use std::future::Future;
use tokio::{io::AsyncReadExt, sync::Notify};
use zero_platform_tokio::EgressInterfaceControl;
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
fn epoch(generation: u64, limits: EpochLimits) -> Arc<Epoch> {
    let preparation = zero_proxy::ProxyPreparation::new();
    let (instance, totals) = preparation.application_network_originals().unwrap();
    Epoch::new(generation, limits, instance, totals).unwrap()
}
async fn connected(
    epoch: &Arc<Epoch>,
) -> (super::super::stream::EventStream, tokio::net::TcpStream) {
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
    let epoch = epoch(
        EgressInterfaceControl::default().generation(),
        EpochLimits::default(),
    );
    let (stream, mut peer) = connected(&epoch).await;
    let original = epoch.originals.lock().unwrap()[0].clone();
    assert!(
        original
            .custody
            .users
            .lock()
            .unwrap()
            .iter()
            .all(TaskJoinWitness::is_joined)
    );
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
    let epoch = epoch(
        EgressInterfaceControl::default().generation(),
        EpochLimits::default(),
    );
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
    assert!(
        !original
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .closed_io
            .is_empty()
    );
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
}

#[tokio::test]
async fn bootstrap_http_uses_the_same_original_task_users_and_native_controller_receipt() {
    use super::super::{ApplicationEventSinkTcpDialer, download::DownloadEvent};
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let network = ApplicationEventSinkTcpDialer::for_test(EgressInterfaceControl::default());
    network.begin_generation().unwrap();
    let epoch = network.current().unwrap();
    let (entered, headers) = tokio::sync::oneshot::channel();
    let (release, response) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(stream.read_u8().await.unwrap());
        }
        entered.send(()).unwrap();
        response.await.unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nreal")
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(stream.read(&mut byte).await.unwrap(), 0);
    });
    let mut download = network
        .download(format!("http://{address}/rules").parse().unwrap())
        .unwrap();
    tokio::select! {
        result = headers => result.unwrap(),
        event = download.next() => panic!("HTTP returned before the actual held response: {}", event.is_ok()),
    }
    let original = epoch.originals.lock().unwrap()[0].clone();
    assert!(
        original.receipt.lock().unwrap().is_none(),
        "actual HTTP response is still pending, with its original native IO retained"
    );
    assert!(original.adoption.closed().is_none());
    release.send(()).unwrap();
    let mut actual = Vec::new();
    while let Some(event) = download.next().await.unwrap() {
        match event {
            DownloadEvent::Data(bytes) => actual.extend_from_slice(&bytes),
            DownloadEvent::Redirect(_) => panic!("actual origin did not redirect"),
        }
    }
    assert_eq!(actual, b"real");
    drop(download);
    network.retire_generation(end()).await.unwrap();
    assert!(epoch.closed());
    let closed = original.adoption.closed().unwrap();
    assert!(closed.matches(&original.adoption) && closed.had_native_acquisition());
    assert!(original.custody.returned_and_joined());
    assert!(
        original
            .custody
            .users
            .lock()
            .unwrap()
            .iter()
            .all(TaskJoinWitness::is_joined)
    );
    assert!(
        !original
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .closed_io
            .is_empty()
    );
    peer.await.unwrap();
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
    let dialer = Arc::new(ApplicationEventSinkTcpDialer::for_test(
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
    assert!(
        !original
            .receipt
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .closed_io
            .is_empty()
    );
    let mut byte = [0];
    assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    // Same original epoch is closed, never silently revived by a new caller.
    assert!(
        dialer
            .connect(address.ip().to_string(), address.port())
            .await
            .is_err()
    );
}

mod lookup;

mod retirement;
