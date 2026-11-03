use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use zero_config::RuntimeConfig;

use super::{run_tcp_listener_loop, TcpListenerLoopRequest};
use crate::runtime::route_runtime::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices};

fn test_runtime_factory(inbound_tag: &str) -> InboundRouteRuntimeFactory {
    let config =
        RuntimeConfig::parse(r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#)
            .expect("minimal runtime config");
    let proxy = crate::runtime::Proxy::new(config).expect("minimal proxy");
    InboundRouteRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        inbound_tag.to_owned(),
    )
}

#[tokio::test]
async fn tcp_listener_accepts_connection_and_stops_on_shutdown() {
    let listener = zero_platform_tokio::TokioListener::bind("127.0.0.1:0")
        .await
        .expect("test listener");
    let listen_addr = listener.local_addr().expect("listener address");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let accepted = Arc::new(Notify::new());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let task = {
        let accepted = accepted.clone();
        let observations = observations.clone();
        tokio::spawn(async move {
            run_tcp_listener_loop(TcpListenerLoopRequest {
                runtime_factory: test_runtime_factory("listener-test"),
                protocol_name: "test",
                listener,
                shutdown: shutdown_rx,
                handler: move |runtime: crate::runtime::route_runtime::InboundRouteRuntime, _| {
                    let accepted = accepted.clone();
                    let observations = observations.clone();
                    async move {
                        observations
                            .lock()
                            .expect("observations lock")
                            .push((runtime.inbound_tag().to_owned(), runtime.source_addr()));
                        accepted.notify_one();
                    }
                },
            })
            .await
        })
    };

    let client = tokio::net::TcpStream::connect(listen_addr)
        .await
        .expect("connect listener");
    let client_addr = client.local_addr().expect("client address");
    tokio::time::timeout(Duration::from_secs(2), accepted.notified())
        .await
        .expect("handler was not invoked");

    shutdown_tx.send(true).expect("send listener shutdown");
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("listener did not stop")
        .expect("listener task panicked");
    result.expect("listener loop failed");
    drop(client);

    let observations = observations.lock().expect("observations lock");
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].0, "listener-test");
    assert_eq!(observations[0].1, Some(client_addr));
}

#[cfg(feature = "udp-runtime")]
#[tokio::test]
async fn tcp_listener_waits_for_claimed_udp_dispatch_before_aborting_connections() {
    use std::{
        future::poll_fn,
        sync::atomic::{AtomicBool, Ordering},
        task::Poll,
    };
    let listener = zero_platform_tokio::TokioListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = watch::channel(false);
    let pending = Arc::new(Notify::new());
    let finishing = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let finished = Arc::new(AtomicBool::new(false));
    let mut task = {
        let (pending, finishing, release, finished) = (
            pending.clone(),
            finishing.clone(),
            release.clone(),
            finished.clone(),
        );
        tokio::spawn(run_tcp_listener_loop(TcpListenerLoopRequest {
            runtime_factory: test_runtime_factory("udp-stop"),
            protocol_name: "test",
            listener,
            shutdown: stop_rx,
            handler: move |runtime: crate::runtime::route_runtime::InboundRouteRuntime, _| {
                let (pending, finishing, release, finished) = (
                    pending.clone(),
                    finishing.clone(),
                    release.clone(),
                    finished.clone(),
                );
                async move {
                    let guard = runtime
                        .udp_runtime()
                        .claim_listener_stop()
                        .unwrap()
                        .unwrap();
                    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
                    let mut bytes = [0; 8];
                    let mut read = tokio::io::ReadBuf::new(&mut bytes);
                    poll_fn(|cx| {
                        assert!(socket.poll_recv_from(cx, &mut read).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    pending.notify_one();
                    tokio::select! {
                        _ = socket.recv_from(&mut bytes) => panic!("no peer sent a packet"),
                        _ = guard.cancelled() => {}
                    }
                    finishing.notify_one();
                    release.notified().await;
                    finished.store(true, Ordering::Release);
                    drop(guard);
                }
            },
        }))
    };
    let _client = tokio::net::TcpStream::connect(address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), finishing.notified())
        .await
        .unwrap();
    poll_fn(|cx| {
        assert!(std::future::Future::poll(std::pin::Pin::new(&mut task), cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(!finished.load(Ordering::Acquire));
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(finished.load(Ordering::Acquire));
}
