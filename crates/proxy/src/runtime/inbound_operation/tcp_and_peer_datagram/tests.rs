use super::*;
use crate::runtime::Proxy;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::Poll,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::{timeout, Duration, Instant},
};
fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap()
}
async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, accepted) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    (client.unwrap(), accepted.unwrap().0)
}

#[tokio::test]
async fn actual_tcp_failure_stops_peer_admission_and_drains_pending_peer_io_before_driver_exit() {
    let proxy = proxy();
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "peer",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let host = installed.tasks();
    let tasks = installed.peer_tasks();
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let address = socket.local_addr().unwrap();
    let (first, mut first_server) = tcp_pair().await;
    let (second, mut second_server) = tcp_pair().await;
    let stream = Arc::new(Mutex::new(vec![first, second]));
    let started = Arc::new(AtomicUsize::new(0));
    let io_pending = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(Notify::new());
    let peers = Arc::new(AtomicUsize::new(0));
    let work = {
        let (started, io_pending, fail, peers) = (
            started.clone(),
            io_pending.clone(),
            fail.clone(),
            peers.clone(),
        );
        let peer_socket = socket.clone();
        let udp_host = host.clone();
        async move {
            run_both(
                async {
                    fail.notified().await;
                    Err(std::io::Error::other("actual TCP driver failure").into())
                },
                peers::run(socket, shutdown, udp_host, tasks, move |_, mut packets| {
                    peers.fetch_add(1, Ordering::SeqCst);
                    let mut stream = stream
                        .lock()
                        .unwrap()
                        .pop()
                        .expect("two actual admitted peers");
                    let (started, io_pending, socket) =
                        (started.clone(), io_pending.clone(), peer_socket.clone());
                    async move {
                        assert_eq!(packets.recv().await.unwrap().as_slice(), b"first");
                        started.fetch_add(1, Ordering::SeqCst);
                        assert!(
                            packets.recv().await.is_none(),
                            "actual peer input closes before draining"
                        );
                        let mut byte = [0];
                        let read = stream.read(&mut byte);
                        tokio::pin!(read);
                        let mut signalled = false;
                        let read = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                            Poll::Pending => {
                                if !signalled {
                                    io_pending.fetch_add(1, Ordering::SeqCst);
                                    signalled = true;
                                }
                                Poll::Pending
                            }
                            Poll::Ready(read) => Poll::Ready(read),
                        })
                        .await?;
                        assert_eq!(read, 0);
                        drop(socket);
                        Ok(())
                    }
                }),
                host,
            )
            .await
        }
    };
    owner.spawn_driver(work).unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"first", address).await.unwrap();
    let second_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    second_client.send_to(b"first", address).await.unwrap();
    timeout(Duration::from_secs(2), async {
        while started.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fail.notify_one();
    timeout(Duration::from_secs(2), async {
        while io_pending.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut observation = Box::pin(owner.observe_driver());
    std::future::poll_fn(|cx| {
        assert!(observation.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(UdpSocket::bind(address).await.is_err());
    let another = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    another.send_to(b"late", address).await.unwrap();
    assert_eq!(peers.load(Ordering::SeqCst), 2);
    first_server.shutdown().await.unwrap();
    std::future::poll_fn(|cx| {
        assert!(observation.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        UdpSocket::bind(address).await.is_err(),
        "one real peer still owns pending I/O"
    );
    second_server.shutdown().await.unwrap();
    assert!(timeout(Duration::from_secs(2), &mut observation)
        .await
        .unwrap()
        .is_err());
    drop(observation);
    assert!(proxy
        .inbound_owners
        .shutdown("peer", Instant::now() + Duration::from_secs(2))
        .await
        .is_err());
    let _rebound = UdpSocket::bind(address).await.unwrap();
}

#[tokio::test]
async fn actual_peer_panic_stops_both_loops_and_retains_its_real_native_join_id_and_source() {
    let proxy = proxy();
    let (stop, mut stopping) = tokio::sync::watch::channel(false);
    let installed = proxy
        .inbound_owners
        .install(
            "peer-panic",
            &proxy.protocols,
            crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let host = installed.tasks();
    let tasks = installed.peer_tasks();
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let address = socket.local_addr().unwrap();
    let (stream, mut server) = tcp_pair().await;
    let stream = Arc::new(Mutex::new(Some(stream)));
    let native_id = Arc::new(Mutex::new(None));
    let pending = Arc::new(Notify::new());
    let work = {
        let (native_id, pending) = (native_id.clone(), pending.clone());
        let udp_host = host.clone();
        let shutdown = stopping.clone();
        async move {
            run_both(
                async {
                    let _ = stopping.wait_for(|stop| *stop).await;
                    Ok(())
                },
                peers::run(socket, shutdown, udp_host, tasks, move |_, mut packets| {
                    let mut stream = stream.lock().unwrap().take().unwrap();
                    let (native_id, pending) = (native_id.clone(), pending.clone());
                    async move {
                        assert_eq!(packets.recv().await.unwrap().as_slice(), b"panic");
                        *native_id.lock().unwrap() = Some(tokio::task::id());
                        let mut byte = [0];
                        let read = stream.read(&mut byte);
                        tokio::pin!(read);
                        std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                            Poll::Pending => {
                                pending.notify_one();
                                Poll::Pending
                            }
                            Poll::Ready(read) => Poll::Ready(read),
                        })
                        .await?;
                        panic!("actual UDP peer task panic after pending I/O")
                    }
                }),
                host,
            )
            .await
        }
    };
    owner.spawn_driver(work).unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"panic", address).await.unwrap();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    server.write_all(b"x").await.unwrap();
    let error = timeout(Duration::from_secs(2), owner.observe_driver())
        .await
        .unwrap()
        .unwrap_err();
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut joined = None;
    while let Some(error) = source {
        if let Some(error) = error.downcast_ref::<tokio::task::JoinError>() {
            assert!(error.is_panic());
            assert!(error.to_string().contains("actual UDP peer task panic"));
            joined = Some(error.id());
            break;
        }
        source = error.source();
    }
    assert_eq!(
        joined,
        *native_id.lock().unwrap(),
        "actual native peer JoinError survives in the persistent root failure"
    );
    let _rebound = UdpSocket::bind(address).await.unwrap();
}
