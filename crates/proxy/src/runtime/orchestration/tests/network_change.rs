use super::*;
use std::{future::Future, task::Poll};

#[tokio::test]
async fn accepted_apply_keeps_network_replacement_pending_through_ready_and_cancelled_wait() {
    let serial = Arc::new(Mutex::new(()));
    let owner = Arc::new(zero_runtime::ApplyOwner::default());
    let egress = zero_platform_tokio::EgressInterfaceControl::default();
    let mut changes = egress.subscribe_generation();
    let before = egress.generation();
    let (ready, accepted) = tokio::sync::oneshot::channel();
    let (commit, finish) = tokio::sync::oneshot::channel();
    let applying = tokio::spawn({
        let owner = owner.clone();
        let serial = serial.clone();
        async move {
            owner
                .run(serial, async move {
                    ready.send(()).unwrap();
                    finish.await.unwrap();
                    Ok(())
                })
                .await
                .unwrap();
        }
    });
    accepted.await.unwrap();
    let mut network = Box::pin(wait(&serial, &mut changes, before, None));
    std::future::poll_fn(|cx| {
        assert!(network.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    egress.invalidate_network();
    std::future::poll_fn(|cx| {
        assert!(network.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(network);
    assert!(
        !changes.has_changed().unwrap(),
        "the real watch wake was consumed"
    );
    assert_ne!(
        *changes.borrow(),
        before,
        "its original generation still owes replacement"
    );
    let mut resumed = Box::pin(wait(&serial, &mut changes, before, None));
    std::future::poll_fn(|cx| {
        assert!(resumed.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    commit.send(()).unwrap();
    applying.await.unwrap();
    let original_gate = resumed.await;
    assert!(serial.try_lock().is_err());
    drop(original_gate);
    owner.wait_idle().await.unwrap();
    owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn inbound_dial_with_failing_node_dns_completes_session_error_without_poisoning_listener_or_exiting_runtime(
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Hold a bound UDP socket that never replies (or drop right before? No: bind
    // a real TCP listener on 127.0.0.1:0 for a second healthy route, and point
    // node DNS to a closed local TCP/DoH endpoint so NodeService::resolve fails
    // immediately with `Dial: Io (Session, NotSubmitted)`).
    let echo_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    let echo_task = tokio::spawn(async move {
        let (mut socket, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 4];
        socket.read_exact(&mut buf).await.unwrap();
        socket.write_all(&buf).await.unwrap();
    });

    // Reserve a closed TCP port on loopback by binding and immediately closing
    // a listener — connecting to it yields ECONNREFUSED inside DoH Node DNS,
    // producing `Dial: Io (Session, NotSubmitted)` without `UpstreamRejection`.
    let closed_doh_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let socks_reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let socks_addr = socks_reserved.local_addr().unwrap();

    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "runtime": {
                "dns": {
                    "servers": {
                        "broken_doh": {
                            "type": "doh",
                            "host": "127.0.0.1",
                            "port": closed_doh_port,
                            "server_name": "localhost",
                            "path": "/dns-query"
                        }
                    },
                    "default_server": "broken_doh",
                    "policy": {
                        "node_server": "broken_doh",
                        "timeout_ms": 1000
                    }
                }
            },
            "inbounds": [{
                "tag": "socks-in",
                "listen": { "address": "127.0.0.1", "port": socks_addr.port() },
                "protocol": { "type": "socks5" }
            }],
            "outbounds": [{
                "tag": "domain-out",
                "protocol": {
                    "type": "socks5",
                    "server": "unreachable-node.example",
                    "port": 1080
                }
            }],
            "route": {
                "rules": [{
                    "condition": { "type": "ip", "values": ["127.0.0.1/32"] },
                    "action": { "type": "direct" }
                }],
                "final": { "type": "route", "outbound": "domain-out" }
            }
        })
        .to_string(),
    )
    .unwrap();

    let proxy = crate::Proxy::new(config.clone()).unwrap();
    drop(socks_reserved);
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, std::time::Duration::from_secs(5))
        .await
        .unwrap();

    let inbound_owner = proxy.inbound_owners.owner("socks-in").unwrap();
    let prior_join = inbound_owner
        .accepted_join_for_test()
        .map(|joined| joined.id());

    // 1. Connect SOCKS5 client requesting domain `example.org:80`, routed to `domain-out`,
    //    whose outbound setup task fails in `NodeService::resolve` with `Dial: Io (Session, NotSubmitted)`.
    let mut client = tokio::net::TcpStream::connect(socks_addr).await.unwrap();
    client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut greeting = [0_u8; 2];
    client.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [0x05, 0x00]);
    let host = b"example.org";
    let mut req = vec![0x05, 0x01, 0x00, 0x03, host.len() as u8];
    req.extend_from_slice(host);
    req.extend_from_slice(&80_u16.to_be_bytes());
    client.write_all(&req).await.unwrap();
    let mut reply = [0_u8; 10];
    let _ = client.read(&mut reply).await;
    drop(client);

    // Wait for session completion and peer cleanup pass on `socks-in`.
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if !proxy.engine().completed_sessions().is_empty()
                && inbound_owner
                    .accepted_join_for_test()
                    .is_some_and(|joined| Some(joined.id()) != prior_join)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed dial session must complete and retire cleanly");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(
        inbound_owner.is_running() && inbound_owner.terminal_for_test().is_none(),
        "session-level Node DNS dial failure must never poison InboundOwner"
    );

    // 2. Verify the same proxy & SOCKS5 listener are still healthy by relaying to `echo_addr`.
    let mut client2 = tokio::net::TcpStream::connect(socks_addr).await.unwrap();
    client2.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    client2.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [0x05, 0x00]);
    let mut req2 = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    req2.extend_from_slice(&echo_addr.port().to_be_bytes());
    client2.write_all(&req2).await.unwrap();
    let mut connect_reply = [0_u8; 10];
    client2.read_exact(&mut connect_reply).await.unwrap();
    assert_eq!(connect_reply[1], 0x00);
    client2.write_all(b"ping").await.unwrap();
    let mut pong = [0_u8; 4];
    client2.read_exact(&mut pong).await.unwrap();
    assert_eq!(&pong, b"ping");
    drop(client2);
    echo_task.await.unwrap();

    running.shutdown().await.unwrap();
    let terminal = inbound_owner.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none());
    assert!(terminal.task_failures().is_empty());
    assert!(terminal.closed_io_receipts() >= 1);
}
