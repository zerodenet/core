//! Ordinary Source admission and acknowledged replacement over real TCP2022 IO.
use std::{future::Future, sync::Arc, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod phase;
use phase::Phase;

const METHOD: &str = "2022-blake3-aes-128-gcm";
const PASSWORD: &str = "MDEyMzQ1Njc4OWFiY2RlZg==";

async fn client(
    endpoint: std::net::SocketAddr,
    target: std::net::SocketAddr,
) -> ::shadowsocks::ShadowsocksAeadStream<zero_platform_tokio::TokioSocket> {
    let mut socket =
        zero_platform_tokio::TokioSocket::new(TcpStream::connect(endpoint).await.unwrap());
    let config = ::shadowsocks::ShadowsocksTcpConnectConfig::from_config(METHOD, PASSWORD).unwrap();
    let session = config
        .establish_tcp_to(
            &mut socket,
            &zero_core::request::Destination::new(
                zero_core::Address::Ipv4([127, 0, 0, 1]),
                target.port(),
            ),
        )
        .await
        .unwrap();
    let mut stream = config.wrap_outbound_stream(socket, session);
    stream.write_all(b"salt").await.unwrap();
    stream.flush().await.unwrap();
    stream
}

#[tokio::test]
async fn ordinary_source_projects_total_and_replaces_originals_on_runtime_only_budget_change() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let reserved = crate::socket_fixture::tcp_udp();
        let endpoint = reserved.0.local_addr().unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"ss_tcp_replay_max_salts":1},
            "inbounds":[{"tag":"salt-source", "listen":{"address":"127.0.0.1","port":endpoint.port()},
                "protocol":{"type":"shadowsocks", "cipher":METHOD, "password":PASSWORD}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        let registry = proxy.system_tun_source_registry();
        let mut phase = Phase::new("pure Source", &proxy);
        phase.set("await original Source registry and readiness");
        drop(reserved);
        let running = proxy.spawn();
        let original = loop {
            if let Some(owner) = registry.owner("salt-source") { break owner; }
            tokio::task::yield_now().await;
        };
        phase.watch(original.clone());
        original.wait_ready().await.unwrap();
        phase.set("first actual encrypted request and Direct target accept");
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut first = client(endpoint, target.local_addr().unwrap()).await;
        let (mut accepted, _) = target.accept().await.unwrap();
        let mut bytes = [0;4]; accepted.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"salt");
        accepted.write_all(b"seen").await.unwrap();
        first.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"seen");
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        let mut busy = (*proxy.engine().config()).clone();
        busy.runtime.ss_tcp_replay_max_salts = Some(2);
        phase.set("active original user: await cold Busy rejection");
        assert!(handle.apply_runtime_config_and_wait(busy, Duration::from_secs(5)).await.is_err(),
            "pure Source budget change cannot stop an already admitted original user");
        assert!(Arc::ptr_eq(&original, &registry.owner("salt-source").unwrap()));
        phase.set("Busy rejected: both directions still use original wire");
        first.write_all(b"stay").await.unwrap(); accepted.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"stay");
        accepted.write_all(b"same").await.unwrap(); first.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"same");
        drop(first); drop(accepted);
        phase.set("original user dropped: await actual Source main cleanup idle");
        loop { if original.accepted_join_for_test().is_some() && original.acquisition_idle_for_test() { break; } tokio::task::yield_now().await; }
        let mut next = (*proxy.engine().config()).clone();
        next.runtime.ss_tcp_replay_max_salts = Some(2);
        phase.set("idle growth: await StopOld and replacement ACK");
        handle.apply_runtime_config_and_wait(next.clone(), Duration::from_secs(5)).await.unwrap();
        let closed = original.terminal_for_test().expect("old source closed before runtime-only budget ACK");
        assert!(closed.failure().is_none());
        assert!(closed.closed_io_receipts() >= 4, "both original listeners and the accepted TCP/egress originals");
        phase.set("growth ACK: old Source terminal/native facts");
        let current = registry.owner("salt-source").unwrap();
        phase.watch(current.clone());
        assert_ne!(current.identity(), original.identity());
        current.wait_ready().await.unwrap();
        phase.set("replacement Source second actual encrypted request");
        let second = client(endpoint, target.local_addr().unwrap()).await;
        let (mut accepted, _) = target.accept().await.unwrap();
        accepted.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"salt");
        drop(second); drop(accepted);
        let mut too_small = next;
        too_small.runtime.ss_tcp_replay_max_salts = Some(1);
        phase.set("retained security debt: await shrink rejection");
        assert!(handle.apply_runtime_config_and_wait(too_small, Duration::from_secs(5)).await.is_err(),
            "two still-valid accepted salts forbid shrink to one");
        assert!(Arc::ptr_eq(&current, &registry.owner("salt-source").unwrap()));
        assert!(current.is_running());
        let mut removed = (*proxy.engine().config()).clone();
        removed.inbounds.clear(); removed.runtime.ss_tcp_replay_max_salts = Some(1);
        phase.set("removed consumers: await retained debt rejection");
        assert!(handle.apply_runtime_config_and_wait(removed, Duration::from_secs(5)).await.is_err(),
            "removing all consumers cannot discard two still-valid security salts");
        assert!(Arc::ptr_eq(&current, &registry.owner("salt-source").unwrap()));
        assert_eq!(proxy.engine().config().runtime.effective_ss_tcp_replay_max_salts(), 2);
        // A third authenticated connection has free native admission, but no
        // salt entry. Its real target accept must stay Pending through failure.
        let pending = target.accept(); tokio::pin!(pending);
        std::future::poll_fn(|cx| { assert!(pending.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
        phase.set("third actual request must not acquire a Direct target");
        let mut third = client(endpoint, target.local_addr().unwrap()).await;
        let result = third.read(&mut bytes).await;
        assert!(matches!(result, Ok(0)) || matches!(result, Err(ref error) if
            matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof)));
        std::future::poll_fn(|cx| { assert!(pending.as_mut().poll(cx).is_pending()); Poll::Ready(()) }).await;
        drop(third);
        phase.set("final original graph shutdown and matching receipts");
        running.shutdown().await.unwrap();
        phase.set("all original cleanup complete");
        let closed = current.terminal_for_test().expect("same replacement Source's final original receipts");
        assert!(closed.closed_io_receipts() >= 5, "original listeners, both accepted sockets and successful Direct original");
        assert!(closed.joined_tasks() >= 2, "source plus exact replay maintenance Scope task joined");
    }).await.unwrap();
}

#[cfg(feature = "socks5")]
#[path = "both_budget.rs"]
mod both_budget;

#[path = "udp_budget.rs"]
mod udp_budget;
