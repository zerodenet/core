//! A real ACK must release the same retransmission envelope under exact totals.
use super::*;
use futures_util::FutureExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, OnceLock, Weak,
};
#[tokio::test]
async fn ordinary_wireguard_tcp_ack_releases_full_actual_transmit_cache_without_extra_quota() {
    let memory = Arc::new(OnceLock::<Weak<zero_tun::packet_memory::PacketReturnTotals>>::new());
    let actual_payload = Arc::new(AtomicUsize::new(0));
    let full_at_ack = Arc::new(AtomicUsize::new(0));
    // Bounded read-only observations of actual peer-generated ACKs, including
    // SYN-ACK: the same admission cycle may prevent connect itself completing.
    let ack_snapshots = Arc::new(Mutex::new(Vec::new()));
    let observe = {
        let memory = memory.clone();
        let actual_payload = actual_payload.clone();
        let full_at_ack = full_at_ack.clone();
        let ack_snapshots = ack_snapshots.clone();
        Arc::new(move |from_kernel: bool, bytes: &[u8]| {
            let Some(tcp) = zero_stack::packet::parse_tcp(bytes) else {
                return;
            };
            if from_kernel && !tcp.payload.is_empty() {
                actual_payload.fetch_add(tcp.payload.len(), Ordering::AcqRel);
            }
            if !from_kernel && tcp.ack_flag {
                let root = memory
                    .get()
                    .and_then(Weak::upgrade)
                    .expect("actual same Kernel root");
                let snapshot = root.transmit();
                {
                    let mut snapshots = ack_snapshots.lock().unwrap();
                    if snapshots.len() < 16 {
                        snapshots.push((tcp.syn, tcp.ack, tcp.payload.len(), snapshot));
                    }
                }
                if snapshot.occupied.frames == snapshot.limit.frames {
                    full_at_ack.fetch_add(1, Ordering::AcqRel);
                }
            }
        }) as Arc<peer::InnerObserver>
    };
    let (endpoint, peer_task) = peer::echo_peer_inner(None, Some(observe)).await;
    // Both real originals choose an ephemeral port, on distinct validated
    // addresses. No duplicate listen or assumed free numeric port is used.
    let mut config = configuration_with_source(endpoint, 0, false, "127.0.0.2");
    // The same Source's actual 148-byte control workspace consumes one frame;
    // its real retained IP+padding+cipher envelope consumes the remaining three.
    config.runtime.packet_transmit_total_max_frames = Some(4);
    let input = config.endpoints[0].inbound_tag();
    let proxy = Proxy::new(config.clone()).unwrap();
    let root = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    memory.set(Arc::downgrade(&root)).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let source = proxy.inbound_owners.owner(&input).unwrap();
    source.wait_ready().await.unwrap();
    let original = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    let socks = proxy.inbound_owners.owner("socks").unwrap();
    let port = socks.original_listener_address.get().unwrap().port();
    let diagnostics = || {
        format!(
        "actual peer ACK observations (syn, ack, payload, shared TX): {:?}; payload_from_kernel={}; full_at_ack={}; native={:?}; return={:?}; TX={:?}; Source terminal={:?}",
        ack_snapshots.lock().unwrap(), actual_payload.load(Ordering::Acquire),
        full_at_ack.load(Ordering::Acquire), root.native(), root.returns(), root.transmit(),
        source.terminal_for_test(),
    )
    };
    let mut clients = Vec::new();
    for payload in [
        b"first-real-cached-packet".as_slice(),
        b"same-root-reused-after-ack".as_slice(),
    ] {
        let mut client = match tokio::time::timeout_at(
            end(),
            std::panic::AssertUnwindSafe(connect(port, false)).catch_unwind(),
        )
        .await
        {
            Ok(Ok(client)) => client,
            outcome => panic!(
                "real SOCKS/TCP connect did not complete: {outcome:?}; {}",
                diagnostics()
            ),
        };
        client.write_all(payload).await.unwrap();
        let mut echoed = vec![0; payload.len()];
        tokio::time::timeout_at(end(), client.read_exact(&mut echoed))
            .await
            .unwrap_or_else(|source| {
                panic!(
                    "actual data ACK/echo timed out: {source:?}; {}",
                    diagnostics()
                )
            })
            .unwrap_or_else(|source| {
                panic!("actual data ACK/echo failed: {source:?}; {}", diagnostics())
            });
        assert_eq!(echoed, payload);
        tokio::time::timeout_at(end(), async {
            while root.transmit().occupied.frames != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("only the original control workspace remains charged after actual ACK");
        assert_eq!(root.transmit().limit.frames, 4);
        assert!(!original.driver().unwrap().is_joined());
        assert!(original.closed().is_none());
        clients.push(client);
    }
    assert_eq!(
        clients.len(),
        2,
        "two real business TCP callers use the same actual SOCKS original"
    );
    assert!(actual_payload.load(Ordering::Acquire) >= 48);
    assert!(full_at_ack.load(Ordering::Acquire) > 0,
        "the real encrypted peer generated its ACK while that same retransmission cache exhausted the configured total");
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.transmit().occupied.frames, 0);
    assert!(original
        .driver()
        .unwrap()
        .try_joined()
        .unwrap()
        .registration_released());
    assert!(original.closed().unwrap().matches(&original));
    for mut client in clients {
        let mut byte = [0];
        let stopped = tokio::time::timeout_at(end(), client.read(&mut byte))
            .await
            .unwrap();
        assert!(
            matches!(stopped, Ok(0))
                || stopped.is_err_and(|source| matches!(
                    source.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ))
        );
    }
    peer_task.abort();
    assert!(peer_task.await.is_err_and(|error| error.is_cancelled()));
}
