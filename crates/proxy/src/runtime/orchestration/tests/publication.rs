//! Actual native sources reach Ready under the same pending host snapshot.
//! Business traffic stays fenced until the production final commit routine.
use super::super::state::OrchestrationState;
use crate::runtime::{PendingReloadAck, Proxy};
use crate::ProxyHandle;
use std::{future::Future, sync::Arc, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use zero_config::RuntimeConfig;
use zero_engine::{EngineHandle, EngineRuntimeSnapshot};

#[cfg(feature = "wireguard")]
#[path = "publication/packet.rs"]
mod packet;
fn config(inbounds: serde_json::Value) -> RuntimeConfig {
    RuntimeConfig::parse(
        &serde_json::json!({"inbounds":inbounds,
        "route":{"final":{"type":"direct"}}})
        .to_string(),
    )
    .unwrap()
}
fn direct(tag: &str, listen: &str, target: std::net::SocketAddr) -> serde_json::Value {
    serde_json::json!({"tag":tag,"listen":{"address":listen,"port":0},
        "protocol":{"type":"direct","target":target.ip().to_string(),"port":target.port()}})
}
async fn pending(
    proxy: &Proxy,
    state: &mut OrchestrationState,
    candidate: RuntimeConfig,
) -> Arc<EngineRuntimeSnapshot> {
    let previous = proxy.engine().runtime_snapshot();
    let snapshot = proxy
        .engine()
        .prepare_configuration_snapshot(
            zero_runtime::configuration::PreparedConfiguration::prepare(candidate).unwrap(),
        )
        .unwrap();
    let (send, receive) = tokio::sync::oneshot::channel();
    *proxy.host.reload_ack().lock().unwrap() = Some(PendingReloadAck {
        expected: snapshot.clone(),
        previous: previous.clone(),
        compensating: false,
        sender: send,
    });
    state.reconcile_reload(proxy).await;
    receive.await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&state.applied_snapshot, &snapshot));
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &previous));
    assert!(!snapshot.configuration_publication().is_committed());
    snapshot
}
async fn commit(proxy: &Proxy, snapshot: Arc<EngineRuntimeSnapshot>) {
    ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
        .commit_configuration(snapshot, Duration::from_secs(5), false)
        .await
        .unwrap();
}
async fn assert_pending<F: Future>(future: F) {
    tokio::pin!(future);
    std::future::poll_fn(|context| {
        assert!(future.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
}
#[tokio::test]
#[cfg(feature = "managed-datagram-runtime")]
async fn actual_direct_tcp_udp_ready_candidate_cannot_forward_before_commit() {
    let (target, udp_target) = loop {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        if let Ok(udp) = UdpSocket::bind(tcp.local_addr().unwrap()).await {
            break (tcp, udp);
        }
    };
    let healthy = direct("healthy", "127.0.0.1", target.local_addr().unwrap());
    let proxy = Proxy::new(config(serde_json::json!([healthy.clone()]))).unwrap();
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let healthy_owner = proxy.inbound_owners.owner("healthy").unwrap();
    let candidate = direct("candidate", "127.0.0.2", target.local_addr().unwrap());
    let snapshot = pending(
        &proxy,
        &mut state,
        config(serde_json::json!([healthy, candidate])),
    )
    .await;
    let owner = proxy.inbound_owners.owner("candidate").unwrap();
    assert!(owner.is_ready());
    assert!(Arc::ptr_eq(
        &healthy_owner,
        &proxy.inbound_owners.owner("healthy").unwrap()
    ));
    // This address comes from the original granted native view. No free-port
    // probe, dropped reservation, or post-close rebind serves as evidence.
    let address = *owner.original_listener_address.get().unwrap();
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let prior_join = owner.accepted_join_for_test().map(|joined| joined.id());
    udp.send_to(b"blocked-udp", address).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(joined) = owner
                .accepted_join_for_test()
                .filter(|joined| Some(joined.id()) != prior_join)
            {
                // The source reports its local business rejection and the real
                // peer task returns normally. This is the original Scope's
                // consumed join, not a fabricated failed/successful controller.
                assert!(joined.result().is_ok());
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let prior_tcp_join = owner.accepted_join_for_test().unwrap().id();
    let mut tcp = TcpStream::connect(address).await.unwrap();
    tcp.write_all(b"blocked-tcp").await.unwrap();
    let mut rejected = Vec::new();
    let rejected_read =
        tokio::time::timeout(Duration::from_secs(3), tcp.read_to_end(&mut rejected))
            .await
            .unwrap();
    // Closing before consuming the request can produce native EOF or RST.
    // Neither observation substitutes for the original task/native receipts.
    match rejected_read {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        result => panic!("unexpected candidate TCP rejection: {result:?}"),
    }
    assert!(rejected.is_empty());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(joined) = owner
                .accepted_join_for_test()
                .filter(|joined| joined.id() != prior_tcp_join)
            {
                assert!(joined.result().is_err(), "candidate admission was rejected");
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_pending(target.accept()).await;
    let mut packet = [0; 64];
    assert_pending(udp_target.recv_from(&mut packet)).await;
    assert!(proxy.engine().completed_sessions().is_empty());
    // An unchanged real listener remains usable while this candidate is closed.
    let mut healthy_tcp =
        TcpStream::connect(*healthy_owner.original_listener_address.get().unwrap())
            .await
            .unwrap();
    healthy_tcp.write_all(b"healthy").await.unwrap();
    let (mut healthy_target, _) = tokio::time::timeout(Duration::from_secs(3), target.accept())
        .await
        .unwrap()
        .unwrap();
    let mut healthy_payload = [0; 7];
    healthy_target
        .read_exact(&mut healthy_payload)
        .await
        .unwrap();
    assert_eq!(&healthy_payload, b"healthy");
    healthy_target.write_all(b"ok").await.unwrap();
    let mut healthy_reply = [0; 2];
    healthy_tcp.read_exact(&mut healthy_reply).await.unwrap();
    assert_eq!(&healthy_reply, b"ok");
    drop(healthy_tcp);
    drop(healthy_target);
    drop(tcp);
    commit(&proxy, snapshot.clone()).await;
    assert!(snapshot.configuration_publication().is_committed());
    let mut tcp = TcpStream::connect(address).await.unwrap();
    tcp.write_all(b"after-tcp").await.unwrap();
    let (mut accepted, _) = tokio::time::timeout(Duration::from_secs(3), target.accept())
        .await
        .unwrap()
        .unwrap();
    let mut payload = [0; 9];
    accepted.read_exact(&mut payload).await.unwrap();
    assert_eq!(&payload, b"after-tcp");
    udp.send_to(b"after-udp", address).await.unwrap();
    let (size, _) = tokio::time::timeout(Duration::from_secs(3), udp_target.recv_from(&mut packet))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&packet[..size], b"after-udp");
    drop(tcp);
    drop(accepted);
    state.finish(&proxy, Ok(())).await.unwrap();
    let terminal = owner
        .terminal_for_test()
        .expect("actual original owner physically closed");
    assert_eq!(terminal.identity(), owner.identity());
    assert!(terminal.closed_io_receipts() > 0);
    assert!(terminal.joined_tasks() > 0);
}
