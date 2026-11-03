//! Actual SIP003 activation must finish before the host acknowledges a candidate.
use super::super::state::OrchestrationState;
use crate::runtime::{PendingReloadAck, Proxy};
use std::{future::Future, sync::Arc, task::Poll};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    time::{timeout, Duration},
};
const PLUGIN: &str = r#"
import os, socket, sys
control = socket.create_connection(('127.0.0.1', int(sys.argv[1])))
control.sendall((str(os.getpid()) + '\n').encode())
if control.recv(1) != b'r': sys.exit(2)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind((os.environ['SS_REMOTE_HOST'], int(os.environ['SS_REMOTE_PORT'])))
listener.listen()
control.sendall(b'bound\n')
while True:
    stream, _ = listener.accept()
    stream.close()
"#;
fn candidate(port: u16, gate_port: u16) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"ready-ss","listen":{"address":"127.0.0.1","port":port},
        "protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"readiness-test",
        "plugin":{"command":"python3","args":["-c",PLUGIN,gate_port.to_string()],"mode":"tcp_only"}}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}

#[tokio::test]
async fn candidate_ack_waits_for_actual_plugin_activation_and_both_listener_loops() {
    let initial =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(initial).unwrap();
    let mut state = OrchestrationState::new(&proxy).await.unwrap();
    let previous = state.applied_snapshot.clone();
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    // Retain the real candidate port until the same task begins reconciliation.
    let reserved = crate::socket_fixture::tcp_udp();
    let port = reserved.0.local_addr().unwrap().port();
    let candidate = candidate(port, gate.local_addr().unwrap().port());
    proxy.engine.stage_runtime_config(candidate).unwrap();
    let expected = proxy.engine.runtime_snapshot();
    let (sender, mut acknowledgement) = tokio::sync::oneshot::channel();
    *proxy.host.reload_ack().lock().unwrap() = Some(PendingReloadAck {
        expected: expected.clone(),
        previous: previous.clone(),

        compensating: false,
        sender,
    });
    drop(reserved);
    let mut applying = Box::pin(state.reconcile_reload(&proxy));
    let (control, _) = tokio::select! {
        result = gate.accept() => result.unwrap(),
        _ = &mut applying => panic!("spawn is not a Ready acknowledgement"),
    };
    let mut control = BufReader::new(control);
    let mut line = String::new();
    let pid = tokio::select! {
        result = control.read_line(&mut line) => { result.unwrap(); line.trim().parse::<u32>().unwrap() },
        _ = &mut applying => panic!("real plugin activation is still pending"),
    };
    // Child liveness alone is insufficient: the actual reconcile future must
    // still be suspended in activation, and its real ACK must still be empty.
    std::future::poll_fn(|cx| {
        assert!(applying.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        acknowledgement.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    let owner = proxy.inbound_owners.owner("ready-ss").unwrap();
    let mut waiting = Box::pin(owner.wait_ready());
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(waiting);
    assert!(proxy
        .protocols
        .current_components(expected.config())
        .is_none());
    // The actual source activation remains suspended at this held gate. Its
    // host lifecycle start deadline is five seconds; the protocol's ten-second
    // readiness ceiling does not extend that earlier deadline.
    control.get_mut().write_all(b"r").await.unwrap();
    timeout(Duration::from_secs(4), &mut applying)
        .await
        .unwrap();
    drop(applying);
    acknowledgement.await.unwrap().unwrap();
    owner.wait_ready().await.unwrap();
    assert!(Arc::ptr_eq(&state.applied_snapshot, &expected));
    assert_eq!(
        proxy.inbound_owners.owner("ready-ss").unwrap().identity(),
        owner.identity()
    );
    // This manually driven host ACK hands off to the accepted apply owner in
    // production. Finish uses the same persistent native Child owner here.
    state.finish(&proxy, Ok(())).await.unwrap();
    assert!(proxy.inbound_owners.owner("ready-ss").is_none());
    let terminal = owner
        .terminal_for_test()
        .expect("actual ready owner cleanup");
    assert_eq!(terminal.identity(), owner.identity());
    assert!(terminal.joined_tasks() > 0);
    assert!(terminal.closed_io_receipts() > 0);
    let status = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "the actual SIP003 Child must be reaped");
    let mut tail = Vec::new();
    timeout(Duration::from_secs(2), control.read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tail, b"bound\n");
}

mod deadline;
mod shutdown;
