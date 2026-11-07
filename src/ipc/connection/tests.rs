use super::{
    command_can_run_concurrently, write_command_result, ConcurrentCommandTasks,
    MAX_PENDING_CONCURRENT_IPC_COMMANDS,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, sync::Arc};
use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
use tokio::sync::{oneshot, Mutex};
use zero_api::{
    CommandRequest, CommandResponse, ConfigApplyCommand, DiagnosticsProbeOutboundCommand,
    ModeSetCommand, RawResponse,
};
use zero_platform_tokio::contract::CancellationToken;

#[cfg(all(unix, feature = "wireguard"))]
mod endpoint;

mod events;

#[test]
fn outbound_diagnostics_are_the_only_reordered_ipc_commands() {
    assert!(command_can_run_concurrently(
        &CommandRequest::DiagnosticsProbeOutbound(DiagnosticsProbeOutboundCommand::default())
    ));
    assert!(!command_can_run_concurrently(&CommandRequest::ModeSet(
        ModeSetCommand::default()
    )));
    assert!(!command_can_run_concurrently(&CommandRequest::ConfigApply(
        ConfigApplyCommand::default()
    )));
    assert!(!command_can_run_concurrently(
        &CommandRequest::ConfigApplyRuntime(ConfigApplyCommand::default())
    ));
}

#[tokio::test]
async fn concurrent_commands_overlap_and_keep_response_ids_paired() {
    let (client, server) = tokio::io::duplex(4096);
    let (_, server_writer) = tokio::io::split(server);
    let writer = Arc::new(Mutex::new(BufWriter::new(server_writer)));
    let mut reader = BufReader::new(client);
    let mut tasks = ConcurrentCommandTasks::new(CancellationToken::new());
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_first_tx, release_first_rx) = oneshot::channel();
    let (release_second_tx, release_second_rx) = oneshot::channel();

    for (id, release) in [("first", release_first_rx), ("second", release_second_rx)] {
        let writer = writer.clone();
        let started_tx = started_tx.clone();
        assert!(tasks.try_spawn(async move {
            started_tx.send(id).unwrap();
            release.await.unwrap();
            write_command_result(
                writer,
                Some(serde_json::json!(id)),
                Ok(CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({"command": id})),
                }),
            )
            .await
        }));
    }

    let mut started = vec![
        started_rx.recv().await.unwrap(),
        started_rx.recv().await.unwrap(),
    ];
    started.sort_unstable();
    assert_eq!(started, vec!["first", "second"]);

    release_second_tx.send(()).unwrap();
    release_first_tx.send(()).unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let first: RawResponse = serde_json::from_str(line.trim()).unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    let second: RawResponse = serde_json::from_str(line.trim()).unwrap();
    let responses = [first, second];
    for response in responses {
        let id = response
            .id
            .as_ref()
            .and_then(serde_json::Value::as_str)
            .unwrap();
        assert_eq!(response.result.as_ref().unwrap()["result"]["command"], id);
    }

    tasks.cancel_all().await.unwrap();
}

#[tokio::test]
async fn concurrent_command_queue_is_bounded_and_cancellable() {
    struct CancellationGuard(Arc<AtomicBool>);
    impl Drop for CancellationGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let mut tasks = ConcurrentCommandTasks::new(CancellationToken::new());
    let cancelled = Arc::new(AtomicBool::new(false));
    let (started_tx, started_rx) = oneshot::channel();
    let cancellation = cancelled.clone();
    assert!(tasks.try_spawn(async move {
        let _guard = CancellationGuard(cancellation);
        use std::{
            future::{poll_fn, Future},
            task::Poll,
        };
        use tokio::io::AsyncReadExt;
        let (mut read, _held_peer) = tokio::io::duplex(1);
        let mut byte = [0];
        let pending = read.read(&mut byte);
        tokio::pin!(pending);
        poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        started_tx.send(()).unwrap();
        // The production Probe owner consumes this exact caller signal and
        // still finishes its native/Scope work before this command returns.
        zero_runtime::ApplyObservation::caller_stop()
            .expect("original connection stop metadata")
            .cancelled()
            .await;
        Ok(())
    }));
    started_rx.await.unwrap();

    for _ in 1..MAX_PENDING_CONCURRENT_IPC_COMMANDS {
        assert!(tasks.try_spawn(async {
            zero_runtime::ApplyObservation::caller_stop()
                .expect("original connection stop metadata")
                .cancelled()
                .await;
            Ok(())
        }));
    }
    assert!(!tasks.try_spawn(async { Ok(()) }));

    tasks.cancel_all().await.unwrap();
    assert!(cancelled.load(Ordering::SeqCst));
}

mod probe_stop;
