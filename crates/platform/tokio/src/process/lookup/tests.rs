use super::*;
use crate::contract::scope::TaskScope;
use std::{future::poll_fn, future::Future, num::NonZeroUsize, task::Poll, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Handle,
};

#[tokio::test]
async fn actual_start_wait_and_cancel_keep_same_blocking_join_credit_until_host_ack() {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(2));
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (caller_id, caller) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            let read = reader.read_u8();
            tokio::pin!(read);
            let mut entered = Some(entered);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .map_err(|source| error(Stage::Receive, Reason::Io).with_source(source))?;
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    let original = LocalProcessLookup::prepare(
        caller.clone(),
        "127.0.0.1:1".parse().unwrap(),
        LocalProcessTransport::Tcp,
        registrar.cancellation(),
        NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    let start = original.register(&registrar.downgrade()).unwrap();
    // Reading this mutex requires the real blocking worker to have released
    // it inside Condvar::wait. This is its actual pre-IO handoff input, not a
    // marker task or a simulated native operation.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !original.state.lock().unwrap().waiting_at_start {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let worker = original.witness().unwrap();
    let mut joined = Box::pin(worker.wait());
    poll_fn(|cx| {
        assert!(joined.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(joined);
    assert!(!original.retired());
    assert!(!caller.is_joined());
    drop(start); // Stop only this same still-unstarted metadata operation.
    let receipt = tokio::time::timeout(Duration::from_secs(5), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.id(), worker.task_id().unwrap());
    worker.wait().await.result().unwrap();
    assert!(
        !original.retired(),
        "real join is independent of the hosted registration ACK"
    );
    assert!(matches!(
        registrar.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    ));
    scope.acknowledge(&receipt).unwrap();
    assert!(original.retired());
    assert!(
        !caller.is_joined(),
        "cancelling lookup leaves its actual source/caller live"
    );
    writer.write_u8(1).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), caller_id);
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[tokio::test]
async fn real_local_tcp_and_udp_metadata_use_original_worker_and_matching_native_cleanup() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let tcp = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (_server, _) = listener.accept().unwrap();
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(2));
    scope.enable_registration().unwrap();
    let registrar = scope.registrar();
    let (mut reader, mut writer) = tokio::io::duplex(1);
    let (id, caller) = registrar
        .spawn_tracked(TaskClass::Critical, async move {
            reader
                .read_u8()
                .await
                .map_err(|source| error(Stage::Receive, Reason::Io).with_source(source))?;
            Ok(())
        })
        .unwrap();
    let foreign = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let rejected = LocalProcessLookup::prepare(
        caller.clone(),
        tcp.local_addr().unwrap(),
        LocalProcessTransport::Tcp,
        registrar.cancellation(),
        NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
    )
    .unwrap();
    assert!(rejected.register(&foreign.registrar().downgrade()).is_err());
    assert!(rejected.witness().is_none());
    assert!(!rejected.state.lock().unwrap().attempted);
    for (source, transport) in [
        (tcp.local_addr().unwrap(), LocalProcessTransport::Tcp),
        (udp.local_addr().unwrap(), LocalProcessTransport::Udp),
    ] {
        let original = LocalProcessLookup::prepare(
            caller.clone(),
            source,
            transport,
            registrar.cancellation(),
            NonZeroUsize::new(8 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        original.register(&registrar.downgrade()).unwrap().start();
        let cancel = CancellationToken::new();
        let info = tokio::time::timeout(Duration::from_secs(5), original.result(&cancel))
            .await
            .unwrap()
            .unwrap()
            .expect("same actual local endpoint process");
        assert_eq!(info.pid, std::process::id());
        assert!(
            original.native.confirmed(),
            "actual file/handle closes are separate from the still-unconsumed worker join"
        );
        assert!(
            original.collect_native_after_join().is_err(),
            "body result is not a Scope join"
        );
        let worker = original.witness().unwrap();
        let receipt = scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), worker.task_id().unwrap());
        worker.wait().await.result().unwrap();
        assert!(!original.retired());
        assert!(
            original.native.confirmed(),
            "matching OS metadata handle close is distinct from Scope ACK"
        );
        scope.acknowledge(&receipt).unwrap();
        assert!(original.retired());
    }
    writer.write_u8(1).await.unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), id);
    scope.acknowledge(&receipt).unwrap();
    scope.begin_shutdown();
    assert!(scope.is_drained());
}
