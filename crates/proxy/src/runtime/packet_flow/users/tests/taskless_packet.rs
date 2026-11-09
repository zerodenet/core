//! The shared Source main is real authorization, not each logical view's worker.
use super::*;
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn taskless_packet_views_need_both_control_closed_and_return_without_claiming_source_join() {
    let mut scope = hosted();
    let (mut original, peer) = tokio::io::duplex(1);
    let (ready, pending_read) = tokio::sync::oneshot::channel();
    let (_, main) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let mut byte = [0];
            let mut buffer = tokio::io::ReadBuf::new(&mut byte);
            std::future::poll_fn(|cx| {
                assert!(tokio::io::AsyncRead::poll_read(
                    std::pin::Pin::new(&mut original),
                    cx,
                    &mut buffer
                )
                .is_pending());
                Poll::Ready(())
            })
            .await;
            ready.send(()).unwrap();
            assert_eq!(original.read(&mut byte).await.unwrap(), 0);
            Ok(())
        })
        .unwrap();
    pending_read.await.unwrap();
    let first = FlowUsers::new(limit());
    let (views, control) = first.acquire_packet_source(main.clone()).unwrap();
    let tx = Arc::new(views);
    let rx = tx.clone();
    drop(tx);
    drop(rx);
    pending(first.shutdown(deadline())).await;
    assert_eq!(first.slots.active(), 1);
    assert!(!main.is_joined());
    control.close();
    let report = first.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.exited_tasks.is_empty());
    assert!(report.closed_io.is_empty());
    assert_eq!(first.slots.active(), 0);
    assert!(
        !main.is_joined(),
        "no source join was manufactured for this logical view"
    );

    let second = FlowUsers::new(limit());
    let (held, control) = second.acquire_packet_source(main.clone()).unwrap();
    control.close();
    pending(second.shutdown(deadline())).await;
    assert_eq!(second.slots.active(), 1);
    drop(held);
    assert!(second.shutdown(deadline()).await.unwrap().is_complete());
    assert!(!main.is_joined());

    drop(peer);
    let exit = scope.next_receipt().await.unwrap();
    assert!(main.is_joined());
    assert!(exit.result().is_ok());
    scope.acknowledge(&exit).unwrap();
}
