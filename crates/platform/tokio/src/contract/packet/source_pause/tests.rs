//! Actual read Pending on two original TCP views; pause is never a join/close.
use super::*;
use crate::contract::scope::{TaskClass, TaskScope};
use std::{future::Future, num::NonZeroUsize, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{ComponentId, ResourceGeneration};
async fn actual_reader(
    mut read: tokio::net::tcp::OwnedReadHalf,
    checkpoint: PacketSourceCheckpoint,
) -> Result<(), ChannelError> {
    let mut bytes = [0; 1];
    let receive = read.read(&mut bytes);
    tokio::pin!(receive);
    std::future::poll_fn(|cx| {
        assert!(receive.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    loop {
        tokio::select! {
            request=checkpoint.requested()=>{
                // No byte has been read; the same original future stays live.
                checkpoint.acknowledge(request,true);
                checkpoint.resumed(request).await;
            },
            received=&mut receive=>{
                assert_eq!(received.unwrap(),1);
                return Ok(());
            }
        }
    }
}
#[tokio::test]
async fn original_reads_pause_twice_only_after_inflight_acquisition_then_resume_without_native_or_join_receipts(
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connecting = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
    let (connecting, accepted) = tokio::join!(connecting, listener.accept());
    let (left_read, mut left_write) = connecting.unwrap().into_split();
    let (right_read, mut right_write) = accepted.unwrap().0.into_split();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    let pause = PacketSourcePause::new(
        ComponentInstanceId {
            component: ComponentId(818),
            generation: ResourceGeneration(9),
        },
        scope.registrar().scope_identity(),
        CancellationToken::new(),
    );
    assert!(
        pause.pause(tokio::time::Instant::now()).await.is_err(),
        "missing participants cannot assert idle"
    );
    let (main_send, main_take) = tokio::sync::oneshot::channel();
    let (_, main) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            actual_reader(left_read, main_take.await.unwrap()).await
        })
        .unwrap();
    let (wire_send, wire_take) = tokio::sync::oneshot::channel();
    let (_, wire) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            actual_reader(right_read, wire_take.await.unwrap()).await
        })
        .unwrap();
    let main_checkpoint = pause.bind(PacketPauseParticipant::Main, &main).unwrap();
    assert!(pause.bind(PacketPauseParticipant::Wire, &main).is_err());
    let wire_checkpoint = pause.bind(PacketPauseParticipant::Wire, &wire).unwrap();
    assert!(pause.bind(PacketPauseParticipant::Main, &main).is_err());
    assert!(main_send.send(main_checkpoint).is_ok());
    assert!(wire_send.send(wire_checkpoint).is_ok());
    let acquisition = pause.try_enter().unwrap();
    let mut pausing =
        Box::pin(pause.pause(tokio::time::Instant::now() + std::time::Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(pausing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        pause.try_enter().is_err(),
        "same mutex seals new opens before ACK"
    );
    assert_eq!(
        pause.state.lock().unwrap().acknowledged,
        [None, None],
        "inflight original acquisition must reach custody first"
    );
    assert!(!main.is_joined());
    assert!(!wire.is_joined());
    drop(acquisition);
    let paused = pausing.await.unwrap();
    paused.validate().unwrap();
    assert!(!main.is_joined());
    assert!(!wire.is_joined());
    assert!(pause.try_enter().is_err());
    drop(paused);
    let paused = pause
        .pause(tokio::time::Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    paused.validate().unwrap();
    assert!(!main.is_joined());
    assert!(!wire.is_joined());
    drop(paused);
    let acquisition = pause.try_enter().unwrap();
    drop(acquisition);
    left_write.write_all(&[1]).await.unwrap();
    right_write.write_all(&[2]).await.unwrap();
    scope.next_exit().await.unwrap();
    scope.next_exit().await.unwrap();
    assert!(main.is_joined());
    assert!(wire.is_joined());
    assert!(pause.try_enter().is_err());
    assert!(pause.pause(tokio::time::Instant::now()).await.is_err());
}
