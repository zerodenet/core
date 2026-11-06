//! Actual UserUdpStack input and two real registered TCP reads, without a TUN.
use super::*;
use std::{net::IpAddr, num::NonZeroUsize, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
use zero_platform_tokio::contract::{
    packet::{PacketPauseParticipant, PacketSourceCheckpoint},
    scope::{TaskClass, TaskScope},
    CancellationToken,
};
use zero_stack::{packet, UserNetworkStack};
use zero_traits::UdpStack;

async fn reader(
    mut read: tokio::net::tcp::OwnedReadHalf,
    checkpoint: PacketSourceCheckpoint,
    pending: Arc<tokio::sync::Notify>,
) -> Result<(), ChannelError> {
    let mut byte = [0];
    let receive = read.read_exact(&mut byte);
    tokio::pin!(receive);
    std::future::poll_fn(|cx| {
        assert!(receive.as_mut().poll(cx).is_pending());
        pending.notify_one();
        Poll::Ready(())
    })
    .await;
    loop {
        tokio::select! {
            request = checkpoint.requested() => {
                checkpoint.acknowledge(request, true);
                checkpoint.resumed(request).await;
            },
            received = &mut receive => {
                received.unwrap();
                return Ok(());
            }
        }
    }
}
async fn pending(input: impl Future) {
    tokio::pin!(input);
    std::future::poll_fn(|cx| {
        assert!(input.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn empty_input_releases_acquisition_but_dequeued_datagram_keeps_it_until_custody() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let connecting = tokio::net::TcpStream::connect(listener.local_addr().unwrap());
    let (connecting, accepted) = tokio::join!(connecting, listener.accept());
    let (main_read, mut main_write) = connecting.unwrap().into_split();
    let (wire_read, mut wire_write) = accepted.unwrap().0.into_split();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(2).unwrap(),
    );
    let source = PacketSourcePause::new(
        ComponentInstanceId {
            component: ComponentId(991),
            generation: ResourceGeneration(3),
        },
        scope.registrar().scope_identity(),
        CancellationToken::new(),
    );
    let mut originals = Vec::new();
    for (kind, read) in [
        (PacketPauseParticipant::Main, main_read),
        (PacketPauseParticipant::Wire, wire_read),
    ] {
        let (send, take) = tokio::sync::oneshot::channel();
        let observed = Arc::new(tokio::sync::Notify::new());
        let pending = observed.clone();
        let (_, original) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                reader(read, take.await.unwrap(), pending).await
            })
            .unwrap();
        assert!(send.send(source.bind(kind, &original).unwrap()).is_ok());
        observed.notified().await;
        originals.push(original);
    }
    let (out, _returns) = tokio::sync::mpsc::channel(1);
    let (_tcp, udp) = UserNetworkStack::new(out, 1440).into_parts();
    let mut buffer = [0; 64];
    let mut input = Box::pin(admitted(udp.recv_from_with_peer(&mut buffer), &source));
    pending(&mut input).await; // Actual empty UserUdpStack poll holds no guard.
    let deadline = || tokio::time::Instant::now() + Duration::from_secs(2);
    let paused = source.pause(deadline()).await.unwrap();
    paused.validate().unwrap();
    let frame = packet::build_udp(
        "10.0.0.2".parse::<IpAddr>().unwrap(),
        "10.0.0.3".parse::<IpAddr>().unwrap(),
        32000,
        443,
        b"queued",
    );
    udp.feed(&frame).await;
    pending(&mut input).await;
    assert!(
        !udp.quiescent().await,
        "sealed read preserves the original queued input"
    );
    drop(paused);
    let (received, acquisition) = input.await.unwrap();
    assert_eq!(received.unwrap().0, b"queued".len());
    assert_eq!(&buffer[..6], b"queued");
    let mut pausing = Box::pin(source.pause(deadline()));
    pending(&mut pausing).await; // Dequeued work has not entered child custody yet.
    assert!(originals.iter().all(|original| !original.is_joined()));
    drop(acquisition); // Production releases only after actual record installation.
    let paused = pausing.await.unwrap();
    paused.validate().unwrap();
    drop(paused);
    main_write.write_all(&[1]).await.unwrap();
    wire_write.write_all(&[2]).await.unwrap();
    scope.next_exit().await.unwrap();
    scope.next_exit().await.unwrap();
    assert!(originals.iter().all(|original| original.is_joined()));
}
