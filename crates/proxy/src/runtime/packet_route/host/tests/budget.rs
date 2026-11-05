use super::{ownership::PointerDevice, *};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn configured_one_write_fifo_refuses_unsubmitted_packets_while_the_original_driver_is_pending(
) {
    let native = Arc::new(AtomicUsize::new(0));
    let device = HostPacketDevice::start(
        PointerDevice {
            writes: native.clone(),
            block: true,
        },
        zero_config::DirectPacketDeviceConfig {
            backend: zero_config::DirectPacketDeviceBackend::Descriptor,
            fd: Some(9),
            interface: "host-test".into(),
            router_addresses: vec!["10.64.0.1".parse().unwrap()],
            write_queue: std::num::NonZeroUsize::new(1).unwrap(),
        },
        1500,
    );
    device.activate();
    let packet = |port| {
        packet::build_udp(
            "10.0.0.2".parse().unwrap(),
            "192.0.2.1".parse().unwrap(),
            40000,
            port,
            b"bounded",
        )
        .into()
    };
    let mut first = packet(443);
    let mut second = packet(444);
    let mut excess: zero_traits::PacketBuffer = packet(445);
    let original = excess.to_vec();
    let pointer = excess.as_ptr();
    let (replies, _received) = mpsc::channel::<Vec<u8>>(4);
    let replies: zero_stack::packet_output::PacketSender = replies.into();
    let mut writing = Box::pin(device.forward(&mut first, 1, replies.clone(), 0, None));
    std::future::poll_fn(|cx| {
        assert!(writing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while native.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut queued = Box::pin(device.forward(&mut second, 1, replies.clone(), 0, None));
    std::future::poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(device.writes.capacity(), 0);
    assert_eq!(
        device
            .forward(&mut excess, 1, replies, 0, None)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(excess, original);
    assert_eq!(excess.as_ptr(), pointer);
    device.close();
    assert!(writing.await.is_err());
    assert!(queued.await.is_err());
    // Only the real original driver join is certified by this fixture. It has
    // no native close provider and cannot authorize production replacement.
    device.worker.join().await.unwrap();
    assert!(!device.usable());
}
