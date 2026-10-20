use super::*;
use std::{
    future::poll_fn,
    io::IoSliceMut,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Wake, Waker},
};
fn bound(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn limits() -> MetadataLimits {
    MetadataLimits {
        main_sockets: bound(1),
        probe_sockets: bound(1),
        probe_attempts_per_initialization: bound(4),
        concurrent_send_calls: bound(1),
        write_pollers: bound(2),
    }
}
fn address() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}
fn transmit(destination: SocketAddr, contents: &[u8]) -> MetadataTransmit<'_> {
    MetadataTransmit {
        destination,
        contents,
        source_ip: None,
        ecn: None,
        segment_size: None,
    }
}
async fn send(tx: &MetadataDatagramTx, message: &MetadataTransmit<'_>) -> MetadataSendOutcome {
    let mut poller = tx.create_write_poller().unwrap();
    poll_fn(|cx| loop {
        match poller.poll_writable(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result.unwrap(),
        }
        match tx.try_send(message) {
            Err(error) if error.kind() == std_io::ErrorKind::WouldBlock => continue,
            result => return Poll::Ready(result.unwrap()),
        }
    })
    .await
}
#[tokio::test]
async fn actual_original_receives_ecn_destination_and_zero_payload_with_native_close_receipt() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    let local = tx.local_addr().unwrap();
    let mut message = transmit(local, b"");
    message.source_ip = Some(local.ip());
    message.ecn = Some(DatagramEcn::Ect0);
    let outcome = send(&tx, &message).await;
    assert_eq!(
        outcome,
        MetadataSendOutcome {
            bytes: 0,
            datagrams: 1,
            complete: true
        }
    );
    let mut bytes = [0u8; 32];
    let mut buffers = [IoSliceMut::new(&mut bytes)];
    let mut metadata = [MetadataReceive::default()];
    assert_eq!(
        poll_fn(|cx| rx.poll_recv(cx, &mut buffers, &mut metadata))
            .await
            .unwrap(),
        1
    );
    assert_eq!(metadata[0].source, local);
    assert_eq!(metadata[0].destination_ip, Some(local.ip()));
    assert_eq!(metadata[0].ecn, Some(DatagramEcn::Ect0));
    assert_eq!(metadata[0].len, 0);
    assert_eq!(owner.occupied_main(), 1);
    assert_eq!(owner.occupied_probes(), 0);
    assert_eq!(
        control.close().unwrap().reservation(),
        control.reservation()
    );
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
    assert_eq!(
        tx.try_send(&message).unwrap_err().kind(),
        std_io::ErrorKind::NotConnected
    );
    assert_eq!(owner.occupied_main(), 0);
}
mod capacity;
mod pollers;
