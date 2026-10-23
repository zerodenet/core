//! Actual provider originals close even when a mapping close first reports I/O failure.
use super::*;
use crate::runtime::udp_dispatch::operation::LiveDirectSource;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::Notify,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::component::BoundDatagramRequest;

struct MappingClose {
    first: TcpStream,
    retry: TcpStream,
    failure: Arc<Mutex<Option<Arc<std::io::Error>>>>,
    failed: Arc<Notify>,
    pending: Arc<Notify>,
    first_done: bool,
}
impl ResourceLifecycle for MappingClose {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        if !self.first_done {
            self.first_done = true;
            let source = Arc::new(self.first.read_u8().await.unwrap_err());
            *self.failure.lock().unwrap() = Some(source.clone());
            self.failed.notify_one();
            return Err(
                error(Reason::Io, "actual mapping close gate reported EOF").with_source(source)
            );
        }
        let read = self.retry.read_u8();
        tokio::pin!(read);
        std::future::poll_fn(|cx| {
            let result = std::future::Future::poll(read.as_mut(), cx);
            if result.is_pending() {
                self.pending.notify_one();
            }
            result
        })
        .await
        .map_err(|cause| error(Reason::Io, "mapping retry failed").with_source(cause))?;
        Ok(CloseReport::default()) // no native descriptor is acknowledged here
    }
}
async fn pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    (listener.accept().await.unwrap().0, peer)
}
fn retains_original(
    error: &(dyn std::error::Error + 'static),
    original: &Arc<std::io::Error>,
) -> bool {
    let mut next = Some(error);
    while let Some(error) = next {
        if error
            .downcast_ref::<Arc<std::io::Error>>()
            .is_some_and(|actual| Arc::ptr_eq(actual, original))
        {
            return true;
        }
        next = error.source();
    }
    false
}
#[tokio::test]
async fn bound_peer_native_close_continues_after_real_mapping_error_and_cancelled_retry() {
    let live = LiveDirectSource::new().await;
    let (first, mut eof) = pair().await;
    eof.shutdown().await.unwrap();
    let (retry, mut allow_retry) = pair().await;
    let failure = Arc::new(Mutex::new(None));
    let failed = Arc::new(Notify::new());
    let pending = Arc::new(Notify::new());
    let (resources, authority) = live
        .context
        .claim_bound_outbound_datagram(
            Instant::now() + Duration::from_secs(2),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            |grant| {
                Ok((
                    erase_lifecycle(MappingClose {
                        first,
                        retry,
                        failure: failure.clone(),
                        failed: failed.clone(),
                        pending: pending.clone(),
                        first_done: false,
                    }),
                    grant.resources,
                ))
            },
        )
        .await
        .unwrap();
    let registration = authority.registration();
    assert!(registration.same_owner(&authority.registration()));
    assert!(registration.belongs_to(&live.context.binding()));
    assert!(registration.closed().is_none());
    assert_eq!(
        registration
            .joined(registration.identity().task)
            .unwrap_err()
            .reason,
        Reason::Revoked
    );
    let generation = NetworkGeneration(
        live.runtime
            .services()
            .upstream()
            .egress_control()
            .generation(),
    );
    let binding = authority
        .authorize(live.source().await, generation)
        .unwrap();
    let same_epoch = authority
        .authorize(live.source().await, generation)
        .unwrap();
    assert_eq!(
        binding.id, same_epoch.id,
        "logical sessions in one healthy epoch share one binding"
    );
    let echo = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let granted = resources
        .open_bound_outbound_datagram(BoundDatagramRequest {
            binding: binding.id,
            peer: echo.local_addr().unwrap(),
            expected_network_generation: generation,
        })
        .unwrap();
    let original_address = granted.socket.local_addr().unwrap();
    granted
        .socket
        .send_to_addr(b"", echo.local_addr().unwrap())
        .await
        .unwrap();
    let mut bytes = [0; 4];
    let (size, sender) = timeout(Duration::from_secs(2), echo.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(size, 0);
    echo.send_to(b"", sender).await.unwrap();
    assert_eq!(
        timeout(
            Duration::from_secs(2),
            granted.socket.recv_from_addr(&mut bytes)
        )
        .await
        .unwrap()
        .unwrap()
        .0,
        0
    );
    drop(granted);
    assert!(
        UdpSocket::bind(original_address).await.is_err(),
        "view Drop is not native close"
    );
    let mut confirming = Box::pin(
        live.proxy
            .inbound_owners
            .confirm_shutdown("source", Instant::now() + Duration::from_secs(2)),
    );
    tokio::select! {
        result = &mut confirming => panic!("actual mapping retry must remain pending: {result:?}"),
        _ = pending.notified() => {}
    }
    drop(confirming);
    failed.notified().await;
    live.context
        .binding()
        .wait_joined(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    // Reaching the real second mapping read follows the completed first
    // provider shutdown attempt; the OS rebind checks its original close.
    let _confirmed = UdpSocket::bind(original_address).await.unwrap();
    assert!(
        registration.closed().is_none(),
        "actual Scope join and native close cannot replace pending protocol cleanup"
    );
    let original = failure.lock().unwrap().clone().unwrap();
    assert_eq!(original.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(live.proxy.inbound_owners.owner("source").is_some());
    allow_retry.write_u8(1).await.unwrap();
    let receipt = live
        .proxy
        .inbound_owners
        .confirm_shutdown("source", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    let closed = registration
        .closed()
        .expect("same original provider and mapping completed");
    assert!(closed.matches(&registration));
    assert_eq!(closed.identity().instance, registration.identity().instance);
    assert_eq!(
        closed.identity().association,
        registration.identity().association
    );
    assert!(Arc::ptr_eq(
        &closed.joined(),
        &live.context.binding().observed_join().unwrap()
    ));
    // Equal numeric values and even the same actual caller cannot substitute
    // another provider's private completion origin.
    let other = PeerBoundDatagramRegistration::new(registration.identity(), live.context.binding());
    assert!(!registration.same_owner(&other));
    assert!(!closed.matches(&other));
    assert!(other.closed().is_none());
    let retained = receipt.terminal().failure().unwrap();
    assert!(retains_original(retained.as_ref(), &original));
    assert_eq!(
        receipt.terminal().closed_io_receipts(),
        1,
        "one aggregated actual provider receipt, not a numeric FD count"
    );
    let handed_off = live
        .proxy
        .inbound_owners
        .release_closed(receipt, |terminal| Ok(terminal.failure().unwrap()))
        .unwrap();
    assert!(Arc::ptr_eq(&retained, &handed_off));
    assert!(live.finish().await.is_err());
}
