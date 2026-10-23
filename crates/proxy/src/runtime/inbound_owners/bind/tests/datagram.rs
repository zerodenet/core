//! The host lease keeps the original UDP owner through pending I/O and cleanup.
use super::*;
use tokio::net::UdpSocket;
use zero_platform_tokio::contract::component::GrantedDatagramListener;

fn datagram_lease(proxy: &Proxy, config: &Arc<RuntimeConfig>) -> InboundBindLease {
    let inbound = &config.inbounds[0];
    let operation = proxy
        .protocols
        .prepare_inbound_listener(inbound.clone(), None)
        .unwrap();
    let declaration = ResourceDeclaration::DatagramListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(50).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    let prepared = PreparedInboundBind::new(operation, Arc::from([declaration])).unwrap();
    let (shutdown, _) = watch::channel(false);
    proxy
        .inbound_owners
        .reserve_listener(
            &inbound.tag,
            config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap()
}

#[tokio::test]
async fn original_datagram_lease_pending_receive_is_revoked_and_same_fd_closed_before_rebind() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = datagram_lease(&proxy, &config);
    let original = lease.owner();
    let identity = original.identity();
    lease.bind_native().unwrap();
    let (operation, view, installed) = lease.finish().unwrap().promote_native().unwrap();
    drop(operation); // The real host driver below owns the unique receive view.
    let NativeListenerView::Datagram(GrantedDatagramListener { mut rx, tx }) = view else {
        panic!("canonical UDP declaration must produce original UDP views")
    };
    assert!(Arc::ptr_eq(&original, &installed.owner()));
    assert_eq!(identity, installed.owner().identity());
    let address = rx.local_addr().unwrap();
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.send_to(b"native", address).await.unwrap();
    let mut bytes = [0; 64];
    let (size, source) = rx.recv_from_addr(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"native");
    assert_eq!(source, client.local_addr().unwrap());
    drop(tx); // Dropping a view must neither close nor acknowledge the FD.
    assert!(UdpSocket::bind(address).await.is_err());
    let pending = Arc::new(Notify::new());
    let entered = pending.clone();
    original
        .spawn_driver(async move {
            let mut bytes = [0; 64];
            let read = rx.recv_from_addr(&mut bytes);
            tokio::pin!(read);
            let result = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => {
                    entered.notify_one();
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await;
            assert!(result.is_err()); // Actual original RX was revoked, not a fixture delay.
            Ok(())
        })
        .unwrap();
    original.seal_listener_installation();
    timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    proxy
        .inbound_owners
        .shutdown("first", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(original.identity(), identity);
    let terminal = original.terminal_for_test().unwrap();
    assert_eq!(terminal.joined_tasks(), 1);
    assert_eq!(terminal.closed_io_receipts(), 1);
    let rebound = UdpSocket::bind(address).await.unwrap();
    assert_eq!(rebound.local_addr().unwrap(), address);
}

#[tokio::test]
async fn dropped_unpromoted_datagram_view_requires_same_reservation_close_receipt() {
    let proxy = proxy();
    let config = proxy.engine().config();
    let mut lease = datagram_lease(&proxy, &config);
    let owner = lease.owner();
    let identity = owner.identity();
    lease.bind_native().unwrap();
    let bound = lease.finish().unwrap();
    let address = bound.local_addr().unwrap();
    drop(bound);
    assert!(UdpSocket::bind(address).await.is_err());
    assert!(proxy.inbound_owners.identities().contains(&identity));
    proxy
        .inbound_owners
        .retire_bind_reservation_reported(
            "first",
            Instant::now() + Duration::from_secs(2),
            |terminal| {
                assert_eq!(terminal.identity(), identity);
                assert_eq!(terminal.joined_tasks(), 0);
                assert_eq!(terminal.closed_io_receipts(), 1);
                Ok(())
            },
        )
        .await
        .unwrap();
    assert!(!proxy.inbound_owners.identities().contains(&identity));
    let rebound = UdpSocket::bind(address).await.unwrap();
    assert_eq!(rebound.local_addr().unwrap(), address);
}
