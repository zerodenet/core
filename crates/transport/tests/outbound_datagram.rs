use std::sync::Arc;

use zero_platform_tokio::{EgressInterface, EgressInterfaceControl};
use zero_transport::{
    OutboundDatagramSocketFactory, OutboundHostResolveFuture, OutboundHostResolver,
};

#[derive(Debug)]
struct TestResolver;

impl OutboundHostResolver for TestResolver {
    fn resolve(&self, _host: String, port: u16) -> OutboundHostResolveFuture {
        Box::pin(async move {
            Ok(vec![
                std::net::SocketAddr::new("192.0.2.1".parse().unwrap(), port),
                std::net::SocketAddr::new("192.0.2.1".parse().unwrap(), port),
            ])
        })
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn factory_reads_the_current_egress_for_each_new_socket() {
    let control = EgressInterfaceControl::default();
    let factory = OutboundDatagramSocketFactory::new(control.clone());
    let peer = "192.0.2.1:443".parse().unwrap();

    let selected = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();
    control.replace_for(false, Some(selected.clone()));
    assert_eq!(factory.egress_for(peer), Some(selected));

    control.replace_for(false, None);
    assert!(factory.egress_for(peer).is_none());
    let socket = factory
        .bind_std(peer)
        .expect("new socket must observe the reconciled automatic egress");
    assert!(socket.local_addr().unwrap().is_ipv4());
}

#[test]
fn factory_rejects_an_active_tun_without_a_physical_egress() {
    let control = EgressInterfaceControl::default();
    let factory = OutboundDatagramSocketFactory::new(control.clone());
    let peer = "192.0.2.1:443".parse().unwrap();
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);

    let error = factory.bind_std(peer).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
}

#[tokio::test]
async fn factory_resolves_node_domains_through_the_runtime_bridge() {
    let factory = OutboundDatagramSocketFactory::new(EgressInterfaceControl::default())
        .with_host_resolver(Arc::new(TestResolver));

    assert_eq!(
        factory
            .resolve_server_addresses("node.example", 443)
            .await
            .unwrap(),
        vec!["192.0.2.1:443".parse().unwrap()]
    );
}

#[tokio::test]
async fn factory_never_implicitly_uses_the_system_resolver() {
    let factory = OutboundDatagramSocketFactory::new(EgressInterfaceControl::default());

    let error = factory
        .resolve_server_addresses("node.example", 443)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn relay_socket_preserves_packets_and_cancels_its_only_reader_on_drop() {
    use std::io::{self, IoSliceMut};
    use zero_transport::datagram_relay::ConnectedDatagram;
    struct Carrier {
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
        receiver: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Vec<u8>>>,
    }
    #[async_trait::async_trait]
    impl ConnectedDatagram for Carrier {
        async fn send(&self, bytes: &[u8]) -> io::Result<()> {
            self.sender
                .send(bytes.to_vec())
                .await
                .map_err(io::Error::other)
        }
        async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
            let packet = self
                .receiver
                .lock()
                .await
                .recv()
                .await
                .ok_or(io::ErrorKind::BrokenPipe)?;
            bytes[..packet.len()].copy_from_slice(&packet);
            Ok(packet.len())
        }
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let carrier = Arc::new(Carrier {
        sender,
        receiver: tokio::sync::Mutex::new(receiver),
    });
    let weak = Arc::downgrade(&carrier);
    let peer = "192.0.2.1:443".parse().unwrap();
    let factory = OutboundDatagramSocketFactory::new(Default::default()).with_relay(Arc::new(
        move |actual| {
            assert_eq!(actual, peer);
            let carrier = carrier.clone();
            Box::pin(async move { Ok(carrier as Arc<dyn ConnectedDatagram>) })
        },
    ));
    assert_eq!(
        factory.bind_std(peer).unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    assert!(factory.bind_tokio(peer).await.is_err());
    let socket = factory.open_socket(peer).await.unwrap();
    drop(factory);
    for payload in [b"one".as_slice(), &[0x57; 1600], b"last"] {
        socket
            .try_send(&quinn::udp::Transmit {
                destination: peer,
                contents: payload,
                ecn: None,
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
        let mut bytes = [0; 2048];
        let mut meta = [quinn::udp::RecvMeta::default()];
        let count = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            std::future::poll_fn(|cx| {
                socket.poll_recv(cx, &mut [IoSliceMut::new(&mut bytes)], &mut meta)
            }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 1);
        assert_eq!(meta[0].addr, peer);
        assert_eq!(&bytes[..meta[0].len], payload);
    }
    let bad = socket
        .try_send(&quinn::udp::Transmit {
            destination: "192.0.2.2:443".parse().unwrap(),
            contents: b"no bypass",
            ecn: None,
            segment_size: None,
            src_ip: None,
        })
        .unwrap_err();
    assert_eq!(bad.kind(), io::ErrorKind::InvalidInput);
    drop(socket);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("socket drop releases the carrier and its pending receive future");
}
