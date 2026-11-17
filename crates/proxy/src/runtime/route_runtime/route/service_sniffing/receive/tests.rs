//! Actual UDP receive suspension survives interruption of its outer waiter.
use super::*;
use std::future::Future;
struct Native {
    io: tokio::net::UdpSocket,
    pending: Option<tokio::sync::oneshot::Sender<()>>,
}
impl DatagramRx<Event> for Native {
    fn poll_recv(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ResultEvent> {
        let mut bytes = [0; 64];
        let mut read = tokio::io::ReadBuf::new(&mut bytes);
        let sender = match self.io.poll_recv_from(cx, &mut read) {
            Poll::Pending => {
                if let Some(pending) = self.pending.take() {
                    pending.send(()).unwrap();
                }
                return Poll::Pending;
            }
            Poll::Ready(Ok(sender)) => sender,
            Poll::Ready(Err(error)) => panic!("real UDP receive failed: {error}"),
        };
        let payload = read.filled().to_vec();
        let dispatch = zero_core::InboundUdpDispatch::new(
            zero_core::ProtocolType::new("vless"),
            zero_core::Address::Ipv4([192, 0, 2, 1]),
            443,
            payload.clone(),
            Some(7),
        );
        Poll::Ready(Ok(Some(if payload.as_slice() == b"second" {
            let mut authentication = zero_core::SessionAuth::new("actual");
            authentication.principal_key = Some("second".into());
            Event::AuthenticatedPacket {
                dispatch,
                sender,
                protocol_bytes: 11,
                authentication,
            }
        } else {
            Event::Packet {
                dispatch,
                sender,
                protocol_bytes: 5,
            }
        })))
    }
}
#[tokio::test]
async fn interrupted_standard_receive_keeps_real_socket_and_each_packet_envelope() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let config =
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = crate::Proxy::new(config).unwrap();
        let runtime =
            crate::runtime::udp_ingress::UdpIngressRuntime::new(proxy.tcp_runtime_services());
        let policy =
            crate::runtime::sniff::SniffingPolicy::new(true, &["quic".into()], &[], false, false)
                .unwrap();
        let sniffing = UdpSniffingState::try_new(policy, runtime).unwrap();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = socket.local_addr().unwrap();
        let (pending, observed) = tokio::sync::oneshot::channel();
        let mut receiver = Box::pin(InspectedRx::new(
            Box::pin(Native {
                io: socket,
                pending: Some(pending),
            }),
            sniffing,
        ));
        {
            let read = recv(receiver.as_mut());
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            observed.await.unwrap();
        }
        let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        first.send_to(b"first", endpoint).await.unwrap();
        let packet = recv(receiver.as_mut()).await.unwrap().unwrap();
        match packet {
            Event::Packet {
                dispatch,
                sender,
                protocol_bytes,
            } => {
                assert_eq!(dispatch.payload(), b"first");
                assert_eq!(sender, first.local_addr().unwrap());
                assert_eq!(protocol_bytes, 5);
            }
            _ => panic!("unexpected original envelope"),
        }
        second.send_to(b"second", endpoint).await.unwrap();
        let packet = recv(receiver.as_mut()).await.unwrap().unwrap();
        match packet {
            Event::AuthenticatedPacket {
                dispatch,
                sender,
                protocol_bytes,
                authentication,
            } => {
                assert_eq!(dispatch.payload(), b"second");
                assert_eq!(sender, second.local_addr().unwrap());
                assert_eq!(protocol_bytes, 11);
                assert_eq!(authentication.principal_key.as_deref(), Some("second"));
            }
            _ => panic!("authentication from this actual packet was lost"),
        }
    })
    .await
    .unwrap();
}
