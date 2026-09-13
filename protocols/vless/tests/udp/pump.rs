use super::*;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn inbound_udp_response_does_not_discard_a_partially_read_request() {
    timeout(Duration::from_secs(2), async {
        let target = Address::Ipv4([192, 0, 2, 3]);
        let wire = b"\0\x03xyz";
        for split in 1..wire.len() {
            let (mut peer, mut stream) = tokio::io::duplex(64);
            let mut responder = VlessInboundUdpResponder::new(target.clone(), 53);
            peer.write_all(&wire[..split]).await.unwrap();
            let mut partial =
                std::boxed::Box::pin(responder.read_inbound_dispatch_tokio(&mut stream));
            assert!(futures_util::poll!(&mut partial).is_pending());
            drop(partial);
            responder
                .write_response_for_target_tokio(&mut stream, &target, 53, b"reply")
                .await
                .unwrap();
            let mut response = [0; 7];
            peer.read_exact(&mut response).await.unwrap();
            assert_eq!(&response, b"\0\x05reply");
            peer.write_all(&wire[split..]).await.unwrap();
            let packet = responder
                .read_inbound_dispatch_tokio(&mut stream)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(packet.target(), &target);
            assert_eq!(packet.port(), 53);
            assert_eq!(packet.payload(), b"xyz");
        }
    })
    .await
    .expect("an inbound response discarded partial request framing");
}

struct Observed {
    io: DuplexStream,
    bytes: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = output.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, output);
        let read = output.filled().len() - before;
        if read != 0 {
            self.bytes.fetch_add(read, Ordering::Release);
            self.changed.notify_one();
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn sending_preserves_partial_response_addon_length_and_payload() {
    timeout(Duration::from_secs(2), async {
        let wire = [0, 2, 0x41, 0x42, 0, 3, b'x', b'y', b'z'];
        let target = Address::Ipv4([192, 0, 2, 1]);
        for split in 1..wire.len() {
            let (client, mut server) = tokio::io::duplex(64);
            let bytes = Arc::new(AtomicUsize::new(0));
            let changed = Arc::new(Notify::new());
            let flow = VlessUdpFlowConnection::new(spawn_udp_flow(
                Observed {
                    io: client,
                    bytes: bytes.clone(),
                    changed: changed.clone(),
                },
                VlessEstablishedUdpFlow {
                    target: target.clone(),
                    port: 53,
                },
            ));
            let mut responses = flow.subscribe_responses();
            server.write_all(&wire[..split]).await.unwrap();
            while bytes.load(Ordering::Acquire) < split {
                changed.notified().await;
            }
            assert_eq!(flow.send(&target, 53, b"request").await.unwrap(), 9);
            let mut request = [0; 9];
            server.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"\0\x07request");
            server.write_all(&wire[split..]).await.unwrap();
            assert_eq!(
                responses.recv().await.unwrap(),
                (target.clone(), 53, b"xyz".to_vec()),
                "split={split}"
            );
            drop(flow);
            assert_eq!(server.read(&mut [0]).await.unwrap(), 0);
        }
    })
    .await
    .expect("a concurrent send lost partial VLESS UDP response bytes");
}

#[tokio::test]
async fn downstream_progresses_while_a_large_upstream_packet_is_backpressured() {
    timeout(Duration::from_secs(2), async {
        let (client, mut server) = tokio::io::duplex(32);
        let target = Address::Ipv4([192, 0, 2, 2]);
        let flow = VlessUdpFlowConnection::new(spawn_udp_flow(
            client,
            VlessEstablishedUdpFlow {
                target: target.clone(),
                port: 53,
            },
        ));
        let mut responses = flow.subscribe_responses();
        let sender = flow.clone();
        let send_target = target.clone();
        let send = tokio::spawn(async move { sender.send(&send_target, 53, &[0x51; 4096]).await });
        assert_eq!(server.read_u16().await.unwrap(), 4096);
        assert!(!send.is_finished());
        server.write_all(b"\0\0\0\x02ok").await.unwrap();
        assert_eq!(
            responses.recv().await.unwrap(),
            (target, 53, b"ok".to_vec())
        );
        let mut payload = [0; 4096];
        server.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload, [0x51; 4096]);
        assert_eq!(send.await.unwrap().unwrap(), 4098);
        drop(flow);
        assert_eq!(server.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .expect("backpressured UDP uplink blocked its downstream");
}

#[tokio::test]
async fn inbound_domain_udp_response_accepts_runtime_resolved_endpoint() {
    let responder = VlessInboundUdpResponder::new(Address::Domain("udp.example".into()), 53);
    let (mut peer, mut stream) = tokio::io::duplex(64);
    responder
        .write_response_for_target_tokio(&mut stream, &Address::Ipv4([192, 0, 2, 53]), 53, b"reply")
        .await
        .unwrap();
    let mut wire = [0; 7];
    peer.read_exact(&mut wire).await.unwrap();
    assert_eq!(&wire, b"\0\x05reply");
}
