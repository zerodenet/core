use super::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
#[tokio::test]
async fn actual_tcp_upload_end_retains_same_sid_until_complete_reverse_data_and_end() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upload: Vec<u8> = (0..256 * 1024).map(|n| (n % 251) as u8).collect();
        let reply: Vec<u8> = (0..1025).map(|n| (n % 239) as u8).collect();
        let sid = 7;
        let mut states = VlessInboundMuxStreams::new();
        let session = Session::new(
            0,
            Address::Ipv4([127, 0, 0, 1]),
            address.port(),
            Network::Tcp,
            ProtocolType::new("vless"),
        );
        let (incoming, _) = states.open_stream(sid, &session);
        for bytes in upload.chunks(16 * 1024) {
            assert!(states.push_stream_data(sid, None, bytes.to_vec()).unwrap());
        }
        let mut mux = VlessInboundMuxSession::new();
        let (wire, mut received) = tokio::io::duplex(16384);
        let mut wire = TcpRelayStream::new(wire);
        states
            .apply_inbound_action(
                &mut mux,
                &mut wire,
                VlessInboundMuxAction::End { session_id: sid },
            )
            .await
            .unwrap();
        assert!(states.contains_stream(sid));
        assert!(states.streams[&sid].upload.is_none());
        let (writer, mut responses) =
            VlessInboundMuxWriter::channel(MuxResponseBacklogPolicy::default());
        let mut logical =
            VlessInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let target = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut body = Vec::new();
            socket.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, upload);
            socket.write_all(&reply).await.unwrap();
            socket.shutdown().await.unwrap();
        };
        let relay = async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            let counts = tokio::io::copy_bidirectional(&mut logical, &mut socket)
                .await
                .unwrap();
            assert_eq!(counts, (upload.len() as u64, reply.len() as u64));
        };
        let flush = async {
            loop {
                let downlink = responses
                    .recv()
                    .await
                    .expect("actual response and END")
                    .into_inner();
                let end = downlink.is_end();
                assert!(states
                    .send_inbound_downlink(&mut mux, &mut wire, downlink)
                    .await
                    .unwrap());
                if end {
                    assert!(!states.contains_stream(sid));
                    break;
                }
                assert!(states.contains_stream(sid));
            }
        };
        let decode = async {
            let mut body = Vec::new();
            loop {
                let frame = codec::read_frame_tokio(&mut received).await.unwrap();
                assert_eq!(frame.session_id, sid);
                if frame.status == STATUS_END {
                    break;
                }
                assert_eq!(frame.status, STATUS_KEEP);
                assert_ne!(frame.options & OPTION_DATA, 0);
                body.extend_from_slice(&frame.payload);
            }
            assert_eq!(body, reply);
        };
        tokio::join!(target, relay, flush, decode);
    })
    .await
    .expect("original TCP halfclose and full reverse wire deadline");
}

#[tokio::test]
async fn actual_tcp_full_response_fifo_waits_then_delivers_complete_tail_and_end() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upload: Vec<u8> = (0..256 * 1024).map(|n| (n % 251) as u8).collect();
        let reply: Vec<u8> = (0..256 * 1024 + 1025).map(|n| (n % 239) as u8).collect();
        let sid = 7;
        let mut states = VlessInboundMuxStreams::new();
        let session = Session::new(
            0,
            Address::Ipv4([127, 0, 0, 1]),
            address.port(),
            Network::Tcp,
            ProtocolType::new("vless"),
        );
        let (incoming, _) = states.open_stream(sid, &session);
        for bytes in upload.chunks(16 * 1024) {
            assert!(states.push_stream_data(sid, None, bytes.to_vec()).unwrap());
        }
        let mut mux = VlessInboundMuxSession::new();
        let (wire, mut received) = tokio::io::duplex(16384);
        let mut wire = TcpRelayStream::new(wire);
        states
            .apply_inbound_action(
                &mut mux,
                &mut wire,
                VlessInboundMuxAction::End { session_id: sid },
            )
            .await
            .unwrap();
        assert!(states.contains_stream(sid));
        assert!(states.streams[&sid].upload.is_none());
        let (writer, mut responses) = VlessInboundMuxWriter::channel(
            MuxResponseBacklogPolicy::from_config(Some(1), Some(16 * 1024)).unwrap(),
        );
        let logical = VlessInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let (blocked, waiting) = tokio::sync::oneshot::channel();
        let mut logical = CapacityProbe {
            io: logical,
            blocked: Some(blocked),
        };
        let target = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut body = Vec::new();
            socket.read_to_end(&mut body).await.unwrap();
            assert_eq!(body, upload);
            socket.write_all(&reply).await.unwrap();
            socket.shutdown().await.unwrap();
        };
        let relay = async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            let counts = tokio::io::copy_bidirectional(&mut logical, &mut socket)
                .await
                .unwrap();
            assert_eq!(counts, (upload.len() as u64, reply.len() as u64));
        };
        let flush = async {
            waiting
                .await
                .expect("actual writer reached the same finite FIFO capacity");
            loop {
                let downlink = responses
                    .recv()
                    .await
                    .expect("actual response and END")
                    .into_inner();
                let end = downlink.is_end();
                assert!(states
                    .send_inbound_downlink(&mut mux, &mut wire, downlink)
                    .await
                    .unwrap());
                if end {
                    assert!(!states.contains_stream(sid));
                    break;
                }
                assert!(states.contains_stream(sid));
            }
        };
        let decode = async {
            let mut body = Vec::new();
            loop {
                let frame = codec::read_frame_tokio(&mut received).await.unwrap();
                assert_eq!(frame.session_id, sid);
                if frame.status == STATUS_END {
                    break;
                }
                assert_eq!(frame.status, STATUS_KEEP);
                assert_ne!(frame.options & OPTION_DATA, 0);
                body.extend_from_slice(&frame.payload);
            }
            assert_eq!(body, reply);
        };
        tokio::join!(target, relay, flush, decode);
    })
    .await
    .expect("original TCP halfclose and full reverse wire deadline");
}

struct CapacityProbe<T> {
    io: T,
    blocked: Option<tokio::sync::oneshot::Sender<()>>,
}
impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CapacityProbe<T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().io).poll_read(cx, bytes)
    }
}
impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for CapacityProbe<T> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let result = std::pin::Pin::new(&mut this.io).poll_write(cx, bytes);
        if result.is_pending() {
            if let Some(blocked) = this.blocked.take() {
                let _ = blocked.send(());
            }
        }
        result
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
