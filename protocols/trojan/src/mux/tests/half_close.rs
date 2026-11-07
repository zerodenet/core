use super::super::{
    backlog::{MuxResponseBacklog, MuxResponseBacklogPolicy},
    read_mux_server_event, TrojanInboundMuxAction, TrojanInboundMuxStreams,
    TrojanInboundMuxTcpRelay, TrojanInboundMuxWriter, TrojanMuxServerEvent,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

#[tokio::test]
async fn actual_tcp_reply_survives_mux_upload_end_after_all_accepted_data() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upload: Vec<u8> = (0..256 * 1024).map(|n| (n % 251) as u8).collect();
        let reply: Vec<u8> = (0..1025).map(|n| (n % 239) as u8).collect();
        let sid = 7;
        let mut streams = TrojanInboundMuxStreams::new();
        let incoming = streams.open_stream(sid, Vec::new());
        for bytes in upload.chunks(16 * 1024) {
            assert!(streams.push_stream_data(sid, bytes.to_vec()));
        }
        assert!(streams
            .apply_inbound_action(TrojanInboundMuxAction::End { session_id: sid })
            .is_none());
        assert!(!streams.streams.contains_key(&sid));
        let policy = MuxResponseBacklogPolicy::default();
        let (sender, mut responses) = mpsc::channel(policy.frames());
        let writer = TrojanInboundMuxWriter::new(sender, MuxResponseBacklog::from_policy(policy));
        let mut stream =
            TrojanInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let target = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            socket.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, upload);
            // The target replies only after the logical upload half has ended.
            socket.write_all(&reply).await.unwrap();
            socket.shutdown().await.unwrap();
        };
        let relay = async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            let counts = tokio::io::copy_bidirectional(&mut stream, &mut socket)
                .await
                .unwrap();
            assert_eq!(counts, (upload.len() as u64, reply.len() as u64));
        };
        let downlink = async {
            let mut received = Vec::new();
            loop {
                let frame = responses
                    .recv()
                    .await
                    .expect("reply DATA and final END")
                    .into_inner();
                match read_mux_server_event(&mut frame.as_slice()).await.unwrap() {
                    TrojanMuxServerEvent::Data {
                        session_id,
                        payload,
                    } => {
                        assert_eq!(session_id, sid);
                        received.extend_from_slice(&payload);
                    }
                    TrojanMuxServerEvent::End { session_id } => {
                        assert_eq!(session_id, sid);
                        break;
                    }
                    other => panic!("unexpected reply frame: {other:?}"),
                }
            }
            assert_eq!(received, reply);
        };
        tokio::join!(target, relay, downlink);
    })
    .await
    .expect("original upload/reply half-close deadline");
}

#[tokio::test]
async fn actual_tcp_full_response_fifo_waits_then_delivers_complete_tail_and_end() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upload: Vec<u8> = (0..256 * 1024).map(|n| (n % 251) as u8).collect();
        let reply: Vec<u8> = (0..256 * 1024 + 1025).map(|n| (n % 239) as u8).collect();
        let sid = 7;
        let mut streams = TrojanInboundMuxStreams::new();
        let incoming = streams.open_stream(sid, Vec::new());
        for bytes in upload.chunks(16 * 1024) {
            assert!(streams.push_stream_data(sid, bytes.to_vec()));
        }
        assert!(streams
            .apply_inbound_action(TrojanInboundMuxAction::End { session_id: sid })
            .is_none());
        assert!(!streams.streams.contains_key(&sid));
        let policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16 * 1024)).unwrap();
        let (sender, mut responses) = mpsc::channel(policy.frames());
        let writer = TrojanInboundMuxWriter::new(sender, MuxResponseBacklog::from_policy(policy));
        let stream = TrojanInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let (blocked, waiting) = tokio::sync::oneshot::channel();
        let mut stream = CapacityProbe {
            io: stream,
            blocked: Some(blocked),
        };
        let target = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            socket.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, upload);
            // The target replies only after the logical upload half has ended.
            socket.write_all(&reply).await.unwrap();
            socket.shutdown().await.unwrap();
        };
        let relay = async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            let counts = tokio::io::copy_bidirectional(&mut stream, &mut socket)
                .await
                .unwrap();
            assert_eq!(counts, (upload.len() as u64, reply.len() as u64));
        };
        let downlink = async {
            waiting
                .await
                .expect("actual writer reached the same finite FIFO capacity");
            let mut received = Vec::new();
            loop {
                let frame = responses
                    .recv()
                    .await
                    .expect("reply DATA and final END")
                    .into_inner();
                match read_mux_server_event(&mut frame.as_slice()).await.unwrap() {
                    TrojanMuxServerEvent::Data {
                        session_id,
                        payload,
                    } => {
                        assert_eq!(session_id, sid);
                        received.extend_from_slice(&payload);
                    }
                    TrojanMuxServerEvent::End { session_id } => {
                        assert_eq!(session_id, sid);
                        break;
                    }
                    other => panic!("unexpected reply frame: {other:?}"),
                }
            }
            assert_eq!(received, reply);
        };
        tokio::join!(target, relay, downlink);
    })
    .await
    .expect("original upload/reply half-close deadline");
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
