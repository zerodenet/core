use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn cancelled_tcp_keeps_full_fifo_end_until_the_original_driver_sends_it() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let sid = 9;
        let session = Session::new(
            0,
            Address::Ipv4([127, 0, 0, 1]),
            80,
            Network::Tcp,
            ProtocolType::new("vless"),
        );
        let mut states = VlessInboundMuxStreams::new();
        let (incoming, _) = states.open_stream(sid, &session);
        let policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16384)).unwrap();
        let (writer, mut responses) = VlessInboundMuxWriter::channel(policy);
        let owner = writer.clone();
        let mut stream = VlessInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let payload = vec![7; MUX_MAX_PAYLOAD];
        stream.write_all(&payload).await.unwrap();
        drop(stream);
        assert!(owner.pending_ends.lock().unwrap().contains(&sid));
        assert!(
            states.contains_stream(sid),
            "Drop is not actual END delivery"
        );
        let (wire, mut received) = tokio::io::duplex(1024);
        let mut wire = TcpRelayStream::new(wire);
        let mut mux = VlessInboundMuxSession::new();
        let sending = async {
            loop {
                let frame = responses.recv().await.unwrap().into_inner();
                let ended = frame.is_end();
                states
                    .send_inbound_downlink(&mut mux, &mut wire, frame)
                    .await
                    .unwrap();
                owner.retry_end_requests();
                if ended {
                    break;
                }
                assert!(states.contains_stream(sid));
            }
            assert!(!states.contains_stream(sid));
            assert!(owner.pending_ends.lock().unwrap().is_empty());
            wire.shutdown().await.unwrap();
        };
        let reading = async {
            let data = codec::read_frame_tokio(&mut received).await.unwrap();
            assert_eq!(data.session_id, sid);
            assert_eq!(data.status, STATUS_KEEP);
            assert_eq!(data.payload, payload);
            let end = codec::read_frame_tokio(&mut received).await.unwrap();
            assert_eq!(end.session_id, sid);
            assert_eq!(end.status, STATUS_END);
            assert!(end.payload.is_empty());
            assert_eq!(received.read(&mut [0]).await.unwrap(), 0);
        };
        tokio::join!(sending, reading);
    })
    .await
    .expect("same actual writer cancellation deadline");
}
