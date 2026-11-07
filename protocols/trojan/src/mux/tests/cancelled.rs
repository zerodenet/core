use super::super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn cancelled_tcp_keeps_full_fifo_end_until_the_original_driver_sends_it() {
    tokio::time::timeout(Duration::from_secs(8), async {
        let sid = 9;
        let (wire, mut received) = tokio::io::duplex(1024);
        let policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16384)).unwrap();
        let (writer, task) = TrojanInboundMuxWriter::prepare(wire, policy);
        // The existing server reader owns this same writer throughout the SID.
        let server_writer = writer.clone();
        let (sender, incoming) = mpsc::unbounded_channel();
        drop(sender);
        let mut stream =
            TrojanInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let payload = vec![7; 8000];
        stream.write_all(&payload).await.unwrap();
        drop(stream);
        assert!(server_writer.pending_ends.lock().unwrap().contains(&sid));
        let reading = async {
            match read_mux_server_event(&mut received).await.unwrap() {
                TrojanMuxServerEvent::Data {
                    session_id,
                    payload: body,
                } => {
                    assert_eq!(session_id, sid);
                    assert_eq!(body, payload);
                }
                other => panic!("DATA must precede actual cancelled END: {other:?}"),
            }
            match read_mux_server_event(&mut received).await.unwrap() {
                TrojanMuxServerEvent::End { session_id } => assert_eq!(session_id, sid),
                other => panic!("same writer must deliver retained END: {other:?}"),
            }
            assert!(server_writer.pending_ends.lock().unwrap().is_empty());
            drop(server_writer);
            assert_eq!(received.read(&mut [0]).await.unwrap(), 0);
        };
        let (result, ()) = tokio::join!(task, reading);
        result.unwrap();
    })
    .await
    .expect("same actual writer cancellation deadline");
}
