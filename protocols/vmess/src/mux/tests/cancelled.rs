use super::super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn cancelled_tcp_keeps_full_fifo_end_until_the_original_driver_sends_it() {
    exchange(false).await;
}
#[tokio::test]
async fn cancelled_last_tcp_writer_keeps_full_fifo_end_until_the_original_driver_sends_it() {
    exchange(true).await;
}
async fn exchange(drop_last_sender: bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        let sid = 9;
        let (wire, mut received) = tokio::io::duplex(1024);
        let policy = MuxResponseBacklogPolicy::from_config(Some(1), Some(16384)).unwrap();
        let (writer, task) = VmessInboundMuxWriter::prepare(wire, policy);
        // The existing server reader owns this same writer throughout the SID.
        let mut server_writer = Some(writer.clone());
        let pending = writer.pending_ends.clone();
        let (sender, incoming) = mpsc::unbounded_channel();
        drop(sender);
        let mut stream = VmessInboundMuxTcpRelay::new(sid, incoming, writer).into_standard_stream();
        let payload = vec![7; 8000];
        stream.write_all(&payload).await.unwrap();
        drop(stream);
        assert!(pending.lock().unwrap().contains(&sid));
        if drop_last_sender {
            drop(server_writer.take());
        }
        let reading = async {
            match read_mux_server_event(&mut received).await.unwrap() {
                VmessMuxServerEvent::Data {
                    session_id,
                    payload: body,
                } => {
                    assert_eq!(session_id, sid);
                    assert_eq!(body, payload);
                }
                other => panic!("DATA must precede actual cancelled END: {other:?}"),
            }
            match read_mux_server_event(&mut received).await.unwrap() {
                VmessMuxServerEvent::End { session_id } => assert_eq!(session_id, sid),
                other => panic!("same writer must deliver retained END: {other:?}"),
            }
            assert!(pending.lock().unwrap().is_empty());
            drop(server_writer.take());
            assert_eq!(received.read(&mut [0]).await.unwrap(), 0);
        };
        let (result, ()) = tokio::join!(task, reading);
        result.unwrap();
    })
    .await
    .expect("same actual writer cancellation deadline");
}
