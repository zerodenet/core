#![cfg(feature = "h2")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn settings(max_streams: u32) -> Vec<u8> {
    let mut data = vec![0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 3];
    data.extend_from_slice(&max_streams.to_be_bytes());
    data
}

#[tokio::test]
async fn alps_settings_apply_before_requests_and_do_not_generate_an_ack() {
    let (client_io, mut server_io) = tokio::io::duplex(65536);
    let mut builder = h2::client::Builder::new();
    builder.peer_application_settings(&settings(1));
    let (sender, conn) = builder
        .handshake::<_, bytes::Bytes>(client_io)
        .await
        .unwrap();
    assert_eq!(sender.current_max_send_streams(), 1);
    let driver = tokio::spawn(conn);
    let mut preface = [0; 24];
    server_io.read_exact(&mut preface).await.unwrap();
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    server_io.write_all(&settings(3)).await.unwrap();
    // A following ping puts a deterministic boundary after processing SETTINGS.
    server_io
        .write_all(&[0, 0, 8, 6, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8])
        .await
        .unwrap();
    let mut settings_acks = 0;
    loop {
        let mut header = [0; 9];
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            server_io.read_exact(&mut header),
        )
        .await
        .unwrap()
        .unwrap();
        let n = ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
        let mut body = vec![0; n];
        server_io.read_exact(&mut body).await.unwrap();
        if header[3] == 4 && header[4] & 1 != 0 {
            settings_acks += 1;
        }
        if header[3] == 6 && header[4] & 1 != 0 {
            break;
        }
    }
    assert_eq!(settings_acks, 1, "only the wire SETTINGS receives an ACK");
    assert_eq!(sender.current_max_send_streams(), 3);
    driver.abort();
}

#[tokio::test]
async fn alps_rejects_truncation_ack_forbidden_frames_and_invalid_setting_values() {
    let mut ack = settings(1);
    ack[4] = 1;
    let mut stream = settings(1);
    stream[8] = 1;
    let mut window = settings(u32::MAX);
    window[10] = 4;
    let mut push = settings(1);
    push[10] = 2;
    for data in [
        vec![0],
        ack,
        stream,
        window,
        push,
        vec![0, 0, 0, 0, 0, 0, 0, 0, 0],
    ] {
        let (io, _peer) = tokio::io::duplex(4096);
        let mut builder = h2::client::Builder::new();
        builder.peer_application_settings(&data);
        assert!(builder.handshake::<_, bytes::Bytes>(io).await.is_err());
    }
}
