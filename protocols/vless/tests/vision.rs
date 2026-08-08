#![cfg(feature = "reality")]

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vless::vision::VisionStream;
use zero_traits::TransportBypassControl;

const UUID: [u8; 16] = [0x42; 16];

#[tokio::test]
async fn vision_writer_emits_xray_frame_shape() {
    let (client, mut peer) = tokio::io::duplex(4096);
    let mut stream = VisionStream::new(client, UUID, None);

    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();

    let mut prefix_and_header = [0_u8; 21];
    peer.read_exact(&mut prefix_and_header).await.unwrap();
    assert_eq!(&prefix_and_header[..16], &UUID);
    assert_eq!(prefix_and_header[16], 0);
    assert_eq!(
        u16::from_be_bytes([prefix_and_header[17], prefix_and_header[18]]),
        5
    );
    let padding_len = u16::from_be_bytes([prefix_and_header[19], prefix_and_header[20]]) as usize;
    let mut body = vec![0_u8; 5 + padding_len];
    peer.read_exact(&mut body).await.unwrap();
    assert_eq!(&body[..5], b"hello");
    assert!(body[5..].iter().all(|byte| *byte == 0));
}

#[tokio::test]
async fn vision_reader_handles_fragmented_frames_and_end_command() {
    let (client, mut peer) = tokio::io::duplex(4096);
    let mut stream = VisionStream::new(client, UUID, None);
    let mut frame = frame(true, 1, b"hello", 3);
    frame.extend_from_slice(b"raw");

    for chunk in frame.chunks(2) {
        peer.write_all(chunk).await.unwrap();
    }

    let mut output = [0_u8; 8];
    stream.read_exact(&mut output).await.unwrap();
    assert_eq!(&output, b"helloraw");
}

#[tokio::test]
async fn vision_direct_command_requests_raw_read_transition() {
    let (client, mut peer) = tokio::io::duplex(4096);
    let control = TransportBypassControl::default();
    let mut stream = VisionStream::new(client, UUID, Some(control.clone()));

    peer.write_all(&frame(true, 2, b"data", 0)).await.unwrap();
    let mut output = [0_u8; 4];
    stream.read_exact(&mut output).await.unwrap();

    assert_eq!(&output, b"data");
    assert!(control.read_bypass_requested());
}

#[tokio::test]
async fn tls13_application_data_ends_with_direct_command() {
    let (client, mut peer) = tokio::io::duplex(8192);
    let control = TransportBypassControl::default();
    let mut stream = VisionStream::new(client, UUID, Some(control.clone()));

    let server_hello = tls13_server_hello_record();
    peer.write_all(&frame(true, 0, &server_hello, 0))
        .await
        .unwrap();
    let mut received = vec![0_u8; server_hello.len()];
    stream.read_exact(&mut received).await.unwrap();
    assert_eq!(received, server_hello);

    let application_data = [0x17, 0x03, 0x03, 0x00, 0x01, 0xaa];
    stream.write_all(&application_data).await.unwrap();
    stream.flush().await.unwrap();

    let mut header = [0_u8; 21];
    peer.read_exact(&mut header).await.unwrap();
    assert_eq!(header[16], 2);
    assert!(control.write_bypass_requested());
}

fn frame(first: bool, command: u8, content: &[u8], padding_len: usize) -> Vec<u8> {
    let mut frame = Vec::new();
    if first {
        frame.extend_from_slice(&UUID);
    }
    frame.push(command);
    frame.extend_from_slice(&(content.len() as u16).to_be_bytes());
    frame.extend_from_slice(&(padding_len as u16).to_be_bytes());
    frame.extend_from_slice(content);
    frame.resize(frame.len() + padding_len, 0);
    frame
}

fn tls13_server_hello_record() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]);
    body.extend_from_slice(&[0_u8; 32]);
    body.push(0); // legacy_session_id_echo
    body.extend_from_slice(&[0x13, 0x01]);
    body.push(0); // legacy_compression_method
    body.extend_from_slice(&[0x00, 0x06]);
    body.extend_from_slice(&[0x00, 0x2b, 0x00, 0x02, 0x03, 0x04]);

    let mut handshake = vec![
        0x02,
        ((body.len() >> 16) & 0xff) as u8,
        ((body.len() >> 8) & 0xff) as u8,
        (body.len() & 0xff) as u8,
    ];
    handshake.extend_from_slice(&body);

    let mut record = vec![
        0x16,
        0x03,
        0x03,
        (handshake.len() >> 8) as u8,
        handshake.len() as u8,
    ];
    record.extend_from_slice(&handshake);
    record
}
