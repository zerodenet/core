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
async fn vision_writer_uses_configured_testseed() {
    let (client, mut peer) = tokio::io::duplex(4096);
    let mut stream = VisionStream::with_testseed(client, UUID, None, [10, 1, 20, 1]);

    let hello = [0x16, 0x03, 0x01, 0, 2, 0x01, 0];
    stream.write_all(&hello).await.unwrap();
    stream.flush().await.unwrap();

    let mut header = [0_u8; 21];
    peer.read_exact(&mut header).await.unwrap();
    assert_eq!(u16::from_be_bytes([header[19], header[20]]), 13);
    let mut body = [0; 20];
    peer.read_exact(&mut body).await.unwrap();

    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();
    let mut header = [0; 5];
    peer.read_exact(&mut header).await.unwrap();
    assert_eq!(u16::from_be_bytes([header[3], header[4]]), 15);
}

#[tokio::test]
async fn vision_non_tls_payload_uses_short_padding_even_on_first_write() {
    let (client, mut peer) = tokio::io::duplex(4096);
    let mut stream = VisionStream::with_testseed(client, UUID, None, [10, 1, 20, 1]);
    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();
    let mut header = [0; 21];
    peer.read_exact(&mut header).await.unwrap();
    assert_eq!(u16::from_be_bytes([header[19], header[20]]), 0);
}

#[tokio::test]
async fn vision_padding_is_bounded_by_the_reference_buffer_size() {
    let (client, mut peer) = tokio::io::duplex(16384);
    let mut stream = VisionStream::with_testseed(client, UUID, None, [u32::MAX, 1, u32::MAX, 1]);
    let hello = tls13_server_hello_record();
    stream.write_all(&hello).await.unwrap();
    stream.flush().await.unwrap();
    let mut header = [0; 21];
    peer.read_exact(&mut header).await.unwrap();
    let padding = usize::from(u16::from_be_bytes([header[19], header[20]]));
    assert_eq!(21 + hello.len() + padding, 8192);
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

#[tokio::test]
async fn vision_never_switches_inside_an_application_record() {
    for data in [
        vec![0x17, 3, 3, 0, 4, 0xaa],
        vec![0x17, 3, 3, 0, 1, 0xaa, 0x17],
    ] {
        let (socket, mut peer) = tokio::io::duplex(8192);
        let control = TransportBypassControl::default();
        let mut stream =
            VisionStream::with_testseed(socket, UUID, Some(control.clone()), [0, 1, 0, 1]);
        let hello = tls13_server_hello_record();
        peer.write_all(&frame(true, 0, &hello, 0)).await.unwrap();
        stream.read_exact(&mut vec![0; hello.len()]).await.unwrap();
        stream.write_all(&data).await.unwrap();
        stream.flush().await.unwrap();
        let mut header = [0; 21];
        peer.read_exact(&mut header).await.unwrap();
        assert_eq!(header[16], 0);
        assert!(!control.write_bypass_requested());
    }
}

#[tokio::test]
async fn vision_does_not_authorize_direct_for_ccm8_or_unknown_ciphers() {
    for cipher in [0x1305_u16, 0xc02f] {
        let (socket, mut peer) = tokio::io::duplex(8192);
        let control = TransportBypassControl::default();
        let mut stream =
            VisionStream::with_testseed(socket, UUID, Some(control.clone()), [0, 1, 0, 1]);
        let mut hello = tls13_server_hello_record();
        hello[76..78].copy_from_slice(&cipher.to_be_bytes());
        peer.write_all(&frame(true, 0, &hello, 0)).await.unwrap();
        stream.read_exact(&mut vec![0; hello.len()]).await.unwrap();
        stream.write_all(&[0x17, 3, 3, 0, 1, 0xaa]).await.unwrap();
        stream.flush().await.unwrap();
        let mut header = [0; 21];
        peer.read_exact(&mut header).await.unwrap();
        assert_eq!(header[16], 1);
        assert!(!control.write_bypass_requested());
    }
}

#[tokio::test]
async fn vision_ends_non_tls_padding_one_packet_before_the_shared_window_expires() {
    let (socket, mut peer) = tokio::io::duplex(8192);
    let mut stream = VisionStream::with_testseed(socket, UUID, None, [0, 1, 0, 1]);
    peer.write_all(&frame(true, 0, b"reply", 0)).await.unwrap();
    stream.read_exact(&mut [0; 5]).await.unwrap();
    for index in 0..6 {
        stream.write_all(b"payload").await.unwrap();
        stream.flush().await.unwrap();
        let mut wire = vec![0; if index == 0 { 28 } else { 12 }];
        peer.read_exact(&mut wire).await.unwrap();
        assert_eq!(
            wire[if index == 0 { 16 } else { 0 }],
            if index == 5 { 1 } else { 0 }
        );
    }
}

#[tokio::test]
async fn vision_short_unframed_response_does_not_wait_for_a_padding_header() {
    let (socket, mut peer) = tokio::io::duplex(128);
    let mut stream = VisionStream::new(socket, UUID, None);
    let mut short = UUID.to_vec();
    short.push(0xff);
    peer.write_all(&short).await.unwrap();
    let mut output = [0; 17];
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        stream.read_exact(&mut output),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.as_slice(), short);
}

#[tokio::test]
async fn vision_combined_continue_frames_consume_one_filter_packet() {
    let (socket, mut peer) = tokio::io::duplex(8192);
    let mut stream = VisionStream::with_testseed(socket, UUID, None, [0, 1, 0, 1]);
    let mut input = frame(true, 0, b"first", 0);
    input.extend(frame(false, 0, b"second", 0));
    peer.write_all(&input).await.unwrap();
    stream.read_exact(&mut [0; 11]).await.unwrap();
    for index in 0..6 {
        stream.write_all(b"payload").await.unwrap();
        stream.flush().await.unwrap();
        let mut wire = vec![0; if index == 0 { 28 } else { 12 }];
        peer.read_exact(&mut wire).await.unwrap();
        assert_eq!(
            wire[if index == 0 { 16 } else { 0 }],
            if index == 5 { 1 } else { 0 }
        );
    }
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
    body.push(32); // legacy_session_id_echo
    body.extend_from_slice(&[0; 32]);
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
