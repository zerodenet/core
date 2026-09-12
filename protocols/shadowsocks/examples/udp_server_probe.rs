//! Manual interop probe: send a SIP022 2022 UDP client packet (Zero-encoded,
//! reference-compatible) to a running Zero SS 2022 server and verify it
//! returns a valid type-1 server response carrying a DNS answer.
//!
//! Run with Zero listening on 127.0.0.1:18388 (2022-blake3-aes-256-gcm):
//!   cargo run --example udp_server_probe --features crypto,blake3

#[cfg(all(feature = "crypto", feature = "blake3"))]
use std::net::UdpSocket;
#[cfg(all(feature = "crypto", feature = "blake3"))]
use std::time::Duration;

#[cfg(all(feature = "crypto", feature = "blake3"))]
use shadowsocks::udp::ShadowsocksDatagramCodec;
#[cfg(all(feature = "crypto", feature = "blake3"))]
use shadowsocks::{CipherKind, ShadowsocksInboundProfile};
#[cfg(all(feature = "crypto", feature = "blake3"))]
use zero_core::Address;
#[cfg(all(feature = "crypto", feature = "blake3"))]
use zero_traits::DatagramCodec;

#[cfg(not(all(feature = "crypto", feature = "blake3")))]
fn main() {
    eprintln!("udp_server_probe requires --features crypto,blake3");
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
fn main() {
    let cipher = CipherKind::Blake3Aes256Gcm;
    let password = b"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
    let zero_addr = "127.0.0.1:18388";
    let dns_server = Address::Ipv4([8, 8, 8, 8]);

    // Minimal DNS query: id=0x1234, RD, QD=1, "example.com" A IN.
    let mut query = Vec::new();
    query.extend_from_slice(&[0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    query.extend_from_slice(b"\x07example\x03com\x00");
    query.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);

    let client_codec = ShadowsocksDatagramCodec::new(cipher, password);
    let packet = client_codec
        .encode(&dns_server, 53, &query)
        .expect("encode");

    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    sock.send_to(&packet, zero_addr).expect("send to zero");

    let mut buf = [0u8; 2048];
    let (n, _from) = sock.recv_from(&mut buf).expect("recv response from zero");
    let resp = &buf[..n];

    let profile = ShadowsocksInboundProfile::from_config(
        "2022-blake3-aes-256-gcm",
        "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    )
    .expect("build inbound profile");
    let mut server_session = profile.udp_session();
    let decoded = server_session
        .decode_request(resp)
        .expect("decode response");
    let (target, port, payload, server_ssid) = decoded.into_parts();
    let server_ssid = server_ssid.unwrap_or(0);
    eprintln!("response target={target:?} port={port} server_ssid={server_ssid:#x}");
    eprintln!("payload {} bytes", payload.len());

    // A DNS response echoes the query id (0x1234) and has QR=1.
    assert!(payload.len() >= 12, "response too short for DNS");
    assert_eq!(&payload[0..2], &[0x12, 0x34], "DNS id mismatch");
    assert_eq!(payload[2] & 0x80, 0x80, "DNS response QR bit not set");

    // SIP022 3.2.4: resending the exact same packet (same session id + packet
    // id) must be dropped by the per-session sliding window — no response.
    sock.set_read_timeout(Some(Duration::from_millis(800)))
        .unwrap();
    sock.send_to(&packet, zero_addr).expect("resend replay");
    match sock.recv_from(&mut buf) {
        Ok((n, _)) => panic!("replayed packet must be dropped, got {n} bytes"),
        Err(e) => eprintln!("replay correctly rejected (recv timed out: {e})"),
    }
    println!("UDP-2022-SERVER-OK");
}
