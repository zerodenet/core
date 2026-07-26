//! Tests for Hysteria2 protocol constants and helpers.
//!
//! Migrated from the inline `#[cfg(test)] mod tests` in
//! `protocols/hysteria2/src/shared.rs`.

use hysteria2::shared::{
    build_auth_error, build_auth_frame, build_auth_ok, build_connect_error, build_connect_ok,
    build_tcp_connect_header, parse_auth_frame, parse_auth_response, parse_tcp_connect_header,
};
use zero_core::Address;

#[test]
fn test_auth_frame_roundtrip() {
    let hmac = [0xAAu8; 32];
    let frame = build_auth_frame(&hmac);
    assert_eq!(frame[0], 0x02);
    let parsed = parse_auth_frame(&frame).unwrap();
    assert_eq!(parsed, hmac);
}

#[test]
fn test_auth_response_ok() {
    let resp = build_auth_ok();
    assert!(parse_auth_response(&resp).is_ok());
}

#[test]
fn test_auth_response_err() {
    let resp = build_auth_error("bad password");
    assert!(parse_auth_response(&resp).is_err());
}

#[test]
fn test_tcp_connect_header_roundtrip() {
    let addr = Address::Domain("example.com".into());
    let header = build_tcp_connect_header(&addr, 443).unwrap();
    let (parsed_addr, parsed_port) = parse_tcp_connect_header(&header).unwrap();
    assert_eq!(parsed_addr, addr);
    assert_eq!(parsed_port, 443);
    assert_eq!(&header[..2], &[0x44, 0x01]);
    assert!(header
        .windows(b"example.com:443".len())
        .any(|part| part == b"example.com:443"));
}

#[test]
fn test_tcp_connect_ipv4() {
    let addr = Address::Ipv4([127, 0, 0, 1]);
    let header = build_tcp_connect_header(&addr, 80).unwrap();
    let (parsed_addr, parsed_port) = parse_tcp_connect_header(&header).unwrap();
    assert_eq!(parsed_addr, addr);
    assert_eq!(parsed_port, 80);
}

#[test]
fn test_connect_response_ok() {
    let resp = build_connect_ok();
    assert_eq!(resp, [0x00, 0x00, 0x00]);
}

#[test]
fn test_connect_response_err() {
    let resp = build_connect_error("connection refused");
    assert_eq!(resp[0], 0x01);
    let msg_len = usize::from(resp[1]);
    assert_eq!(&resp[2..2 + msg_len], b"connection refused");
    assert_eq!(resp[2 + msg_len], 0x00);
}
