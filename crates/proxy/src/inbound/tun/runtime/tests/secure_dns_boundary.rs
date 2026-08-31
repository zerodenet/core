use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{Address, Network, ProtocolType, Session, TargetHostSource};

use super::serialized_client_hello;
use crate::inbound::tun::sniff::sniff_tcp_target;

#[tokio::test]
async fn application_doh_remains_opaque_and_only_exposes_the_endpoint_sni() {
    let mut original = serialized_client_hello("cloudflare-dns.com");
    // Application-owned DoH is encrypted application data on an ordinary
    // port-443 flow. The public endpoint SNI may supplement routing, but the
    // hidden DNS question is never treated as intercepted port-53 traffic.
    original.extend_from_slice(&[
        0x17, 0x03, 0x03, 0x00, 0x08, 0x8f, 0x42, 0x11, 0x6a, 0x00, 0xfe, 0x7d, 0x93,
    ]);
    let (mut writer, reader) = tokio::io::duplex(original.len() * 2);
    writer
        .write_all(&original)
        .await
        .expect("write opaque application DoH flow");
    writer.shutdown().await.expect("close writer");
    let original_target = Address::Ipv4([1, 1, 1, 1]);
    let session = Session::new(
        0,
        original_target.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );

    let (session, mut stream) = sniff_tcp_target(session, reader).await;

    assert_eq!(
        session.target,
        Address::Domain("cloudflare-dns.com".to_owned())
    );
    assert_eq!(session.sni.as_deref(), Some("cloudflare-dns.com"));
    assert_eq!(session.original_target, Some(original_target.clone()));
    assert_eq!(session.direct_target, Some(original_target));
    assert_eq!(session.target_host_source, Some(TargetHostSource::TlsSni));
    let mut replayed = Vec::new();
    stream
        .read_to_end(&mut replayed)
        .await
        .expect("read replayed application DoH bytes");
    assert_eq!(replayed, original);
}

#[tokio::test]
async fn ech_keeps_the_ip_target_and_never_promotes_the_outer_public_name() {
    let original = serialized_ech_client_hello("public.example");
    let (mut writer, reader) = tokio::io::duplex(original.len() * 2);
    writer
        .write_all(&original)
        .await
        .expect("write ECH ClientHello");
    writer.shutdown().await.expect("close writer");
    let original_target = Address::Ipv4([203, 0, 113, 9]);
    let session = Session::new(
        0,
        original_target.clone(),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );

    let (session, mut stream) = sniff_tcp_target(session, reader).await;

    assert_eq!(session.target, original_target);
    assert!(session.original_target.is_none());
    assert!(session.direct_target.is_none());
    assert!(session.sni.is_none());
    assert!(session.target_host_source.is_none());
    let mut replayed = Vec::new();
    stream
        .read_to_end(&mut replayed)
        .await
        .expect("read replayed ECH ClientHello");
    assert_eq!(replayed, original);
}

fn serialized_ech_client_hello(outer_sni: &str) -> Vec<u8> {
    let hostname = outer_sni.as_bytes();
    let mut server_name = Vec::new();
    server_name.extend_from_slice(&((hostname.len() + 3) as u16).to_be_bytes());
    server_name.push(0);
    server_name.extend_from_slice(&(hostname.len() as u16).to_be_bytes());
    server_name.extend_from_slice(hostname);

    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0_u16.to_be_bytes());
    extensions.extend_from_slice(&(server_name.len() as u16).to_be_bytes());
    extensions.extend_from_slice(&server_name);
    extensions.extend_from_slice(&0xfe0d_u16.to_be_bytes());
    extensions.extend_from_slice(&0_u16.to_be_bytes());

    let mut client_hello = Vec::new();
    client_hello.extend_from_slice(&[0x03, 0x03]);
    client_hello.extend_from_slice(&[0_u8; 32]);
    client_hello.push(0);
    client_hello.extend_from_slice(&2_u16.to_be_bytes());
    client_hello.extend_from_slice(&[0x13, 0x01]);
    client_hello.extend_from_slice(&[1, 0]);
    client_hello.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    client_hello.extend_from_slice(&extensions);

    let mut handshake = vec![0x01];
    push_u24(&mut handshake, client_hello.len());
    handshake.extend_from_slice(&client_hello);

    let mut record = vec![0x16, 0x03, 0x01];
    record.extend_from_slice(&(handshake.len() as u16).to_be_bytes());
    record.extend_from_slice(&handshake);
    record
}

fn push_u24(output: &mut Vec<u8>, value: usize) {
    assert!(value <= 0x00ff_ffff);
    output.extend_from_slice(&[
        ((value >> 16) & 0xff) as u8,
        ((value >> 8) & 0xff) as u8,
        (value & 0xff) as u8,
    ]);
}
