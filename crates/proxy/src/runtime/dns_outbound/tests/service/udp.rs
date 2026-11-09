//! The real SOCKS UDP handshake and negotiated relay keep health tests on UDP.
use super::*;
use tokio::net::UdpSocket;
pub(super) async fn socks(client: TcpStream, expected: SocketAddr) {
    let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let bound = relay.local_addr().unwrap();
    let mut client = zero_platform_tokio::TokioSocket::new(client);
    assert!(matches!(
        ::socks5::Socks5Inbound
            .accept_command(&mut client)
            .await
            .unwrap(),
        ::socks5::Socks5Request::UdpAssociate(_)
    ));
    ::socks5::Socks5Inbound
        .send_success_response_with_bound(
            &mut client,
            &zero_core::Address::Ipv4([127, 0, 0, 1]),
            bound.port(),
        )
        .await
        .unwrap();
    let mut client = client.into_inner().unwrap();
    let codec = ::socks5::udp::Socks5InboundUdpCodec;
    let mut receiver = None;
    let mut bytes = vec![0; 65536];
    let mut extra = [0; 1];
    loop {
        tokio::select! { biased;
            result = client.read(&mut extra) => { assert_eq!(result.unwrap(),0); break; }
            result = relay.recv_from(&mut bytes) => {
                let (size, peer) = result.unwrap();
                if peer == expected {
                    let response = codec.encode_response_to_client(&zero_core::Address::Ipv4([127,0,0,1]),expected.port(),&bytes[..size]).unwrap();
                    relay.send_to(&response,receiver.unwrap()).await.unwrap();
                } else {
                    if let Some(actual) = receiver { assert_eq!(actual,peer); }
                    receiver=Some(peer);
                    let packet=codec.decode_request(&bytes[..size]).unwrap();
                    assert_eq!(packet.target(),&zero_core::Address::Ipv4([127,0,0,1]));
                    assert_eq!(packet.port(),expected.port());
                    relay.send_to(packet.payload(),expected).await.unwrap();
                }
            }
        }
    }
    client.shutdown().await.unwrap();
}
