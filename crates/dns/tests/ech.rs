#![cfg(feature = "udp")]

use tokio::net::UdpSocket;

fn question_end(message: &[u8]) -> usize {
    let mut offset = 12;
    while message[offset] != 0 {
        offset += usize::from(message[offset]) + 1;
    }
    offset + 5
}

fn https_response(query: &[u8], material: &[u8], ttl: u32) -> Vec<u8> {
    let question_end = question_end(query);
    let mut response = Vec::from(&query[..question_end]);
    response[2] = 0x81;
    response[3] = 0x80;
    response[6..8].copy_from_slice(&1_u16.to_be_bytes());
    response[8..12].fill(0);
    response.extend_from_slice(&[0xc0, 0x0c, 0, 65, 0, 1]);
    response.extend_from_slice(&ttl.to_be_bytes());
    let mut rdata = vec![0, 1, 0, 0, 5];
    rdata.extend_from_slice(&(material.len() as u16).to_be_bytes());
    rdata.extend_from_slice(material);
    response.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
    response.extend_from_slice(&rdata);
    response
}

#[tokio::test]
async fn queries_https_record_and_extracts_ech_service_parameter() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let material = vec![0, 4, 0xfe, 0x0d, 0, 0];
    let expected = material.clone();
    let server = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        let (length, peer) = socket.recv_from(&mut buffer).await.unwrap();
        let question_end = question_end(&buffer[..length]);
        assert_eq!(&buffer[question_end - 4..question_end - 2], &[0, 65]);
        socket
            .send_to(&https_response(&buffer[..length], &expected, 321), peer)
            .await
            .unwrap();
    });

    let dns = zero_dns::DnsSystem::build(None).unwrap();
    let answer = dns
        .query_ech_config("secret.example", &format!("udp://{address}"))
        .await
        .unwrap();
    assert_eq!(answer.config_list, Some(material));
    assert_eq!(answer.ttl_seconds, 321);
    server.await.unwrap();
}
