//! Exercise complete AEAD DATA followed by upload EOF and a larger reverse body.
use super::*;

#[tokio::test]
async fn actual_bidirectional_half_close_keeps_every_chunk_with_a_small_carrier_buffer() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        for cipher in [VmessCipher::Aes128Gcm, VmessCipher::Chacha20Poly1305] {
            let (client, server) = tokio::io::duplex(1024);
            let upload = build_payload(256 * 1024);
            let expected = upload.clone();
            let server = async move {
                let profile = VmessInboundProfile::from_config_users([(
                    "11111111-2222-3333-4444-555555555555".to_owned(),
                    cipher.name().to_owned(),
                    None::<String>,
                    None::<u64>,
                    None::<u64>,
                    None::<u32>,
                    None::<u64>,
                    None::<u64>,
                )])
                .unwrap();
                let (_, mut stream) = profile
                    .accept_tcp_stream(VmessInbound, TestSocket(server))
                    .await
                    .unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(
                    received == expected,
                    "complete upload before response: {} of {} bytes",
                    received.len(),
                    expected.len()
                );
                stream.write_all(&received).await.unwrap();
                stream.shutdown().await.unwrap();
            };
            let client = async move {
                let request = PreparedVmessOutboundRequestBundle::from_config(
                    "11111111-2222-3333-4444-555555555555",
                    cipher.name(),
                    None,
                )
                .unwrap();
                let target = Session::new(
                    0,
                    Address::Ipv4([127, 0, 0, 1]),
                    443,
                    Network::Tcp,
                    ProtocolType::new("vmess"),
                );
                let (mut stream, _) = request
                    .establish_tcp_outbound_stream(TestSocket(client), &target)
                    .await
                    .unwrap();
                stream.write_all(&upload).await.unwrap();
                stream.shutdown().await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert!(
                    received == upload,
                    "complete reverse after upload EOF: {} of {} bytes, first difference {:?}",
                    received.len(),
                    upload.len(),
                    received
                        .iter()
                        .zip(&upload)
                        .position(|(left, right)| left != right)
                );
            };
            tokio::join!(client, server);
        }
    })
    .await
    .expect("original eight-second complete half-close deadline");
}
