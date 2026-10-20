use super::fixtures::*;
use shadowsocks::transport::ShadowsocksTransportState;
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_platform_tokio::contract::{context::ConnectFact, stream::ConnectRequest};
#[tokio::test]
async fn actual_authenticated_wrong_request_echo_is_rejected_before_it_can_pollute_shared_response_history(
) {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for wrong in [true, false] {
                let (io, _) = listener.accept().await.unwrap();
                let mut io = zero_platform_tokio::TcpRelayStream::new(io);
                let profile = shadowsocks::ShadowsocksInboundProfile::from_config(
                    "2022-blake3-aes-128-gcm",
                    KEY2022,
                )
                .unwrap();
                let (mut accept, _) = profile
                    .accept_request(&shadowsocks::ShadowsocksInbound, &mut io)
                    .await
                    .unwrap();
                if wrong {
                    accept.request_salt[0] ^= 1;
                }
                let mut io = accept
                    .into_aead_stream_with_response_salt(io, KEY2022.as_bytes(), vec![9; 16])
                    .unwrap();
                let mut body = Vec::new();
                io.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, b"payload");
                io.write_all(b"reply").await.unwrap();
                io.shutdown().await.unwrap();
            }
        });
        let state = ShadowsocksTransportState::default();
        let assembly = assembly(&state, address, "2022-blake3-aes-128-gcm", KEY2022, 2, None);
        assembly.start().await;
        for index in 0..2 {
            let observations = Arc::new(Observations::default());
            let role = assembly.streams[index].stream(deadline()).await.unwrap();
            let mut connected = role
                .connect(
                    ConnectRequest {
                        destination: destination(),
                    },
                    context(address, observations.clone()),
                )
                .await
                .unwrap();
            connected.io.write_all(b"payload").await.unwrap();
            connected.io.shutdown().await.unwrap();
            let mut body = Vec::new();
            let result = connected.io.read_to_end(&mut body).await;
            if index == 0 {
                let error = result.unwrap_err();
                assert!(error.to_string().contains("request salt mismatch"));
                assert!(body.is_empty());
                assert_eq!(
                    observations.facts.lock().unwrap().last(),
                    Some(&(None, ConnectFact::PeerRejected))
                );
            } else {
                result.unwrap();
                assert_eq!(body, b"reply");
                assert_eq!(
                    observations.facts.lock().unwrap().last(),
                    Some(&(None, ConnectFact::PeerConfirmed))
                );
            }
        }
        server.await.unwrap();
        assembly.close().await;
    })
    .await
    .unwrap();
}
