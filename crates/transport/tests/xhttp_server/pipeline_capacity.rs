use super::*;
use crate::split_http::{accept_xhttp_connection, SplitHttpRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

// Captured from the fixed reference's two HTTP/1 upload connections. Each
// connection is ordered; their cross-connection arrival order is different.
const FIRST: &[u64] = &[
    0, 1, 2, 3, 14, 15, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 39, 40, 41, 43, 47, 55,
    56, 57, 58, 59, 60, 65,
];

#[tokio::test]
async fn pipelined_upload_backpressure_does_not_discard_received_missing_sequences() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..8 {
            round_trip().await;
        }
    })
    .await
    .expect("bounded pipelined uploads must keep making progress");
}

async fn round_trip() {
    let options = crate::profile::OwnedSplitHttpProfile {
        host: Some("example.com".into()),
        path: "/tunnel/".into(),
        mode: "auto".into(),
        options: zero_traits::SplitHttpOptions {
            session_placement: "header".into(),
            seq_placement: "cookie".into(),
            uplink_data_placement: "header".into(),
            uplink_http_method: "PUT".into(),
            sc_max_buffered_posts: 8,
            sc_max_each_post_bytes: zero_traits::SplitHttpRange::new(2048, 2048),
            ..Default::default()
        },
    };
    let profile = Profile::new(&options);
    let registry = SplitHttpRegistry::default();
    let mut sources = Vec::new();
    let mut downloads = Vec::new();
    let mut readers = tokio::task::JoinSet::new();
    for index in 0..4 {
        let id = format!("session-{index}");
        let request = profile.request("GET", &id, None, Body::empty()).unwrap();
        let (mut client, server) = tokio::io::duplex(1024 * 1024);
        client.write_all(&headers(&request)).await.unwrap();
        let incoming = accept_xhttp_connection(server, &options, &registry);
        let mut stream = incoming.accept().await.unwrap();
        readers.spawn(async move {
            for sequence in 0..66u8 {
                let mut data = [0; 2048];
                stream.read_exact(&mut data).await.unwrap();
                assert!(data.iter().all(|byte| *byte == sequence));
            }
        });
        sources.push(incoming);
        downloads.push(client);
    }
    let mut replies = tokio::task::JoinSet::new();
    // Buffer both independent lanes before the executor schedules their HTTP
    // drivers. This exercises the same listener permits and real Hyper parser.
    for lane in 0..2 {
        for index in 0..4 {
            let id = format!("session-{index}");
            let mut bytes = Vec::new();
            for sequence in 0..66u64 {
                if FIRST.contains(&sequence) != (lane == 0) {
                    continue;
                }
                let request = profile
                    .packet_request(&id, sequence, &[sequence as u8; 2048])
                    .unwrap();
                bytes.extend_from_slice(&headers(&request));
            }
            let (mut client, server) = tokio::io::duplex(1024 * 1024);
            client.write_all(&bytes).await.unwrap();
            client.shutdown().await.unwrap();
            sources.push(accept_xhttp_connection(server, &options, &registry));
            replies.spawn(async move { check_replies(client).await });
        }
    }
    while let Some(reply) = replies.join_next().await {
        reply.unwrap();
    }
    while let Some(read) = readers.join_next().await {
        read.unwrap();
    }
    drop((sources, downloads));
}

fn headers<B>(request: &http::Request<B>) -> Vec<u8> {
    let mut wire = format!("{} {} HTTP/1.1\r\n", request.method(), request.uri());
    for (name, value) in request.headers() {
        wire.push_str(name.as_str());
        wire.push_str(": ");
        wire.push_str(value.to_str().unwrap());
        wire.push_str("\r\n");
    }
    if !request.headers().contains_key("content-length") {
        wire.push_str("Content-Length: 0\r\n");
    }
    wire.push_str("\r\n");
    wire.into_bytes()
}
async fn check_replies(mut client: DuplexStream) {
    let mut responses = Vec::new();
    client.read_to_end(&mut responses).await.unwrap();
    let responses = String::from_utf8(responses).unwrap();
    for response in responses.split("HTTP/1.1 ").skip(1) {
        assert!(
            response.starts_with("200 "),
            "received upload was discarded: {}",
            &response[..response.len().min(40)]
        );
    }
}
