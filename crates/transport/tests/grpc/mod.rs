use super::*;
use crate::profile::OwnedGrpcProfile;
use tokio::time::{timeout, Duration};

#[test]
fn official_paths_and_multi_hunk_semantics_are_distinct() {
    assert_eq!(
        options::service_path("name with space", false),
        "/name%20with%20space/Tun"
    );
    assert_eq!(
        options::service_path("/deep/service/One|Many", true),
        "/deep/service/Many"
    );
    assert_eq!(
        options::service_path("/deep/service/One|Many", false),
        "/deep/service/One"
    );
    let bytes = [10, 1, b'a', 10, 1, b'b'];
    assert_eq!(decode_grpc_hunk(&bytes, false).unwrap(), b"b");
    assert_eq!(decode_grpc_hunk(&bytes, true).unwrap(), b"ab");
    let profile = OwnedGrpcProfile {
        service_names: vec!["service".into()],
        authority: Some("front.example:8443".into()),
        multi_mode: true,
        user_agent: Some("firefox".into()),
        ..Default::default()
    };
    let request = options::request(&profile, "backend.example").unwrap();
    assert_eq!(
        request.uri().authority().unwrap().as_str(),
        "front.example:8443"
    );
    assert_eq!(request.uri().path(), "/service/TunMulti");
    assert!(request.headers()["user-agent"]
        .to_str()
        .unwrap()
        .contains("Firefox/140.0"));
}
#[tokio::test]
async fn one_h2_connection_accepts_multiple_routes_and_drains_flow_controlled_writes() {
    timeout(Duration::from_secs(10), async {
        let (client, server) = tokio::io::duplex(8192);
        let profile = OwnedGrpcProfile {
            service_names: vec!["service".into()],
            ..Default::default()
        };
        let incoming = accept_grpc_connection(server, &profile).unwrap();
        let server = tokio::spawn(async move {
            let mut routes = tokio::task::JoinSet::new();
            for _ in 0..3 {
                let mut stream = incoming.accept().await.unwrap();
                routes.spawn(async move {
                    let mut bytes = Vec::new();
                    stream.read_to_end(&mut bytes).await.unwrap();
                    AsyncWriteExt::write_all(&mut stream, &bytes).await.unwrap();
                    AsyncWriteExt::shutdown(&mut stream).await.unwrap();
                });
            }
            while let Some(result) = routes.join_next().await {
                result.unwrap();
            }
            // Allow final H2 data/trailers to drain before closing the owner.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let (sender, connection) = h2::client::handshake(client).await.unwrap();
        let driver = tokio::spawn(connection);
        let mut routes = tokio::task::JoinSet::new();
        for id in 0..3 {
            let mut sender = sender.clone().ready().await.unwrap();
            let profile = OwnedGrpcProfile {
                multi_mode: id == 1,
                ..profile.clone()
            };
            let (response, send) = sender
                .send_request(options::request(&profile, "localhost").unwrap(), false)
                .unwrap();
            routes.spawn(async move {
                let mut stream = build_grpc_client_stream(send, response, profile.multi_mode);
                let payload = vec![id as u8; 196_613];
                AsyncWriteExt::write_all(&mut stream, &payload)
                    .await
                    .unwrap();
                AsyncWriteExt::shutdown(&mut stream).await.unwrap();
                let mut received = Vec::new();
                stream.read_to_end(&mut received).await.unwrap();
                assert_eq!(received, payload);
            });
        }
        while let Some(result) = routes.join_next().await {
            result.unwrap();
        }
        server.await.unwrap();
        driver.abort();
    })
    .await
    .expect("gRPC multiplexing or flow-control stalled");
}

#[tokio::test]
async fn pool_reuses_one_connection_and_retirement_preserves_admitted_streams() {
    timeout(Duration::from_secs(10), async {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let count = Arc::new(AtomicUsize::new(0));
        let profile = OwnedGrpcProfile {
            service_names: vec!["service".into()],
            ..Default::default()
        };
        let pool = GrpcPool::default();
        let open = || {
            count.fetch_add(1, Ordering::Relaxed);
            let (client, server) = tokio::io::duplex(8192);
            let incoming = accept_grpc_connection(server, &profile).unwrap();
            tokio::spawn(async move {
                while let Some(mut stream) = incoming.accept().await {
                    tokio::spawn(async move {
                        let mut byte = [0];
                        while stream.read_exact(&mut byte).await.is_ok() {
                            if AsyncWriteExt::write_all(&mut stream, &byte).await.is_err() {
                                break;
                            }
                            if AsyncWriteExt::flush(&mut stream).await.is_err() {
                                break;
                            }
                        }
                    });
                }
            });
            async { Ok::<_, RuntimeError>(client) }
        };
        let mut first = pool.open(&profile, "localhost", open).await.unwrap();
        let mut second = pool.open(&profile, "localhost", open).await.unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1);
        pool.retire();
        let mut fresh = pool.open(&profile, "localhost", open).await.unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 2);
        for (id, stream) in [&mut first, &mut second, &mut fresh]
            .into_iter()
            .enumerate()
        {
            AsyncWriteExt::write_all(stream, &[id as u8]).await.unwrap();
            AsyncWriteExt::flush(stream).await.unwrap();
            let mut received = [0];
            stream.read_exact(&mut received).await.unwrap();
            assert_eq!(received, [id as u8]);
        }
    })
    .await
    .expect("gRPC pool lifetime stalled");
}

mod read;
mod reconnect;

#[test]
fn unknown_protobuf_groups_are_skipped_without_consuming_outer_hunks() {
    let wire = [
        0x0a, 1, b'a', 0x13, 0x0a, 1, b'x', 0x1b, 0x20, 1, 0x1c, 0x14, 0x0a, 1, b'b',
    ];
    assert_eq!(codec::decode_grpc_hunk(&wire, true).unwrap(), b"ab");
    assert_eq!(codec::decode_grpc_hunk(&wire, false).unwrap(), b"b");
    for invalid in [
        &[0x13, 0x1c][..],
        &[0x14],
        &[0x13, 0x10, 1],
        &[
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1,
        ],
    ] {
        assert!(codec::decode_grpc_hunk(invalid, true).is_err());
    }
    assert_eq!(
        options::service_path("has'apostrophe/and?slash", false),
        "/has%27apostrophe%2Fand%3Fslash/Tun"
    );
}
