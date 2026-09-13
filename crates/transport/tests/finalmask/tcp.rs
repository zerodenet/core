use super::*;
use tokio::time::timeout;
fn custom() -> Mask {
    Mask::Custom(Custom {
        clients: vec![vec![Item {
            delay_ms: Range::default(),
            content: super::super::udp::Item::Bytes(b"client".to_vec()),
        }]],
        servers: vec![vec![Item {
            delay_ms: Range::default(),
            content: super::super::udp::Item::Bytes(b"server".to_vec()),
        }]],
        errors: Vec::new(),
    })
}
#[tokio::test]
async fn custom_and_directional_sudoku_preserve_payload_and_half_close() {
    let stage = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    let started = std::time::Instant::now();
    timeout(Duration::from_secs(10), async {
        let masks = vec![
            custom(),
            Mask::Sudoku(super::super::sudoku::Settings {
                password: "secret".into(),
                custom_tables: vec!["xxppvvvv".into(), "xpxpvvvv".into()],
                padding_min: 50,
                padding_max: 50,
                ..Default::default()
            }),
        ];
        let (client, server) = tokio::io::duplex(4096);
        let masks = PreparedMasks::new(&masks).unwrap();
        let server_masks = masks.clone();
        let server = tokio::spawn(async move {
            let mut stream = wrap_prepared(TcpRelayStream::new(server), &server_masks, true)
                .await
                .unwrap();
            let mut bytes = Vec::new();
            stream.read_to_end(&mut bytes).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let mut stream = wrap_prepared(TcpRelayStream::new(client), &masks, false)
            .await
            .unwrap();
        stage.store(1, std::sync::atomic::Ordering::Relaxed);
        let expected = vec![0x3a; 131073];
        stream.write_all(&expected).await.unwrap();
        stage.store(2, std::sync::atomic::Ordering::Relaxed);
        stream.shutdown().await.unwrap();
        stage.store(3, std::sync::atomic::Ordering::Relaxed);
        let mut result = Vec::new();
        stream.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, expected);
        server.await.unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Sudoku stage {} timed out after {:?}",
            stage.load(std::sync::atomic::Ordering::Relaxed),
            started.elapsed()
        )
    });
}
#[tokio::test]
async fn first_hello_is_split_into_tls_records_and_preserves_coalesced_tail() {
    let (client, mut server) = tokio::io::duplex(4096);
    let masks = [Mask::Fragment(Fragment {
        packets: Range {
            minimum: 0,
            maximum: 1,
        },
        length: Range {
            minimum: 2,
            maximum: 2,
        },
        delay_ms: Range::default(),
        max_splits: Range {
            minimum: 2,
            maximum: 2,
        },
    })];
    let mut client = wrap(TcpRelayStream::new(client), &masks, false)
        .await
        .unwrap();
    client
        .write_all(&[22, 3, 1, 0, 6, 1, 2, 3, 4, 5, 6, 99, 100])
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    let mut wire = Vec::new();
    server.read_to_end(&mut wire).await.unwrap();
    assert_eq!(
        wire,
        [22, 3, 1, 0, 2, 1, 2, 22, 3, 1, 0, 4, 3, 4, 5, 6, 99, 100]
    );
}
#[tokio::test]
async fn invalid_custom_header_emits_error_sequence_and_rejects_carrier() {
    let (mut client, server) = tokio::io::duplex(4096);
    let Mask::Custom(mut settings) = custom() else {
        unreachable!()
    };
    settings.errors = vec![vec![Item {
        delay_ms: Range::default(),
        content: super::super::udp::Item::Bytes(b"denied".to_vec()),
    }]];
    let server = tokio::spawn(async move {
        wrap(TcpRelayStream::new(server), &[Mask::Custom(settings)], true)
            .await
            .is_err()
    });
    client.write_all(b"wrong!").await.unwrap();
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"denied");
    assert!(server.await.unwrap());
}
