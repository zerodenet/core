use super::*;
#[tokio::test]
async fn actual_owned_custom_and_directional_sudoku_keep_full_payload_half_close_and_original_material(
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (book, metadata) = roots();
        let before = metadata.snapshot();
        let settings = crate::finalmask::Settings {
            udp: Vec::new(),
            tcp: vec![
                custom(),
                Mask::Sudoku(super::super::super::sudoku::Settings {
                    password: "secret".into(),
                    custom_tables: vec!["xxppvvvv".into(), "xpxpvvvv".into()],
                    padding_min: 50,
                    padding_max: 50,
                    ..Default::default()
                }),
            ],
        };
        let profile =
            crate::finalmask::Profile::new_owned(settings, book.clone(), metadata.clone()).unwrap();
        let masks = profile.tcp().clone();
        drop(profile);
        let client = masks.prepare_owned(book.clone(), metadata.clone()).unwrap();
        let server = masks.prepare_owned(book.clone(), metadata.clone()).unwrap();
        let (client_io, server_io) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let mut stream = server
                .wrap(TcpRelayStream::new(server_io), true)
                .await
                .unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            stream.write_all(&received).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let mut stream = client
            .wrap(TcpRelayStream::new(client_io), false)
            .await
            .unwrap();
        let expected = vec![0x3a; 131073];
        stream.write_all(&expected).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut result = Vec::new();
        stream.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, expected);
        drop(stream);
        server.await.unwrap();
        assert!(
            book.used_bytes() > 0,
            "the actual immutable tables are still held by their last recipe clone"
        );
        drop(masks);
        assert_eq!(book.used_bytes(), 0);
        assert_eq!(metadata.snapshot(), before);
    })
    .await
    .expect("same original 10s wire/half-close deadline");
}
#[tokio::test]
async fn actual_owned_first_hello_keeps_original_record_splits_and_coalesced_tail() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (book, metadata) = roots();
        let before = metadata.snapshot();
        let masks =
            PreparedMasks::new_owned(&[fragmented()], book.clone(), metadata.clone()).unwrap();
        let prepared = masks.prepare_owned(book.clone(), metadata.clone()).unwrap();
        drop(masks);
        let (raw, mut peer) = tokio::io::duplex(4096);
        let mut stream = prepared
            .wrap(TcpRelayStream::new(raw), false)
            .await
            .unwrap();
        stream
            .write_all(&[22, 3, 1, 0, 6, 1, 2, 3, 4, 5, 6, 99, 100])
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut wire = Vec::new();
        peer.read_to_end(&mut wire).await.unwrap();
        assert_eq!(
            wire,
            [22, 3, 1, 0, 2, 1, 2, 22, 3, 1, 0, 4, 3, 4, 5, 6, 99, 100]
        );
        assert!(
            book.used_bytes() > 0,
            "read view and original inline driver are still retained after TX half-close"
        );
        drop(stream);
        drop(peer);
        assert_eq!(book.used_bytes(), 0);
        assert_eq!(metadata.snapshot(), before);
    })
    .await
    .unwrap();
}
