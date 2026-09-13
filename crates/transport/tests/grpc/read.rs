use super::*;

#[test]
fn protobuf_rejects_overflowing_varints_and_invalid_field_numbers() {
    assert!(decode_grpc_hunk(&[0, 0], false).is_err());
    assert!(
        decode_grpc_hunk(&[10, 255, 255, 255, 255, 255, 255, 255, 255, 255, 2], false).is_err()
    );
    let mut input = &[255, 255, 255, 255, 255, 255, 255, 255, 255, 1][..];
    assert_eq!(codec::read_varint(&mut input).unwrap(), u64::MAX);
}

#[tokio::test]
async fn response_status_and_truncated_messages_are_errors_after_buffered_data() {
    for case in ["status", "truncated", "missing", "headers", "http", "ok"] {
        let (client, server) = tokio::io::duplex(8192);
        let backend = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server).await.unwrap();
            let (_request, mut response) = connection.accept().await.unwrap().unwrap();
            let mut headers = http::Response::builder().header("content-type", "application/grpc");
            if case == "headers" {
                headers = headers
                    .header("grpc-status", "7")
                    .header("grpc-message", "not%20allowed");
            }
            if case == "http" {
                headers = headers.status(502);
            }
            let mut stream = response
                .send_response(headers.body(()).unwrap(), false)
                .unwrap();
            if case != "headers" && case != "http" {
                let payload = encode_grpc_hunk(b"before-error");
                let mut wire = grpc_frame_header(payload.len()).to_vec();
                wire.extend(payload);
                if case == "truncated" {
                    wire.extend([0, 0, 0]);
                }
                stream.send_data(Bytes::from(wire), false).unwrap();
            }
            if case == "missing" || case == "headers" || case == "http" {
                stream.send_data(Bytes::new(), true).unwrap();
            } else {
                let mut trailers = http::HeaderMap::new();
                trailers.insert(
                    "grpc-status",
                    if case == "status" { "13" } else { "0" }.parse().unwrap(),
                );
                trailers.insert("grpc-message", "backend%20failed".parse().unwrap());
                stream.send_trailers(trailers).unwrap();
            }
            while connection.accept().await.is_some() {}
        });
        let profile = OwnedGrpcProfile {
            service_names: vec!["service".into()],
            ..Default::default()
        };
        let mut stream = connect_grpc_with_profile(client, &profile, "localhost")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        let result = timeout(Duration::from_secs(2), stream.read_to_end(&mut bytes))
            .await
            .unwrap();
        if case == "ok" {
            result.unwrap();
        } else {
            let error = result.expect_err(case);
            if case == "truncated" {
                assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
            }
            if case == "status" {
                assert!(error.to_string().contains("backend failed"));
            }
        }
        assert_eq!(
            bytes,
            if case == "headers" || case == "http" {
                &b""[..]
            } else {
                &b"before-error"[..]
            }
        );
        drop(stream);
        backend.abort();
    }
}
