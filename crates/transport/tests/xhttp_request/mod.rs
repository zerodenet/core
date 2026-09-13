use super::*;
use crate::profile::OwnedSplitHttpProfile;
use http_body_util::BodyExt;
fn profile() -> OwnedSplitHttpProfile {
    OwnedSplitHttpProfile {
        host: Some("example.com".into()),
        path: "/x?keep=1".into(),
        mode: "packet-up".into(),
        options: Default::default(),
    }
}
#[tokio::test]
async fn metadata_cross_product_roundtrips_and_preserves_encoded_query_keys() {
    for session in ["path", "query", "header", "cookie"] {
        for seq in ["path", "query", "header", "cookie"] {
            let mut config = profile();
            config.options.session_placement = session.into();
            config.options.seq_placement = seq.into();
            config.options.session_key = if session == "query" {
                "s p"
            } else {
                "X-Session-Id"
            }
            .into();
            let p = Profile::new(&config);
            let r = p.packet_request("abc-123", 7, b"payload").unwrap();
            assert_eq!(p.meta(&r).unwrap(), ("abc-123".into(), Some(7)));
            assert!(r.uri().query().unwrap().contains("keep=1"));
            p.validate_padding(&r).unwrap();
            assert_eq!(r.into_body().collect().await.unwrap().to_bytes(), "payload");
        }
    }
}
#[tokio::test]
async fn payload_chunks_are_urlsafe_and_auto_concatenates_all_sources_in_wire_order() {
    let mut config = profile();
    config.options.uplink_data_placement = "header".into();
    config.options.uplink_chunk_size = SplitHttpRange::new(64, 64);
    let p = Profile::new(&config);
    let payload: Vec<u8> = (0..201).map(|i| i as u8).collect();
    let mut request = p.packet_request("session", 0, &payload).unwrap();
    assert!(request.headers().contains_key("x-data-4"));
    assert_eq!(p.packet_prefix(&request).unwrap(), payload);
    config.options.uplink_data_placement = "cookie".into();
    config.options.uplink_data_key = "X-Data".into();
    let cookie_profile = Profile::new(&config);
    let cookies = cookie_profile
        .packet_request("session", 0, b"cookies")
        .unwrap();
    request
        .headers_mut()
        .insert("cookie", cookies.headers()["cookie"].clone());
    config.options.uplink_data_placement = "auto".into();
    let p = Profile::new(&config);
    let expected = [payload.as_slice(), b"cookies"].concat();
    assert_eq!(p.packet_prefix(&request).unwrap(), expected);
    request
        .headers_mut()
        .insert("x-data-0", "not=base64".parse().unwrap());
    assert!(p.packet_prefix(&request).is_err());
}
#[test]
fn all_padding_locations_validate_actual_hpack_length_and_reject_out_of_range() {
    for placement in ["header", "cookie", "query", "queryInHeader"] {
        for method in ["repeat-x", "tokenish"] {
            let mut c = profile();
            c.options.x_padding_obfs_mode = true;
            c.options.x_padding_placement = placement.into();
            c.options.x_padding_method = method.into();
            c.options.x_padding_bytes = SplitHttpRange::new(200, 200);
            let p = Profile::new(&c);
            for _ in 0..20 {
                let r = p.request("GET", "id", None, Body::empty()).unwrap();
                p.validate_padding(&r).unwrap();
                c.options.x_padding_bytes = SplitHttpRange::new(300, 300);
                assert!(Profile::new(&c).validate_padding(&r).is_err());
            }
        }
    }
}
#[test]
fn cors_preflight_reflects_origin_method_headers_and_cookie_credentials() {
    let mut c = profile();
    c.options.session_placement = "cookie".into();
    let p = Profile::new(&c);
    let r = Request::builder()
        .method("OPTIONS")
        .uri("/x/")
        .header("origin", "https://example.com")
        .header("access-control-request-method", "PUT")
        .header("access-control-request-headers", "X-Seq, X-Data-0")
        .body(())
        .unwrap();
    let h = p.response_headers(&r).unwrap();
    assert_eq!(h["access-control-allow-origin"], "https://example.com");
    assert_eq!(h["access-control-allow-methods"], "PUT");
    assert_eq!(h["access-control-allow-headers"], "X-Seq, X-Data-0");
    assert_eq!(h["access-control-allow-credentials"], "true");
}

#[tokio::test]
async fn full_accept_queue_releases_the_rejected_download_session() {
    use crate::split_http::{server::exchange, sessions::Sessions};
    let profile = Profile::new(&profile());
    let sessions = Sessions::default();
    let (accepted, _receiver) = tokio::sync::mpsc::channel(1);
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
    let first = profile
        .request("GET", "first", None, Body::empty())
        .unwrap();
    let response = exchange::handle(
        first,
        profile.clone(),
        sessions.clone(),
        accepted.clone(),
        permits.clone().acquire_owned().await.unwrap(),
    )
    .await
    .unwrap();
    let second = profile
        .request("GET", "second", None, Body::empty())
        .unwrap();
    let rejected = exchange::handle(
        second,
        profile,
        sessions.clone(),
        accepted,
        permits.clone().acquire_owned().await.unwrap(),
    )
    .await;
    assert!(rejected.is_err());
    assert!(sessions.get("second").unwrap().life.error().is_err());
    drop(response);
}

#[tokio::test]
async fn stream_up_listener_accepts_single_stream_and_download_request_modes_match_reference() {
    use crate::split_http::{server::exchange, sessions::Sessions};
    for (server_mode, session) in [("stream-up", ""), ("stream-one", "paired-download")] {
        let mut c = profile();
        c.mode = server_mode.into();
        let p = Profile::new(&c);
        let request = p
            .request(
                if session.is_empty() { "PUT" } else { "GET" },
                session,
                None,
                Body::empty(),
            )
            .unwrap();
        let (accepted, mut incoming) = tokio::sync::mpsc::channel(1);
        let permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap();
        let response = exchange::handle(request, p, Sessions::default(), accepted, permit)
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert!(incoming.try_recv().is_ok());
        drop(response);
    }
}

#[test]
fn cookie_payload_preserves_numbered_order_and_first_duplicate_across_headers() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let mut config = profile();
    config.options.uplink_data_placement = "cookie".into();
    config.options.uplink_data_key = "part".into();
    let p = Profile::new(&config);
    let payload: Vec<u8> = (0..8192).map(|i| (i % 251) as u8).collect();
    let encoded = URL_SAFE_NO_PAD.encode(&payload);
    let chunks: Vec<_> = encoded.as_bytes().chunks(64).collect();
    let mut request = Request::new(());
    for (i, chunk) in chunks.iter().enumerate().rev() {
        request.headers_mut().append(
            "cookie",
            format!(
                "noise=ignored; part_{i}=\"{}\"",
                std::str::from_utf8(chunk).unwrap()
            )
            .parse()
            .unwrap(),
        );
    }
    request.headers_mut().append(
        "cookie",
        "part_0=invalid; part_00=invalid; part_9999=invalid"
            .parse()
            .unwrap(),
    );
    assert_eq!(p.packet_prefix(&request).unwrap(), payload);
}
