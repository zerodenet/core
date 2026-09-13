#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::xhttp::interop_options;
#[test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
fn xhttp_extended_metadata_payload_padding_interoperate_both_directions() {
    for workers in [1, 4] {
        let mut runtime = if workers == 1 {
            tokio::runtime::Builder::new_current_thread()
        } else {
            let mut runtime = tokio::runtime::Builder::new_multi_thread();
            runtime.worker_threads(workers);
            runtime
        };
        runtime
            .enable_all()
            .build()
            .unwrap()
            .block_on(extended_options_matrix());
    }
}
async fn extended_options_matrix() {
    for (session, seq, data, padding, method) in [
        ("header", "cookie", "header", "query", "PUT"),
        ("cookie", "query", "cookie", "header", "GET"),
        ("query", "path", "body", "cookie", "PATCH"),
        ("path", "header", "body", "queryInHeader", "POST"),
    ] {
        let native = json!({"mode":"packet-up","session_placement":session,"seq_placement":seq,"uplink_data_placement":data,"uplink_http_method":method,"x_padding_obfs_mode":true,"x_padding_placement":padding,"x_padding_method":"tokenish","x_padding_bytes":{"from":180,"to":220},"sc_max_each_post_bytes":2048,"sc_min_posts_interval_ms":1,"sc_max_buffered_posts":8,"uplink_chunk_size":{"from":128,"to":256},"server_max_header_bytes":16384,"headers":{"X-Client":"zero-reference-test"}});
        let official = json!({"mode":"packet-up","sessionPlacement":session,"seqPlacement":seq,"uplinkDataPlacement":data,"uplinkHTTPMethod":method,"xPaddingObfsMode":true,"xPaddingPlacement":padding,"xPaddingMethod":"tokenish","xPaddingBytes":"180-220","scMaxEachPostBytes":2048,"scMinPostsIntervalMs":1,"scMaxBufferedPosts":8,"uplinkChunkSize":"128-256","serverMaxHeaderBytes":16384,"headers":{"X-Client":"zero-reference-test"}});
        for zero_client in [true, false] {
            interop_options(
                "packet-up",
                zero_client,
                false,
                native.clone(),
                official.clone(),
            )
            .await;
        }
    }
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xhttp_stream_modes_support_custom_headers_metadata_and_disabled_mime_headers() {
    for mode in ["stream-up", "stream-one"] {
        let native = json!({"mode":mode,"session_placement":"cookie","seq_placement":"header","uplink_http_method":"PUT","no_grpc_header":true,"no_sse_header":true,"x_padding_obfs_mode":true,"x_padding_placement":"header","x_padding_header":"X-Pad","x_padding_method":"repeat-x","x_padding_bytes":500,"sc_stream_up_server_secs":1});
        let official = json!({"mode":mode,"sessionPlacement":"cookie","seqPlacement":"header","uplinkHTTPMethod":"PUT","noGRPCHeader":true,"noSSEHeader":true,"xPaddingObfsMode":true,"xPaddingPlacement":"header","xPaddingHeader":"X-Pad","xPaddingMethod":"repeat-x","xPaddingBytes":500,"scStreamUpServerSecs":1});
        for zero_client in [true, false] {
            interop_options(mode, zero_client, true, native.clone(), official.clone()).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xhttp_omitted_host_uses_tls_server_name() {
    for zero_client in [true, false] {
        let native = if zero_client {
            json!({})
        } else {
            json!({"host":"custom.test"})
        };
        let official = if zero_client {
            json!({"_test_server_name":"custom.test","host":"custom.test"})
        } else {
            json!({"_test_server_name":"custom.test"})
        };
        interop_options("stream-up", zero_client, true, native, official).await;
    }
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xhttp_separate_download_endpoint_interoperates_both_directions() {
    for mode in ["packet-up", "stream-up"] {
        for zero_client in [true, false] {
            interop_options(
                mode,
                zero_client,
                true,
                json!({}),
                json!({"_test_download":true}),
            )
            .await;
        }
    }
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xhttp_xmux_limits_interoperate_with_official_server() {
    for mode in ["packet-up", "stream-up", "stream-one"] {
        interop_options(mode, true, true,
            json!({"xmux":{"max_connections":1,"c_max_reuse_times":3,"h_max_request_times":4,"h_max_reusable_secs":60,"h_keep_alive_period":1},"sc_max_each_post_bytes":8192,"sc_min_posts_interval_ms":1}),
            json!({})).await;
    }
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xhttp_relay_xmux_and_separate_download_interoperate_with_official_server() {
    for tls in [false, true] {
        for mode in ["packet-up", "stream-up", "stream-one", "auto"] {
            interop_options(mode, true, tls,
                json!({"xmux":{"max_connections":1,"c_max_reuse_times":3,"h_max_request_times":4},"sc_max_each_post_bytes":8192,"sc_min_posts_interval_ms":1}),
                json!({"_test_relay":true,"_test_download":mode != "stream-one"})).await;
        }
    }
}
