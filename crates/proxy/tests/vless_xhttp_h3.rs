#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::xhttp::interop_options;
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xhttp_http3_all_modes_and_metadata_interoperate_both_directions() {
    for mode in ["packet-up", "stream-up", "stream-one"] {
        let native = json!({"mode":mode,"session_placement":"header","seq_placement":"query","x_padding_obfs_mode":true,"x_padding_placement":"cookie","x_padding_method":"tokenish"});
        let official = json!({"_test_http3":true,"mode":mode,"sessionPlacement":"header","seqPlacement":"query","xPaddingObfsMode":true,"xPaddingPlacement":"cookie","xPaddingMethod":"tokenish"});
        for zero_client in [true, false] {
            interop_options(mode, zero_client, true, native.clone(), official.clone()).await;
        }
    }
}
