#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use support::xhttp::interop_transport;
async fn interop(mode: &str, zero_client: bool) {
    interop_transport(mode, zero_client, false).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_to_xray_packet_up_tcp_udp() {
    interop("packet-up", true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_to_xray_stream_up_tcp_udp() {
    interop("stream-up", true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_to_xray_auto_tcp_udp() {
    interop("auto", true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xray_to_zero_packet_up_tcp_udp() {
    interop("packet-up", false).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xray_to_zero_stream_up_tcp_udp() {
    interop("stream-up", false).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xray_to_zero_auto_tcp_udp() {
    interop("auto", false).await;
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_to_xray_stream_up_tls_tcp_udp() {
    interop_transport("stream-up", true, true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xray_to_zero_http2_packet_up_tcp_udp() {
    interop_transport("packet-up", false, true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xray_to_zero_http2_stream_up_tcp_udp() {
    interop_transport("stream-up", false, true).await;
}
