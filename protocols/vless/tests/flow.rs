#![cfg(feature = "reality")]

use vless::{
    flow::{
        decode_addons, encode_addons, flow_build_request, parse_flow, FLOW_XTLS_RPRX_VISION,
        FLOW_XTLS_RPRX_VISION_UDP_LEGACY, FLOW_ZERO_AEAD_V1,
    },
    outbound::PreparedVlessOutboundRequestBundle,
    parse_uuid,
};
use zero_core::Address;

#[test]
fn test_flow_roundtrip() {
    let uuid_str = "b831381d-6324-4d53-ad4f-8cda48b30811";
    let uuid = parse_uuid(uuid_str).unwrap();
    let flow = Some(FLOW_ZERO_AEAD_V1);

    let address = Address::Domain("example.com".into());
    let (fbyte, payload) = flow_build_request(&uuid, flow, 0x01, 443, &address).unwrap();

    assert_eq!(fbyte, 0x01);
    assert!(payload.len() >= 8 + 1 + 2 + 16);
}

#[test]
fn test_plain_no_flow() {
    let uuid_str = "b831381d-6324-4d53-ad4f-8cda48b30811";
    let uuid = parse_uuid(uuid_str).unwrap();

    let address = Address::Ipv4([127, 0, 0, 1]);
    let (fbyte, payload) = flow_build_request(&uuid, None, 0x01, 80, &address).unwrap();

    assert_eq!(fbyte, 0x00);
    assert_eq!(payload.len(), 8);
    assert_eq!(payload[0], 0x01);
    assert_eq!(u16::from_be_bytes([payload[1], payload[2]]), 80);
}

#[test]
fn test_parse_flow_valid() {
    assert!(parse_flow(FLOW_XTLS_RPRX_VISION).is_ok());
    assert!(parse_flow(FLOW_ZERO_AEAD_V1).is_ok());
}

#[test]
fn test_parse_flow_invalid() {
    assert!(parse_flow("unknown-flow").is_err());
    assert!(parse_flow("").is_err());
    assert!(parse_flow(FLOW_XTLS_RPRX_VISION_UDP_LEGACY).is_err());
}

#[test]
fn vision_tcp_bundle_does_not_fail_while_materializing_udp_capabilities() {
    PreparedVlessOutboundRequestBundle::from_config(
        "b831381d-6324-4d53-ad4f-8cda48b30811",
        Some(FLOW_XTLS_RPRX_VISION),
        None,
        None,
    )
    .expect("TCP Vision config must not fail while the capability bundle is prepared");
}

#[test]
fn vision_addons_match_xray_protobuf_wire_format() {
    let encoded = encode_addons(Some(FLOW_XTLS_RPRX_VISION)).unwrap();
    assert_eq!(encoded[0] as usize, encoded.len() - 1);
    assert_eq!(encoded[1], 0x0a);
    assert_eq!(encoded[2] as usize, FLOW_XTLS_RPRX_VISION.len());
    assert_eq!(&encoded[3..], FLOW_XTLS_RPRX_VISION.as_bytes());
    assert_eq!(
        decode_addons(&encoded[1..]).unwrap(),
        Some(FLOW_XTLS_RPRX_VISION)
    );
}

#[test]
fn no_flow_addons_are_empty() {
    assert_eq!(encode_addons(None).unwrap(), [0]);
    assert_eq!(decode_addons(&[]).unwrap(), None);
}

#[test]
fn addons_decoder_skips_unknown_protobuf_fields() {
    let mut encoded = vec![0x10, 0x2a]; // unknown varint field 2
    encoded.extend_from_slice(&encode_addons(Some(FLOW_XTLS_RPRX_VISION)).unwrap()[1..]);
    encoded.extend_from_slice(&[0x1a, 0x02, 0xaa, 0xbb]); // unknown bytes field 3

    assert_eq!(
        decode_addons(&encoded).unwrap(),
        Some(FLOW_XTLS_RPRX_VISION)
    );
}
