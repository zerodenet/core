// VLESS standard flow negotiation and Zero-private compatibility flow.

mod addons;
mod zero_aead;

use zero_core::Error;

pub use crate::flow_name::{
    FLOW_XTLS_RPRX_VISION, FLOW_XTLS_RPRX_VISION_UDP_LEGACY, FLOW_ZERO_AEAD_V1,
};
pub use addons::{decode_addons, encode_addons};
pub use zero_aead::flow_build_request;
pub(crate) use zero_aead::flow_read_request;

pub fn parse_flow(name: &str) -> Result<&'static str, Error> {
    match name {
        FLOW_XTLS_RPRX_VISION => Ok(FLOW_XTLS_RPRX_VISION),
        FLOW_ZERO_AEAD_V1 => Ok(FLOW_ZERO_AEAD_V1),
        FLOW_XTLS_RPRX_VISION_UDP_LEGACY => Err(Error::Unsupported(
            "VLESS flow `xtls-rprx-vision-udp443` is obsolete; use `xtls-rprx-vision`",
        )),
        _ => Err(Error::Unsupported(
            "VLESS flow is not supported; expected `xtls-rprx-vision` or `zero-aead-v1`",
        )),
    }
}

pub(crate) fn parse_inbound_flow(name: &str) -> Result<&'static str, Error> {
    match parse_flow(name)? {
        FLOW_ZERO_AEAD_V1 => Ok(FLOW_ZERO_AEAD_V1),
        FLOW_XTLS_RPRX_VISION => Ok(FLOW_XTLS_RPRX_VISION),
        _ => Err(Error::Unsupported("VLESS inbound flow is not supported")),
    }
}

pub(crate) fn is_vision_flow(flow: Option<&str>) -> bool {
    flow == Some(FLOW_XTLS_RPRX_VISION)
}

pub(crate) fn is_zero_aead_flow(flow: Option<&str>) -> bool {
    flow == Some(FLOW_ZERO_AEAD_V1)
}
