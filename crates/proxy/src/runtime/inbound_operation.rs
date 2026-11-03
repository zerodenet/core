//! Standard source dispatch and focused Control/Packet execution.
//! Protocol factories hand off real accepted channels; this facade owns no SPI.

pub(crate) mod control;
#[cfg(feature = "raw-ip-runtime")]
pub(crate) mod raw_ip;
pub(in crate::runtime) mod source;

#[cfg(feature = "raw-ip-runtime")]
pub(crate) use raw_ip::{
    EndpointPeerState, RawIpInboundAction, RawIpInboundDevice, RawIpInboundDispatch,
    RawIpInboundEndpoint,
};
