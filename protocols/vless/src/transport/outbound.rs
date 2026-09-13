mod direct;
mod plan;
mod relay;

use direct::{build_vless_direct_outbound_transport, build_vless_udp_outbound_transport};
use relay::{build_vless_outbound_transport_over_stream, build_vless_split_http_over_relay};

pub use plan::OwnedVlessOutboundTransportPlan;
pub(super) use plan::{
    VlessDirectTransportRequest, VlessFinalHopTransportRequest, VlessOutboundTransportRequest,
    VlessTransportOptions, VlessUdpOutboundTransportRequest, VlessUdpTransportOptions,
};

mod xhttp;
