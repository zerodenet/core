//! Selected facts only; the actual Host validates them before issuing a use.
use crate::{
    contract::{component::ControlledStreamDemandClass, network::UpstreamBinding},
    controlled_carrier::CarrierGrant,
};
use zero_core::{component::ComponentInstanceId, request::Destination};
pub struct ProcessEndpointDemand<'a> {
    pub consumer: ComponentInstanceId,
    pub process: ComponentInstanceId,
    pub process_ordinal: usize,
    pub endpoint: &'a Destination,
    pub binding: &'a UpstreamBinding,
    pub class: ControlledStreamDemandClass,
    pub grant: &'a CarrierGrant,
}
