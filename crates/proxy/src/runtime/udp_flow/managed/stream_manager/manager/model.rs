use std::marker::PhantomData;
use std::sync::Arc;

use zero_core::Session;

use super::super::super::cache::ManagedUdpConnectionCache;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::managed::flow::ManagedRelayStreamCarrier;
use crate::runtime::udp_flow::packet_path::{UdpFlowContext, UdpPacketRef};

pub(crate) struct ManagedStreamFlowManager<T> {
    pub(super) upstreams: ManagedUdpConnectionCache,
    pub(super) establish_stage: &'static str,
    pub(super) relay_upstream_stage: &'static str,
    pub(super) relay_establish_stage: &'static str,
    pub(super) relay_send_stage: &'static str,
    pub(super) mismatch_stage: &'static str,
    pub(super) mismatch_message: &'static str,
    _resume: PhantomData<T>,
}

pub(crate) struct SharedManagedStreamFlowManager<T>(
    pub(super) Arc<tokio::sync::Mutex<ManagedStreamFlowManager<T>>>,
);

impl<T> Clone for SharedManagedStreamFlowManager<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub(super) struct ManagedStreamRelayRequest<'a, T> {
    pub(super) ctx: UdpFlowContext<'a>,
    pub(super) carrier: ManagedRelayStreamCarrier<'a>,
    pub(super) tls_server_name: Option<&'a str>,
    pub(super) services: Option<UdpRuntimeServices>,
    pub(super) session: &'a Session,
    pub(super) endpoint: OutboundEndpoint,
    pub(super) resume: T,
    pub(super) packet_ref: UdpPacketRef<'a>,
}

impl<T> ManagedStreamFlowManager<T> {
    pub(crate) fn new(
        establish_stage: &'static str,
        relay_upstream_stage: &'static str,
        relay_establish_stage: &'static str,
        relay_send_stage: &'static str,
        mismatch_stage: &'static str,
        mismatch_message: &'static str,
    ) -> Self {
        Self {
            upstreams: ManagedUdpConnectionCache::new(),
            establish_stage,
            relay_upstream_stage,
            relay_establish_stage,
            relay_send_stage,
            mismatch_stage,
            mismatch_message,
            _resume: PhantomData,
        }
    }
}

impl<T> SharedManagedStreamFlowManager<T> {
    pub(crate) fn new(manager: ManagedStreamFlowManager<T>) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(manager)))
    }
}
