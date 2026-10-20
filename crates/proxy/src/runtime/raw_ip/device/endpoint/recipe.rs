//! Validated client-stack values. Creating or dropping this recipe starts no I/O.
use super::*;

pub(crate) struct EndpointStackRecipe {
    local_addresses: Vec<IpAddr>,
    mtu: u16,
    peer: usize,
    endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
    options: super::super::super::RawIpStackOptions,
}
impl EndpointStackRecipe {
    pub(crate) fn prepare(
        local_addresses: Vec<IpAddr>,
        mtu: u16,
        peer: usize,
        endpoint: watch::Receiver<mpsc::Sender<EndpointPacket>>,
        options: super::super::super::RawIpStackOptions,
    ) -> Result<Self, zero_stack::client_udp::ClientUdpStackError> {
        ClientUdpStack::validate_configuration(&local_addresses, mtu, options.udp_receive_queue)?;
        Ok(Self {
            local_addresses,
            mtu,
            peer,
            endpoint,
            options,
        })
    }

    /// BuildNew is the only phase allowed to consume this recipe.
    /// This does not certify old resources closed; the host must hold that proof.
    pub(crate) fn start(self) -> Result<Arc<SharedRawIpDevice>, EngineError> {
        SharedRawIpDevice::start_endpoint_stack(
            self.local_addresses,
            self.mtu,
            self.peer,
            self.endpoint,
            self.options,
        )
    }
}
