//! Existing Browser Source originals lend the same raw accepted carrier.
use super::*;
impl Options {
    pub fn into_carrier_owner(mut self, runtime: &crate::transport::VlessTransportRuntime) -> Self {
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"vless.browser.raw-carrier.v1");
        self.identity = identity.into();
        self.carrier = Some(
            runtime
                .carrier_bindings
                .access(&self.identity, self.protocol.endpoint.clone()),
        );
        self
    }
    pub fn carrier(&self) -> Option<crate::component::quic::TransportCarrier> {
        self.carrier.clone()
    }
    pub fn bind(
        &self,
        owner: zero_core::component::ComponentId,
    ) -> crate::component::grpc::StreamOptions {
        let mut protocol = self.protocol.framing_only();
        protocol.encryption = self.plan.owned_encryption();
        crate::component::grpc::StreamOptions::from_carrier(
            self.carrier.clone().expect("actual Browser carrier"),
            Arc::new(protocol),
            self.identity.clone(),
            owner,
        )
    }
}
