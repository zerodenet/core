use super::*;
impl Options {
    pub fn into_carrier_owner(mut self, runtime: &crate::transport::VlessTransportRuntime) -> Self {
        // Only framing-free transport acquisition is shared. Upper VLESS MUX
        // or ordinary Stream applies the original Encryption policy once.
        self.protocol.encryption = self.plan.owned_encryption();
        let mut suffix = self.suffix.to_vec();
        suffix.extend_from_slice(b"vless.packet.raw-carrier.v1");
        self.suffix = suffix.into();
        self.refresh_material_identity();
        self.carrier = Some(
            runtime
                .carrier_bindings
                .access(&self.identity, self.protocol.endpoint.clone()),
        );
        self
    }
    pub fn carrier(&self) -> Option<TransportCarrier> {
        self.carrier.clone()
    }
    pub fn bind(
        &self,
        owner: zero_core::component::ComponentId,
    ) -> crate::component::grpc::StreamOptions {
        crate::component::grpc::StreamOptions::from_carrier(
            self.carrier.clone().expect("actual packet carrier recipe"),
            Arc::new(self.protocol.framing_only()),
            self.identity.clone(),
            owner,
        )
    }
}
