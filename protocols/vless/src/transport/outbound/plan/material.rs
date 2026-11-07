//! Original MUX material remains with its actual owning protocol instance.
use super::*;
impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) fn bind_material_registry(
        &mut self,
        material: crate::component::material::Registry,
    ) {
        self.material = material;
    }
    pub(in crate::transport) async fn open_owned_mux_carrier(
        &self,
        io: TcpRelayStream,
        instance: zero_core::component::ComponentInstanceId,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let Some(bound) = self.material.bound(instance)? else {
            if self.transport.tls.as_ref().is_some_and(|tls| {
                tls.ca_cert_path.is_some()
                    || tls
                        .options
                        .ech
                        .source()
                        .map(|source| {
                            matches!(source, Some(zero_traits::EchConfigSource::Dns { .. }))
                        })
                        .unwrap_or(true)
            }) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "same actual VLESS MUX File/ECH owner is not installed",
                )
                .into());
            }
            return self.open_relay(io).await;
        };
        let prepared = bound.material.prepare(bound.resolver.as_deref()).await?;
        let io = prepared.connect(io).await?;
        let io = zero_transport::outbound_stack::connect_layered_transport_stack(
            io,
            zero_transport::outbound_stack::StreamTransportStack {
                tls: None::<&OwnedClientTlsProfile>,
                ws: self.transport.ws.as_ref(),
                grpc: None::<&OwnedGrpcProfile>,
                h2: self.transport.h2.as_ref(),
                http_upgrade: self.transport.http_upgrade.as_ref(),
                source_dir: None,
            },
            self.server(),
            self.port(),
            "VLESS stream carriers are mutually exclusive",
        )
        .await?;
        self.encrypt(io).await
    }
}
