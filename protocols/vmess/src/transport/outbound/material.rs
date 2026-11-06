//! Same actual MUX material for standard and compatibility consumers.
use super::*;
impl OwnedVmessOutboundTransportPlan {
    pub(in crate::transport) fn bind_material_registry(
        &mut self,
        material: crate::component::material::Registry,
    ) {
        self.material = material;
    }
    /// The native MUX instance selects its original immutable material. A
    /// reconstructed UDP/TEMP compatibility leaf uses this same registry too.
    pub(in crate::transport) async fn open_native_mux_carrier(
        &self,
        stream: TcpRelayStream,
        original: zero_core::component::ComponentInstanceId,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let Some(bound) = self.material.bound(original)? else {
            if self.transport.tls.as_ref().is_some_and(|profile| {
                profile.ca_cert_path.is_some()
                    || profile
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
                    "same actual VMess MUX material owner is not installed",
                )
                .into());
            }
            // Original fixed library setup, without a File or dynamic DNS
            // requirement, retains its existing inline transport duties.
            return self.open_relay(stream).await;
        };
        let prepared = bound.material.prepare(bound.resolver.as_deref()).await?;
        let io = prepared.connect(stream).await?;
        if let Some(grpc) = &self.transport.grpc {
            let settings = io
                .application_settings()
                .filter(|settings| settings.protocol == b"h2")
                .map(|settings| settings.peer.clone());
            let authority = self
                .transport
                .tls
                .as_ref()
                .and_then(|tls| tls.server_name.as_deref())
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    self.server
                        .parse::<std::net::IpAddr>()
                        .map(|ip| std::net::SocketAddr::new(ip, self.port).to_string())
                        .unwrap_or_else(|_| self.server.clone())
                });
            return Ok(TcpRelayStream::new(
                zero_transport::grpc::connect_grpc_inline_with_settings(
                    io,
                    grpc,
                    &authority,
                    settings.as_deref(),
                )
                .await?,
            ));
        }
        zero_transport::outbound_stack::connect_layered_transport_stack(
            io,
            StreamTransportStack {
                tls: None::<&OwnedClientTlsProfile>,
                ws: self.transport.ws.as_ref(),
                grpc: None::<&OwnedGrpcProfile>,
                h2: None::<&OwnedH2Profile>,
                http_upgrade: None::<&OwnedHttpUpgradeProfile>,
                source_dir: None,
            },
            self.server(),
            self.port(),
            "VMess WS and gRPC are mutually exclusive",
        )
        .await
    }
}
